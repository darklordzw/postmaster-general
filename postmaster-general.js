'use strict';

/**
 * @module postmaster-general
 */
const _ = require('lodash');
const amqp = require('amqplib');
const Promise = require('bluebird');
const uuid = require('uuid');
const defaults = require('./defaults');

const mSelf = module.exports = {
	Postmaster: class {
		constructor(queueName, options) {
			this.options = _.defaults({}, options, defaults);
			this.options.listener.name = queueName;
			this.publisherConn = {};
			this.listenerConn = {};

			// Because this class makes heavy use of promises and callbacks, it's
			// easy for the 'this' context to get lost in member functions. To fix this,
			// and to make the context more clear, use 'self' reference within member methods.
			let self = this;

			this.start = () => {
				return self.connect('publisher')
					.then((conn) => {
						self.publisherConn = conn;
					})
					.then(() => self.connect('listener'))
					.then((conn) => {
						self.listenerConn = conn;
					})
					.then(() => self.declareDeadLetter(self.publisherConn.channel))
					.then(() => self.listenForReplies())
					.then(() => self.listenForMessages());
			};

			this.stop = () => {
				return Promise.all([
					self.close('publisher'),
					self.close('listener')
				]);
			};

			/**
			 * Connects to the AMQP host and initializes exchanges and queues.
			 * @param {string} connectionType - Either "publisher" or "listener".
			 * @returns {Promise} - Promise callback indicating connection success or failure.
			 */
			this.connect = (connectionType) => {
				let queueOptions = self.options[connectionType];
				return amqp.connect(self.options.url, self.options.socketOptions)
					.then((conn) => conn.createChannel())
					.then((channel) => {
						let ex = self.options.exchange;
						let queue = queueOptions.queue;
						let queueName = connectionType === 'publisher' ? self.resolveCallbackQueue(queue) : _.trim(queueOptions.name);
						channel.prefetch(queueOptions.channel.prefetch);
						return Promise.props({
							channel,
							exchange: channel.assertExchange(ex.name, ex.type, ex.options),
							queue: channel.assertQueue(queueName, queue.options),
							timeout: queue.options.arguments['x-message-ttl']
						}).then((connection) => {
							return {
								channel: connection.channel,
								exchange: connection.exchange.exchange,
								queue: connection.queue.queue,
								timeout: connection.timeout,
								callMap: {}
							};
						});
					});
			};

			/**
			 * Disconnects from the AMQP server.
			 * @param {string} connectionType - Either "publisher" or "listener".
			 */
			this.close = (connectionType) => {
				let connection = connectionType === 'publisher' ? self.publisherConn : self.listenerConn;
				try {
					connection.channel.close();
					connection.channel.connection.close();
				} catch (err) {
					console.error(`AMQP channel already closed! message='${err.message}'`);
				}
			};

			// #region Publisher

			/**
			 * Publishes a message to the specified address, optionally returning a callback.
			 * @param {string} address - The message address.
			 * @param {object} message - The message data.
			 * @param {boolean} replyRequired - If true, a reply is expected for this message.
			 * @returns {Promise} - Promise returning the message response, if one is requested.
			 */
			this.publish = (address, message, replyRequired) => {
				return new Promise((resolve, reject) => {
					let topic = self.resolveTopic(address);
					let messageString = JSON.stringify(message);
					let options = {
						contentType: 'application/json'
					};

					if (replyRequired) {
						// If we want a reply, we need to store the correlation id so we know which handler to call.
						let correlationId = uuid.v4();
						options.replyTo = self.publisherConn.queue;
						options.correlationId = correlationId;

						// Store a callback to handle the response in the callmap.
						let timeout;
						self.publisherConn.callMap[correlationId] = (error, data) => {
							clearTimeout(timeout);
							delete self.publisherConn.callMap[correlationId];
							if (error) {
								reject(error);
							} else {
								resolve(data);
							}
						};

						// If no response is returned, clear the callback and return an error.
						timeout = setTimeout(() => {
							delete self.publisherConn.callMap[correlationId];
							reject(new mSelf.RPCTimeoutError(`postmaster-general timeout while sending message! topic='${topic}' message='${messageString}'`));
						}, self.publisherConn.timeout);

						// Publish the message. If publishing failed, indicate that the channel's write buffer is full.
						let pubSuccess = self.publisherConn.channel.publish(self.publisherConn.exchange, topic, new Buffer(messageString), options);
						if (!pubSuccess) {
							timeout.clearTimeout();
							delete self.publisherConn.callMap[correlationId];
							reject(new mSelf.PublishBufferFullError(`postmaster-general failed sending message due to full publish buffer! topic='${topic}' message='${messageString}'`));
						}
					} else {
						// if no callback is requested, simply publish the message.
						let pubSuccess = self.publisherConn.channel.publish(self.publisherConn.exchange, topic, new Buffer(messageString), options);
						if (pubSuccess) {
							resolve();
						} else {
							reject(new mSelf.PublishBufferFullError(`postmaster-general failed sending message due to full publish buffer! topic='${topic}' message='${messageString}'`));
						}
					}
				});
			};

			/**
			 * Consumes a reply from the response queue and calls the callback, if it exists.
			 * @param {object} message - The message object returned by the listener.
			 */
			this.consumeReply = (message) => {
				message = message || {};
				message.properties = message.properties || {};

				// Only handle messages that belong to us, and that we have an active handler for.
				let correlationId = message.properties.correlationId;
				if (correlationId && self.publisherConn.callMap[correlationId]) {
					let content = message.content ? message.content.toString() : undefined;
					self.publisherConn.callMap[correlationId](null, JSON.parse(content));
					delete self.publisherConn.callMap[correlationId];
				}
			};

			/**
			 * Called to begin consuming from the RPC queue associated with this instance.
			 */
			this.listenForReplies = () => {
				return self.publisherConn.channel.consume(self.publisherConn.queue, self.consumeReply, {noAck: true});
			};

			/**
			 * Called to resolve the name of the callback queue for this instance.
			 * @param {object} options - The queue data passed from the instance options.
			 */
			this.resolveCallbackQueue = (options) => {
				options = _.defaults({}, options, {
					prefix: 'postmaster',
					separator: '.'
				});
				let sid = options.id || uuid.v4().split('-')[0];
				return `${options.prefix}${options.separator}${sid}`;
			};

			// #endregion

			// #region Listener

			this.addListener = (address, callback) => {
				let topic = self.resolveTopic(address);
				return self.listenerConn.channel.bindQueue(self.listenerConn.queue, self.listenerConn.exchange, topic)
					.then(() => {
						self.listenerConn.callMap[topic] = callback;
					});
			};

			this.handleMessage = (message, data) => {
				return self.listenerConn.callMap[message.fields.routingKey](data, (error, out) => {
					if (error) {
						console.error(`Error processing message. message='${JSON.stringify(message)}' error='${error.message}'`);
					} else if (out && message.properties.replyTo) {
						self.listenerConn.channel.sendToQueue(message.properties.replyTo, new Buffer(JSON.stringify(out)), {
							correlationId: message.properties.correlationId
						});
						self.listenerConn.channel.ack(message);
					} else {
						self.listenerConn.channel.ack(message);
					}
				});
			};

			this.consume = (message) => {
				let content = message.content ? message.content.toString() : undefined;
				if (!content) {
					// Do not requeue message if there is no payload
					// or we don't know where to reply
					return self.listenerConn.channel.nack(message, false, false);
				}
				let data = JSON.parse(content);
				return self.handleMessage(message, data);
			};

			this.listenForMessages = () => {
				return self.listenerConn.channel.consume(self.listenerConn.queue, self.consume);
			};

			// #endregion

			// #region Dead Letter

			/**
			 * Declares an exchange and queue described by
			 * `options` and binds them with '#' as
			 * routing key.
			 *
			 * @param  {Objet} options   Configuration for the dead-letter queue and exchange.
			 * @param  {amqplib.Channel} channel Queue and exchange will be declared on this channel.
			 * @return {Promise}         Resolves when the exchange, queue and binding are created.
			 */
			this.declareDeadLetter = (channel) => {
				let options = self.options.deadLetter;
				return Promise.try(() => {
					if (channel && options.queue && options.exchange) {
						let ex = options.exchange;
						let queue = options.queue;
						return Promise.all([
							channel.assertExchange(ex.name, ex.type, ex.options),
							channel.assertQueue(queue.name, queue.options)
						]).spread((dlx, dlq) =>
							channel.bindQueue(dlq.queue, dlx.exchange, '#')
								.then(() => {
									return {rk: '#'};
								})
								.then((bind) => {
									return {dlq: dlq.queue, dlx: dlx.exchange, rk: bind.rk};
								})
							);
					}
				});
			};

			// #endregion

			/**
			 * Called to resolve the AMQP topic corresponding to an address.
			 * @param {string} address
			 */
			this.resolveTopic = (address) => {
				return address.replace(':', '.');
			};
		}
	},

	// #region Custom Exceptions

	RPCTimeoutError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	},

	PublishBufferFullError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	}

	// #endregion
};
