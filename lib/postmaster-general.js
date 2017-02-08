'use strict';

/**
 * @module lib/amqp-client
 */
const _ = require('lodash');
const amqp = require('amqplib');
const amqpuri = require('amqpuri');
const Promise = require('bluebird');
const uuid = require('uuid');

const mSelf = module.exports = {
	PostmasterGeneral: class {
		constructor(options) {
			this.options = options || {};
			this.publisherConn = {};
			this.listenerConn = {};
		}

		start() {
			return Promise.all([
				// Connect to RabbitMQ
				this.connect("publisher"),
				this.connect("listener"),
				// Declare "dead letter" queues
				this.declareDeadLetter(this.publisherConn.channel),
				this.declareDeadLetter(this.listenerConn.channel),
				// Start listening
				this.listenForReplies(),
				this.listenForMessages()
			]);
		}

		/**
		 * Connects to the AMQP host and initializes exchanges and queues.
		 * @param {string} connectionType - Either "publisher" or "listener".
		 * @returns {Promise} - Promise callback indicating connection success or failure.
		 */
		connect(connectionType) {
			let queueOptions = this.options[connectionType];

			return amqp.connect(this.options.url, this.options.socketOptions)
				.then((conn) => conn.createChannel())
				.then((channel) => {
					let ex = this.options.exchange;
					let queue = queueOptions.queue;
					let queueName = connectionType === "publisher" ? this.resolveCallbackQueue(queue) : _.trim(queueOptions.name);
					channel.prefetch(queueOptions.channel.prefetch);
					return Promise.props({
						channel,
						exchange: channel.assertExchange(ex.name, ex.type, ex.options),
						queue: channel.assertQueue(queueName, queue.options)
					}).then((connection) => {
						return {
							channel: connection.channel,
							exchange: connection.exchange.exchange,
							queue: connection.queue.queue,
							callMap: {}
						};
					});
				});
		}

		/**
		 * Disconnects from the AMQP server.
		 * @param {string} connectionType - Either "publisher" or "listener".
		 */
		close(connectionType) {
			let connection = connectionType === "publisher" ? this.publisherConn : this.listenerConn;
			try {
				connection.channel.close();
				connection.channel.connection.close();
			} catch (err) {
				console.error('AMQP channel already closed!', err);
			}
		}

		//#region Publisher

		/**
		 * Publishes a message to the specified address, optionally returning a callback.
		 * @param {string} address - The message address.
		 * @param {object} message - The message data.
		 * @param {boolean} replyRequired - If true, a reply is expected for this message.
		 * @returns {Promise} - Promise returning the message response, if one is requested.
		 */
		publish(address, message, replyRequired) {
			return new Promise((resolve, reject) => {
				let topic = this.resolveTopic(address);
				let messageString = JSON.stringify(message);
				let options = {
					contentType: 'application/json'
				};

				if (replyRequired) {
					// If we want a reply, we need to store the correlation id so we know which handler to call.
					let correlationId = uuid.v4();
					options.replyTo = this.publisherConn.queue;
					options.correlationId = correlationId;

					// Store a callback to handle the response in the callmap.
					let timeout;
					this.publisherConn.callMap[correlationId] = (error, data) => {
						timeout.clearTimeout();
						delete this.publisherConn.callMap[correlationId];
						if (error) {
							reject(error);
						} else {
							resolve(data);
						}
					};

					// If no response is returned, clear the callback and return an error.
					timeout = setTimeout(() => {
						delete this.publisherConn.callMap[correlationId];
						reject(new mSelf.RPCTimeoutError(`postmaster-general timeout while sending message! topic='${topic} message='${messageString}'`));
					}, this.options.timeout);

					// Publish the message. If publishing failed, indicate that the channel's write buffer is full.
					let pubSuccess = this.publisherConn.channel.publish(this.publisherConn.exchange, topic, new Buffer(messageString), options);
					if (!pubSuccess) {
						timeout.clearTimeout();
						delete this.publisherConn.callMap[correlationId];
						reject(new mSelf.PublishBufferFullError(`postmaster-general failed sending message due to full publish buffer! topic='${topic} message='${messageString}'`));
					}
				} else {
					// if no callback is requested, simply publish the message.
					let pubSuccess = this.publisherConn.channel.publish(this.publisherConn.exchange, topic, new Buffer(messageString), options);
					if (pubSuccess) {
						resolve();
					} else {
						reject(new mSelf.PublishBufferFullError(`postmaster-general failed sending message due to full publish buffer! topic='${topic} message='${messageString}'`));
					}
				}
			});
		}

		/**
		 * Consumes a reply from the response queue and calls the callback, if it exists.
		 * @param {object} message - The message object returned by the listener.
		 */
		consumeReply(message) {
			message = message || {};
			message.properties = message.properties || {};

			// Only handle messages that belong to us, and that we have an active handler for.
			let correlationId = message.properties.correlationId;
			if (correlationId && this.publisherConn.callMap[correlationId]) {
				let content = message.content ? message.content.toString() : undefined;
				this.publisherConn.callMap[correlationId](JSON.parse(content));
				delete this.publisherConn.callMap[correlationId];
			}
		}

		/**
		 * Called to begin consuming from the RPC queue associated with this instance.
		 */
		listenForReplies() {
			return this.publisherConn.channel.consume(this.publisherConn.queue, this.consumeReply, {noAck: true});
		}

		/**
		 * Called to resolve the name of the callback queue for this instance.
		 * @param {object} options - The queue data passed from the instance options.
		 */
		resolveCallbackQueue(options) {
			options = _.defaults({}, options, {
				prefix: 'postmaster',
				separator: '.'
			});
			let sid = options.id || Uuid.v4().split('-')[0];
			return `${options.prefix}${options.separator}${sid}`;
		}

		//#endregion

		//#region Listener

		addListener(address, callback) {
			let topic = this.resolveTopic(address);
			return channel.bindQueue(this.listenerConn.queue, this.listenerConn.exchange, topic)
				.then(() => {
					this.listenerConn.callMap[topic] = callback;
				});
		}

		handleMessage(message, data) {
			return this.listenerConn.callMap[message.fields.routingKey](data)
				.then((out) => {
					if (!out || !message.properties.replyTo) {
						return;
					}
					this.listenerConn.channel.sendToQueue(message.properties.replyTo, new Buffer(JSON.stringify(out)), {
						correlationId: message.properties.correlationId
					});
					this.listenerConn.channel.ack(message);
				});
		}

		consume(message) {
			let content = message.content ? message.content.toString() : undefined;
			if (!content) {
				// Do not requeue message if there is no payload
				// or we don't know where to reply
				return this.listenerConn.channel.nack(message, false, false);
			}
			let data = JSON.parse(content);
			return this.handleMessage(message, data);
		}

		listenForMessages() {
			return this.listenerConn.channel.consume(this.listenerConn.queue, this.consume);
		}

		//#endregion

		//#region Dead Letter

		/**
		 * Declares an exchange and queue described by
		 * `options` and binds them with '#' as
		 * routing key.
		 *
		 * `options` is an Object matching the following example:
		 * {
		 *   "queue": {
		 *     "name": "seneca.dlq"
		 *   },
		 *   "exchange": {
		 *     "type": "topic",
		 *     "name": "seneca.dlx",
		 *     "options": {
		 *       "durable": true,
		 *       "autoDelete": false
		 *     }
		 *   }
		 * }
		 *
		 * @param  {Objet} options   Configuration for the dead-letter queue and exchange.
		 * @param  {amqplib.Channel} channel Queue and exchange will be declared on this channel.
		 * @return {Promise}         Resolves when the exchange, queue and binding are created.
		 */
		declareDeadLetter(channel) {
			let options = this.options.deadLetter;
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
								return { rk: '#' };
							})
							.then((bind) => {
								return { dlq: dlq.queue, dlx: dlx.exchange, rk: bind.rk };
							})
						);
				}
			});
		}

		//#endregion

		/**
	     * Called to resolve the AMQP topic corresponding to an address.
		 * @param {string} address
		 */
		resolveTopic(address) {
			address.replace(':', '.');
		}
	},

	//#region Custom Exceptions

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
	},

	//#endregion
};
