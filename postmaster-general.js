'use strict';

/**
 * A simple library for making both RPC and fire-and-forget microservice
 * calls using AMQP.
 *
 * @module postmaster-general
 */
const domain = require('domain'); // eslint-disable-line no-restricted-modules
const _ = require('lodash');
const amqp = require('amqplib');
const Promise = require('bluebird');
const uuid = require('uuid');
const defaults = require('./defaults');

const mSelf = module.exports = {
	PostmasterGeneral: class {
		/**
		 * Constructor function for the Postmaster object.
		 * @param {string} queueName
		 * @param {object} options
		 */
		constructor(queueName, options) {
			options = options || {};
			this.options = defaults;
			this.options.listener.name = queueName;
			this.options.url = options.url || this.options.url;
			this.options.listener.queue.options.durable = options.durable || this.options.listener.queue.options.durable;
			this.options.logSent = options.logSent || this.options.logSent;
			this.publisherConn = {};
			this.listenerConn = {};
			this.shuttingDown = false;

			// Because this class makes heavy use of promises and callbacks, it's
			// easy for the 'this' context to get lost in member functions. To fix this,
			// and to make the context more clear, use 'self' reference within member methods.
			let self = this;

			// amqplib uses emitters for channel errors. We need to bind those to a domain
			// in order to properly handle them.
			this.dom = domain.create();
			this.dom.on('error', (err) => {
				console.error(err.message);
				if (!self.shuttingDown) {
					self.stop();
				}
			});

			/**
			 * Called to start the PostmasterGeneral instance.
			 */
			this.start = function () {
				console.log('Starting postmaster-general...');
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
					.then(() => self.listenForMessages())
					.then(() => {
						console.log('postmaster-general started!');
					});
			};

			/**
			 * Called to stop the PostmasterGeneral instance.
			 */
			this.stop = function () {
				self.shuttingDown = true;
				self.close('publisher');
				self.close('listener');
			};

			/**
			 * Connects to the AMQP host and initializes exchanges and queues.
			 * @param {string} connectionType - Either "publisher" or "listener".
			 * @returns {Promise} - Promise callback indicating connection success or failure.
			 */
			this.connect = function (connectionType) {
				console.log(`Connecting ${connectionType} to AMQP host ${self.options.url}`);
				let queueOptions = self.options[connectionType];
				return amqp.connect(self.options.url, self.options.socketOptions)
					.then((conn) => {
						self.dom.add(conn);
						return conn.createChannel();
					})
					.then((channel) => {
						self.dom.add(channel);
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
							console.log(`${connectionType} connected!`);
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
			this.close = function (connectionType) {
				let connection = connectionType === 'publisher' ? self.publisherConn : self.listenerConn;

				console.log(`Closing ${connectionType} connection.`);
				try {
					connection.channel.connection.close();
				} catch (err) {
					console.error(`Encountered error while closing ${connectionType} connection: ${err.message}`);
				}
			};

			/**
			 * Checks the health of the connection.
			 * @param {string} connectionType - Either "publisher" or "listener".
			 * @returns {Promise} - A promise that resolves to true, if postmaster is healthy.
			 */
			this.healthCheck = function () {
				let publisherChannel = self.publisherConn.channel;
				let listenerChannel = self.listenerConn.channel;

				// Simple health check just verifies both the incoming and outgoing queues.
				// The promises will reject if either channel is invalidated, or if the queues don't exist.
				return publisherChannel.checkQueue(self.publisherConn.queue)
					.then(() => listenerChannel.checkQueue(self.listenerConn.queue))
					.then(() => {
						return true;
					})
					.catch((err) => {
						throw new mSelf.ConnectionFailedError(err.message);
					});
			};

			// #region Publisher

			/**
			 * Publishes a message to the specified address, optionally returning a callback.
			 * @param {string} address - The message address.
			 * @param {object} message - The message data.
			 * @param {object} args - Optional arguments, including "requestId", "replyRequired", and "trace".
			 * @returns {Promise} - Promise returning the message response, if one is requested.
			 */
			this.publish = function (address, message, args) {
				return new Promise((resolve, reject) => {
					args = args || {};
					let replyRequired = args.replyRequired;
					let requestId = args.requestId;
					let trace = args.trace;
					let topic = self.resolveTopic(address);
					let messageString = JSON.stringify(message);
					let options = {
						contentType: 'application/json'
					};

					// Generate a new request id if none is set.
					requestId = requestId || uuid.v4();

					// Pass the request info and the trace settings to the recipient.
					options.headers = {
						requestId: requestId,
						trace: trace
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
							reject(new mSelf.RPCTimeoutError(`postmaster-general timeout while sending message! topic='${topic}' message='${messageString}' requestId='${requestId}'`));
						}, self.publisherConn.timeout);

						// Publish the message. If publishing failed, indicate that the channel's write buffer is full.
						let pubSuccess = self.publisherConn.channel.publish(self.publisherConn.exchange, topic, new Buffer(messageString), options);
						if (pubSuccess) {
							if (self.options.logSent) {
								console.log(`postmaster-general sent message successfully! topic='${topic}' message='${messageString}' requestId='${requestId}'`);
							}
							if (trace) {
								let traceMessage = JSON.parse(messageString);
								traceMessage.sentAt = new Date();
								traceMessage.address = address;
								self.publisherConn.channel.publish(self.publisherConn.exchange, self.resolveTopic(`log:${requestId}`), new Buffer(JSON.stringify(traceMessage)), {
									contentType: 'application/json',
									headers: {
										requestId: requestId
									}
								});
							}
						} else {
							timeout.clearTimeout();
							delete self.publisherConn.callMap[correlationId];
							reject(new mSelf.PublishBufferFullError(`postmaster-general failed sending message due to full publish buffer! topic='${topic}' message='${messageString}' requestId='${requestId}'`));
						}
					} else {
						// if no callback is requested, simply publish the message.
						let pubSuccess = self.publisherConn.channel.publish(self.publisherConn.exchange, topic, new Buffer(messageString), options);
						if (pubSuccess) {
							if (self.options.logSent) {
								console.log(`postmaster-general sent message successfully! topic='${topic}' message='${messageString}' requestId='${requestId}'`);
							}
							// Log the request, if the tracing request id is passed.
							if (trace) {
								let traceMessage = JSON.parse(messageString);
								traceMessage.sentAt = new Date();
								traceMessage.address = address;
								self.publisherConn.channel.publish(self.publisherConn.exchange, self.resolveTopic(`log:${requestId}`), new Buffer(JSON.stringify(traceMessage)), {
									contentType: 'application/json',
									headers: {
										requestId: requestId
									}
								});
							}
							resolve();
						} else {
							reject(new mSelf.PublishBufferFullError(`postmaster-general failed sending message due to full publish buffer! topic='${topic}' message='${messageString}' requestId='${requestId}'`));
						}
					}
				});
			};

			/**
			 * Consumes a reply from the response queue and calls the callback, if it exists.
			 * @param {object} message - The message object returned by the listener.
			 */
			this.consumeReply = function (message) {
				message = message || {};
				message.properties = message.properties || {};

				// Only handle messages that belong to us, and that we have an active handler for.
				let correlationId = message.properties.correlationId;
				if (correlationId && self.publisherConn.callMap[correlationId]) {
					let content = message.content ? message.content.toString() : undefined;
					// If there's no content in the response, indicate error
					if (content) {
						content = JSON.parse(content);
						// Add special fields for logging.
						content.$requestId = message.properties.headers.requestId;
						content.$trace = message.properties.headers.trace;
						self.publisherConn.callMap[correlationId](null, content);
					} else {
						self.publisherConn.callMap[correlationId](new mSelf.InvalidRPCResponseError(`Invalid response received for call with correlationId ${correlationId}`));
					}
					delete self.publisherConn.callMap[correlationId];
				} else {
					console.warn(`postmaster-general received reply to a request it didn't send! message=${JSON.stringify(message)}`);
				}
			};

			/**
			 * Called to begin consuming from the RPC queue associated with this instance.
			 */
			this.listenForReplies = function () {
				return self.publisherConn.channel.consume(self.publisherConn.queue, self.consumeReply, {noAck: true});
			};

			/**
			 * Called to resolve the name of the callback queue for this instance.
			 * @param {object} options - The queue data passed from the instance options.
			 */
			this.resolveCallbackQueue = function (options) {
				options = _.defaults({}, options, {
					prefix: 'postmaster',
					separator: '.'
				});
				let sid = options.id || uuid.v4().split('-')[0];
				return `${options.prefix}${options.separator}${sid}`;
			};

			// #endregion

			// #region Listener

			/**
			 * Called to bind a new listener to the queue.
			 */
			this.addListener = function (address, callback) {
				let topic = self.resolveTopic(address);

				// If this topic is a regular expression pre-compile it for faster comparison later.
				if (topic.includes('*') || topic.includes('#')) {
					let regExpStr = topic.replace('.', '\\.');
					// In AMPQ, '*' matches a single word...
					regExpStr = topic.replace('*', '[^.]+');
					// ...while '#' matches 0 or more words
					regExpStr = topic.replace('#', '.*');
					self.listenerConn.regexMap = self.listenerConn.regexMap || [];

					// Search the regexMap to make sure we don't introduce duplicates.
					let exists = self.listenerConn.regexMap.find((element) => {
						return element.topic === topic;
					});
					if (!exists) {
						self.listenerConn.regexMap.push({regex: new RegExp(regExpStr), topic: topic});
					}
				}

				// Wildcards in AMQP work differently than standard regex, '#' effectively corresponds to '*'.
				return self.listenerConn.channel.bindQueue(self.listenerConn.queue, self.listenerConn.exchange, topic)
					.then(() => {
						self.listenerConn.callMap[topic] = callback;
					});
			};

			/**
			 * Called to unbind a new listener from the queue.
			 */
			this.removeListener = function (address) {
				let topic = self.resolveTopic(address);
				return self.listenerConn.channel.unbindQueue(self.listenerConn.queue, self.listenerConn.exchange, topic)
					.then(() => {
						delete self.listenerConn.callMap[topic];
					});
			};

			/**
			 * Called to process a message when it's received.
			 */
			this.handleMessage = function (message, data) {
				// Add special message fields for logging.
				data.$requestId = message.properties.headers.requestId;
				data.$trace = message.properties.headers.trace;

				// Find the callback, either by direct match or regex.
				let routingKey = message.fields.routingKey;
				let callMapKey;
				if (self.listenerConn.callMap[routingKey]) {
					callMapKey = routingKey;
				} else if (self.listenerConn.regexMap) {
					let regexMap = self.listenerConn.regexMap;
					let mapping;
					for (mapping of regexMap) {
						if (mapping.regex.test(routingKey)) {
							callMapKey = mapping.topic;
						}
					}
				}

				// If we don't have a handler, just re-queue it.
				if (!callMapKey || !self.listenerConn.callMap[callMapKey]) {
					return self.listenerConn.channel.nack(message, false);
				}

				return self.listenerConn.callMap[callMapKey](data, (error, out) => {
					if (error) {
						console.error(`Error processing message. message='${JSON.stringify(message)}' error='${error.message}'`);
						return self.listenerConn.channel.nack(message, false, false);
					} else if (out && message.properties.replyTo) {
						let outMessage = JSON.stringify(out);
						self.listenerConn.channel.sendToQueue(message.properties.replyTo, new Buffer(outMessage), {
							correlationId: message.properties.correlationId,
							headers: message.properties.headers
						});
						if (self.options.logSent) {
							console.log(`postmaster-general sent reply! message='${outMessage}' requestId='${message.properties.headers.requestId}'`);
						}
						if (message.properties.headers.trace) {
							let requestId = message.properties.headers.requestId || 'default';
							let traceMessage = JSON.parse(outMessage);
							traceMessage.sentAt = new Date();
							traceMessage.replyTo = message.properties.replyTo;
							traceMessage.correlationId = message.properties.correlationId;
							self.publisherConn.channel.publish(self.publisherConn.exchange, self.resolveTopic(`log:${requestId}`), new Buffer(JSON.stringify(traceMessage)), {
								contentType: 'application/json',
								headers: {
									requestId: requestId
								}
							});
						}
						self.listenerConn.channel.ack(message);
					} else {
						self.listenerConn.channel.ack(message);
					}
				});
			};

			/**
			 * Called upon receipt of a new message.
			 */
			this.consume = function (message) {
				let content = message.content ? message.content.toString() : undefined;
				if (!content) {
					// Do not requeue message if there is no payload.
					return self.listenerConn.channel.nack(message, false, false);
				}
				let data = JSON.parse(content);
				return self.handleMessage(message, data);
			};

			/**
			 * Called to set the listener to listening.
			 */
			this.listenForMessages = function () {
				return self.listenerConn.channel.consume(self.listenerConn.queue, self.consume);
			};

			// #endregion

			// #region Dead Letter

			/**
			 * Declares an exchange and queue described by
			 * `options` and binds them with '#' as
			 * routing key.
			 *
			 * @param  {amqplib.Channel} channel Queue and exchange will be declared on this channel.
			 * @return {Promise}         Resolves when the exchange, queue and binding are created.
			 */
			this.declareDeadLetter = function (channel) {
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
			this.resolveTopic = function (address) {
				return address.replace(':', '.');
			};
		}
	},

	// #region Errors

	/**
	 * Error sent when RPC-style callbacks timeout.
	 */
	RPCTimeoutError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	},

	/**
	 * Error sent when publishing failed due a full send buffer.
	 */
	PublishBufferFullError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	},

	/**
	 * Error sent when the rpc response sends back an invalid response.
	 */
	InvalidRPCResponseError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	},

	/**
	 * Error generated as the result of a failed health check.
	 */
	ConnectionFailedError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	}

	// #endregion
};
