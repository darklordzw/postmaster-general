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
const bunyan = require('bunyan');
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

			// Allow full control of each component.
			this.options.listener = options.listener || this.options.listener;
			this.options.publisher = options.publisher || this.options.publisher;
			this.options.deadLetter = options.deadLetter || this.options.deadLetter;
			this.options.channel = options.channel || this.options.channel;

			// Set convenience parameters.
			this.options.listener.name = queueName;
			this.options.url = options.url || this.options.url;
			this.options.logger = options.logger;
			this.options.logLevel = options.logLevel;
			this.channel = null;
			this.publisherConn = {};
			this.listenerConn = {};
			this.shuttingDown = false;
			this.reconnectTries = 0;
			this.reconnecting = false;
			this.reconnectTimer = null;

			// If queues should auto-delete, make sure they're exclusive and not durable, unless specified.
			if (this.options.listener.queue.options.autoDelete) {
				this.options.listener.queue.options.durable = false;

				if (_.isUndefined(this.options.listener.queue.options.exclusive)) {
					this.options.listener.queue.options.exclusive = true;
				}
			}

			// Postmaster expects loggers to be syntactially-compatible with the excellent Bunyan library.
			this.logger = this.options.logger;
			if (!this.logger) {
				this.logger = bunyan.createLogger({
					name: 'postmaster-general',
					serializers: {err: bunyan.stdSerializers.err}
				});

				// Default log level to info.
				this.logger.level(this.options.logLevel || 'info');
			}

			// Because this class makes heavy use of promises and callbacks, it's
			// easy for the 'this' context to get lost in member functions. To fix this,
			// and to make the context more clear, use 'self' reference within member methods.
			let self = this;

			// amqplib uses emitters for channel errors. We need to bind those to a domain
			// in order to properly handle them.
			this.dom = domain.create();
			this.dom.on('error', (err) => {
				self.logger.error({err: err}, err.message);

				if (!self.shuttingDown) {
					if (!self.reconnecting) {
						self.reconnect();
					} else if (self.reconnectTries >= self.options.channel.reconnectLimit) {
						self.stop();
					}
				}
			});

			/**
			 * Called to start the PostmasterGeneral instance.
			 */
			this.start = function () {
				self.logger.info('Starting postmaster-general...');
				return self.connect()
					.then((conns) => {
						self.publisherConn = conns[0];
						self.listenerConn = conns[1];
					})
					.then(() => self.declareDeadLetter())
					.then(() => self.listenForReplies())
					.then(() => self.listenForMessages())
					.then(() => {
						self.logger.info('postmaster-general started!');
					});
			};

			/**
			 * Called to stop the PostmasterGeneral instance.
			 */
			this.stop = function () {
				self.shuttingDown = true;
				self.close();
				if (self.reconnectTimer) {
					clearTimeout(self.reconnectTimer);
				}
			};

			/**
			 * Called to start the PostmasterGeneral instance.
			 */
			this.reconnect = function () {
				self.reconnecting = true;
				try {
					self.channel.connection.close();
				} catch (err) {}

				self.logger.info(`Reconnecting to AMQP host ${self.options.url}. Retry # ${self.reconnectTries + 1}`);
				return self.connect()
					.then((conns) => {
						const publisherCallMap = self.publisherConn.callMap;
						const listenerCallMap = self.listenerConn.callMap;

						self.publisherConn = conns[0];
						self.publisherConn.callMap = publisherCallMap;

						self.listenerConn = conns[1];
						self.listenerConn.callMap = listenerCallMap;
					})
					.then(() => self.declareDeadLetter())
					.then(() => self.listenForReplies())
					.then(() => self.listenForMessages())
					.then(() => {
						self.reconnectTries = 0;
						self.reconnecting = false;
						if (self.reconnectTimer) {
							clearTimeout(self.reconnectTimer);
						}
						self.logger.info(`Reconnected to AMQP host ${self.options.url}!`);
					})
					.catch(() => {
						self.reconnectTries += 1;
						if (self.reconnectTries < self.options.channel.reconnectLimit) {
							self.reconnectTimer = setTimeout(() => {
								self.reconnect();
							}, self.options.channel.reconnectInterval);
						} else {
							self.stop();
						}
					});
			};

			/**
			 * Connects to the AMQP host and initializes exchanges and queues.
			 * @returns {Promise} - Promise callback indicating connection success or failure.
			 */
			this.connect = function () {
				self.logger.info(`Connecting to AMQP host ${self.options.url}`);
				return amqp.connect(self.options.url)
					.then((conn) => {
						self.dom.add(conn);
						return conn.createChannel();
					})
					.then((channel) => {
						self.channel = channel;
						self.dom.add(self.channel);
						self.channel.prefetch(self.options.channel.prefetch);

						return Promise.all([
							self.initializeQueues('publisher'),
							self.initializeQueues('listener')
						]);
					});
			};

			/**
			 * Initializes the exchanges and queues for the passed connection type.
			 * @param {string} connectionType - Either "publisher" or "listener".
			 * @returns {Promise} - Promise callback indicating connection success or failure.
			 */
			this.initializeQueues = function (connectionType) {
				let queueOptions = self.options[connectionType];
				let ex = queueOptions.exchange;
				let queue = queueOptions.queue;
				let queueName = connectionType === 'publisher' ? self.resolveCallbackQueue(queue) : _.trim(queueOptions.name);
				return Promise.props({
					exchange: self.channel.assertExchange(ex.name, ex.type, ex.options),
					queue: self.channel.assertQueue(queueName, queue.options),
					timeout: queue.options.arguments['x-message-ttl']
				}).then((connection) => {
					self.logger.info(`${connectionType} connected!`);
					return {
						exchange: connection.exchange.exchange,
						queue: connection.queue.queue,
						timeout: connection.timeout,
						callMap: {}
					};
				});
			};

			/**
			 * Disconnects from the AMQP server.
			 */
			this.close = function () {
				self.logger.info(`Closing amqp connection.`);
				try {
					self.channel.connection.close();
				} catch (err) {
					self.logger.error({err: err}, `Encountered error while closing amqp connection!`);
				}
			};

			/**
			 * Checks the health of the connection.
			 * @returns {Promise} - A promise that resolves to true, if postmaster is healthy.
			 */
			this.healthCheck = function () {
				// Simple health check just verifies both the incoming and outgoing queues.
				// The promises will reject if either channel is invalidated, or if the queues don't exist.
				return self.channel.checkQueue(self.publisherConn.queue)
					.then(() => self.channel.checkQueue(self.listenerConn.queue))
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
			this.publish = function (address, message, args, retryCount) {
				if (self.reconnecting) {
					return new Promise(function (resolve, reject) {
						retryCount = retryCount || 0;

						if (retryCount < self.options.publisher.retryCount) {
							retryCount += 1;

							setTimeout(() => {
								self.publish(address, message, args, retryCount)
									.then((results) => {
										resolve(results);
									})
									.catch((err) => {
										reject(err);
									});
							}, self.options.publisher.retryInterval);
						} else {
							reject(new Error(`postmaster-general failed to publish message after ${retryCount} tries!`));
						}
					});
				}

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
						let pubSuccess;
						try {
							pubSuccess = self.channel.publish(self.publisherConn.exchange, topic, new Buffer(messageString), options);
						} catch (err) {
							self.logger.error({err: err}, 'postmaster-general encountered exception while publishing a message!');
							pubSuccess = false;
						}
						if (pubSuccess) {
							self.logger.trace({topic: topic, message: message, requestId: requestId}, 'postmaster-general sent message successfully!');
							if (trace) {
								let traceMessage = JSON.parse(messageString);
								traceMessage.sentAt = new Date();
								traceMessage.address = address;
								try {
									self.channel.publish(self.publisherConn.exchange, self.resolveTopic(`log:${requestId}`), new Buffer(JSON.stringify(traceMessage)), {
										contentType: 'application/json',
										headers: {
											requestId: requestId
										}
									});
								} catch (err) {
									self.logger.warn({topic: topic, message: message, requestId: requestId}, `Failed to send log message!`);
								}
							}
						} else {
							timeout.clearTimeout();
							delete self.publisherConn.callMap[correlationId];
							reject(new mSelf.PublishBufferFullError(`postmaster-general failed sending message!! topic='${topic}' message='${messageString}' requestId='${requestId}'`));
						}
					} else {
						// if no callback is requested, simply publish the message.
						let pubSuccess;
						try {
							pubSuccess = self.channel.publish(self.publisherConn.exchange, topic, new Buffer(messageString), options);
						} catch (err) {
							self.logger.error({err: err}, 'postmaster-general encountered exception while publishing a message!');
							pubSuccess = false;
						}
						if (pubSuccess) {
							self.logger.trace({topic: topic, message: message, requestId: requestId}, 'postmaster-general sent message successfully!');
							// Log the request, if the tracing request id is passed.
							if (trace) {
								let traceMessage = JSON.parse(messageString);
								traceMessage.sentAt = new Date();
								traceMessage.address = address;
								try {
									self.channel.publish(self.publisherConn.exchange, self.resolveTopic(`log:${requestId}`), new Buffer(JSON.stringify(traceMessage)), {
										contentType: 'application/json',
										headers: {
											requestId: requestId
										}
									});
								} catch (err) {
									self.logger.warn({topic: topic, message: message, requestId: requestId}, `Failed to send log message!`);
								}
							}
							resolve();
						} else {
							reject(new mSelf.PublishBufferFullError(`postmaster-general failed sending message! topic='${topic}' message='${messageString}' requestId='${requestId}'`));
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
				let content = message.content ? message.content.toString() : undefined;
				if (correlationId && self.publisherConn.callMap[correlationId]) {
					// If there's no content in the response, indicate error
					if (content) {
						try {
							content = JSON.parse(content);
							// Add special fields for logging.
							content.$requestId = message.properties.headers.requestId;
							content.$trace = message.properties.headers.trace;
							self.publisherConn.callMap[correlationId](null, content);
						} catch (err) {
							self.publisherConn.callMap[correlationId](new mSelf.InvalidRPCResponseError(`Invalid response received for call with correlationId ${correlationId}. Could not parse as JSON.`));
						}
					} else {
						self.publisherConn.callMap[correlationId](new mSelf.InvalidRPCResponseError(`Invalid response received for call with correlationId ${correlationId}`));
					}
					delete self.publisherConn.callMap[correlationId];
				} else {
					self.logger.warn({message: JSON.stringify(content)}, 'postmaster-general received reply to a request it didn\'t send!');
				}
			};

			/**
			 * Called to begin consuming from the RPC queue associated with this instance.
			 */
			this.listenForReplies = function () {
				return self.channel.consume(self.publisherConn.queue, self.consumeReply, {noAck: true});
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
					let regExpStr = topic.replace(/\./g, '\\.');
					// In AMPQ, '*' matches a single word...
					regExpStr = topic.replace(/\*/g, '[^.]+');
					// ...while '#' matches 0 or more words
					regExpStr = topic.replace(/#/g, '.*');
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
				return self.channel.bindQueue(self.listenerConn.queue, self.listenerConn.exchange, topic)
					.then(() => {
						self.listenerConn.callMap[topic] = callback;
					});
			};

			/**
			 * Called to unbind a new listener from the queue.
			 */
			this.removeListener = function (address) {
				let topic = self.resolveTopic(address);
				return self.channel.unbindQueue(self.listenerConn.queue, self.listenerConn.exchange, topic)
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
					return self.channel.nack(message, false);
				}

				return self.listenerConn.callMap[callMapKey](data, (error, out) => {
					if (error) {
						self.logger.error({err: error, message: message}, 'Error processing message.');
					} else if (out && message.properties.replyTo) {
						self.sendReply(out, message)
							.catch((err) => {
								self.logger.error({err: err}, 'postmaster-general failed to send reply!');
							});
					}

					// If we get here, we're going to try and process the message. Go ahead and ack.
					self.channel.ack(message);
				});
			};

			this.sendReply = function (out, message, retryCount) {
				if (self.reconnecting) {
					return new Promise(function (resolve, reject) {
						retryCount = retryCount || 0;

						if (retryCount < self.options.listener.retryCount) {
							retryCount += 1;

							setTimeout(() => {
								self.sendReply(out, message, retryCount)
									.then((results) => {
										resolve(results);
									})
									.catch((err) => {
										reject(err);
									});
							}, self.options.listener.retryInterval);
						} else {
							reject(new Error(`postmaster-general failed to publish reply message after ${retryCount} tries!`));
						}
					});
				}

				return new Promise(function (resolve, reject) {
					let outMessage = JSON.stringify(out);

					try {
						self.channel.sendToQueue(message.properties.replyTo, new Buffer(outMessage), {
							correlationId: message.properties.correlationId,
							headers: message.properties.headers
						});
						self.logger.trace({message: out, requestId: message.properties.headers.requestId}, 'postmaster-general sent reply!');
						if (message.properties.headers.trace) {
							let requestId = message.properties.headers.requestId || 'default';
							let traceMessage = JSON.parse(outMessage);
							traceMessage.sentAt = new Date();
							traceMessage.replyTo = message.properties.replyTo;
							traceMessage.correlationId = message.properties.correlationId;
							try {
								self.channel.publish(self.publisherConn.exchange, self.resolveTopic(`log:${requestId}`), new Buffer(JSON.stringify(traceMessage)), {
									contentType: 'application/json',
									headers: {
										requestId: requestId
									}
								});
							} catch (err) {
								self.logger.warn({message: out, requestId: message.properties.headers.requestId}, `Failed to send log message!`);
							}
						}

						resolve();
					} catch (err) {
						reject(err);
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
					return self.channel.nack(message, false, false);
				}
				let data;
				try {
					data = JSON.parse(content);
				} catch (err) {
					data = {};
				}
				return self.handleMessage(message, data);
			};

			/**
			 * Called to set the listener to listening.
			 */
			this.listenForMessages = function () {
				return self.channel.consume(self.listenerConn.queue, self.consume);
			};

			// #endregion

			// #region Dead Letter

			/**
			 * Declares an exchange and queue described by
			 * `options` and binds them with '#' as
			 * routing key.
			 *
			 * @return {Promise}         Resolves when the exchange, queue and binding are created.
			 */
			this.declareDeadLetter = function () {
				let options = self.options.deadLetter;
				return Promise.try(() => {
					if (options.queue && options.exchange) {
						let ex = options.exchange;
						let queue = options.queue;
						return Promise.all([
							self.channel.assertExchange(ex.name, ex.type, ex.options),
							self.channel.assertQueue(queue.name, queue.options)
						]).spread((dlx, dlq) =>
							self.channel.bindQueue(dlq.queue, dlx.exchange, '#')
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
				return address.replace(/:/g, '.');
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
