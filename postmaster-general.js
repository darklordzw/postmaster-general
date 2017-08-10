'use strict';

/**
 * A simple library for making microservice message bus
 * calls using RabbitMQ.
 *
 * @module postmaster-general
 */
const EventEmitter = require('events');
const Promise = require('bluebird');
const uuid = require('uuid');
const log4js = require('log4js');
const defaults = require('./defaults');

class PostmasterGeneral extends EventEmitter {
	/**
	 * Constructor function for the PostmasterGeneral object.
	 * @param {string} queueName
	 * @param {object} [options]
	 */
	constructor(queueName, options) {
		super();

		options = options || {};
		this.settings = defaults;

		// Set uri, or default to localhost.
		if (typeof options.uri !== 'undefined') {
			this.settings.connection.uri = options.uri;
		}

		// Set options for listening.
		const listenerQueue = this.settings.queues[0];
		listenerQueue.name = queueName;

		if (typeof options.durable !== 'undefined') {
			listenerQueue.durable = options.durable;
		}
		if (typeof options.autoDelete !== 'undefined') {
			listenerQueue.autoDelete = options.autoDelete;
		}
		if (!listenerQueue.autoDelete) {
			listenerQueue.expires = this.settings.queues[0].messageTtl;
		}
		if (typeof options.exclusive !== 'undefined') {
			listenerQueue.exclusive = options.exclusive;
		}
		if (typeof options.limit !== 'undefined') {
			listenerQueue.limit = options.limit;
		}
		if (typeof options.messageTtl !== 'undefined') {
			listenerQueue.messageTtl = options.messageTtl;
		}
		if (typeof options.noAck !== 'undefined') {
			listenerQueue.noAck = options.noAck;
		}
		if (typeof options.noBatch !== 'undefined') {
			listenerQueue.noBatch = options.noBatch;
		}

		// Only allow listening on two exchanges: postmaster.topic and postmaster.dlx.
		if (options.exchange === 'postmaster.dlx') {
			this.listenExchangeName = this.settings.exchanges[1].name;
		} else {
			this.listenExchangeName = this.settings.exchanges[0].name;
		}

		// Set options for publishing.
		if (typeof options.publishTimeout !== 'undefined') {
			this.settings.connection.publishTimeout = options.publishTimeout;
		}
		this.settings.connection.replyTimeout = this.settings.connection.publishTimeout * 2;

		const replyQueueName = options.replyQueue || 'queue';
		this.settings.connection.replyQueue = {
			name: `postmaster.reply.${replyQueueName}.${uuid.v4()}`,
			subscribe: true,
			durable: true,
			autoDelete: false,
			expires: this.settings.queues[0].messageTtl,
			noAck: true
		};

		// Postmaster expects loggers to be syntactially-compatible with the excellent log4js library.
		this.logger = options.logger;
		if (!this.logger) {
			this.logger = log4js.getLogger();

			// Default log level to info.
			this.logger.level = options.logLevel || 'info';
		}

		// Set logging for Rabbot, if "debug" is specified.
		if (this.logger.level === 'debug') {
			process.env.DEBUG = 'rabbot.*';
		}

		// Store a reference to rabbot.
		this.rabbit = require('rabbot');

		// We want to automatically send all unhandled messages and errors to the dead letter queue.
		this.rabbit.rejectUnhandled();

		// Add event listeners to log events from rabbot.
		this.rabbit.on('connected', () => {
			this.logger.info('postmaster-general connected successfully!');
		});
		this.rabbit.on('failed', () => {
			this.logger.warn('postmaster-general failed to connect to RabbitMQ! Retrying...');
		});
		this.rabbit.on('unreachable', () => {
			this.logger.error('postmaster-general is unable to reach RabbitMQ!');
			this.emit('unreachable');
		});

		// Store a list of message handlers.
		this.listeners = {};

		// Store a collection of handler timings.
		this.handlerTimings = {};
		if (!options.manualTimingReset) {
			this.handlerTimingsTimeout = null;
			this.handlerTimingsInterval = 30000;
		}
	}

	/**
	 * Called to start the PostmasterGeneral instance.
	 */
	start() {
		this.logger.info('Starting postmaster-general...');
		return this.rabbit.configure(this.settings)
			.then(() => {
				const promises = [];
				for (let topic of Object.keys(this.listeners)) {
					promises.push(this.rabbit.bindQueue(this.listenExchangeName, this.settings.queues[0].name, topic));
				}
				return Promise.all(promises);
			})
			.then(() => {
				this.resetHandlerTimings();
				this.logger.info('postmaster-general started.');
			})
			.catch((err) => {
				this.logger.error('postmaster-general failed to start!', err);
				throw err;
			});
	}

	/**
	 * Called to stop the PostmasterGeneral instance.
	 */
	stop() {
		this.logger.info('Shutting down postmaster-general...');
		return this.rabbit.shutdown().then(() => {
			if (this.handlerTimingsTimeout) {
				clearTimeout(this.handlerTimingsTimeout);
			}
			this.logger.info('postmaster-general shutdown successfully.');
		})
			.catch((err) => {
				this.logger.warn('postmaster-general encountered error when trying to shutdown!', err);
			});
	}

	/**
	 * Called periodically to reset the handler timing metrics.
	 */
	resetHandlerTimings() {
		this.handlerTimings = {};

		// If we're not manually managing the timing refresh, schedule the next timeout.
		if (this.handlerTimingsInterval) {
			this.handlerTimingsTimeout = setTimeout(() => {
				this.resetHandlerTimings();
			}, this.handlerTimingsInterval);
		}
	}

	/**
	 * Called to resolve the AMQP topic key corresponding to an address.
	 * @param {string} address
	 */
	resolveTopic(address) {
		return address.replace(/:/g, '.');
	}

	/**
	 * Publishes a message to the specified address.
	 * @param {string} address - The message address.
	 * @param {object} message - The message data.
	 * @param {object} [args] - Optional arguments, including "requestId", and "trace".
	 * @returns {Promise} - Promise returning the message response.
	 */
	publish(address, message, args) {
		args = args || {};

		const exchange = this.settings.exchanges[0].name;
		const options = {
			type: this.resolveTopic(address),
			body: message,
			headers: {
				requestId: args.requestId || uuid.v4(),
				trace: args.trace
			}
		};

		this.logger.debug(`Sent fire-and-forget message.`, {address: address, message: message});
		return this.rabbit.publish(exchange, options).catch((err) => {
			this.logger.error(`postmaster-general failed to send fire-and-forget message!`, {address: address, message: message}, err);
			throw err;
		});
	}

	/**
	 * Publishes a message to the specified address, returning a promise that resolves on reply.
	 * @param {string} address - The message address.
	 * @param {object} message - The message data.
	 * @param {object} [args] - Optional arguments, including "requestId", and "trace".
	 * @returns {Promise} - Promise returning the message response.
	 */
	request(address, message, args) {
		args = args || {};

		const exchange = this.settings.exchanges[0].name;
		const options = {
			type: this.resolveTopic(address),
			body: message,
			headers: {
				requestId: args.requestId || uuid.v4(),
				trace: args.trace
			},
			correlationId: args.correlationId || uuid.v4()
		};

		// If we want a reply, call request.
		this.logger.debug(`postmaster-general sent RPC call.`, {address: address, message: message});
		return this.rabbit.request(exchange, options)
			.then((reply) => {
				reply.ack();

				reply.properties = reply.properties || {};
				reply.properties.headers = reply.properties.headers || {};

				const body = reply.body;
				body.$requestId = reply.properties.headers.requestId;
				body.$trace = reply.properties.headers.trace;
				return body;
			})
			.catch((err) => {
				this.logger.error(`postmaster-general failed to send RPC call!`, {address: address, message: message}, err);
				throw err;
			});
	}

	/**
	 * Called to bind a new listener to the queue.
	 * @param {string} address - The message address.
	 * @param {function} callback - The callback function that handles the message.
	 * @param {object} [context] - The object to use as the "this" context during execution of the callback function.
	 * @param {boolean} [bindQueue] - If true, go ahead and call "bindQueue" for the listener.
	 * @param {boolean} [exchangeName] - The name of the exchange to bind to.
	 */
	addListener(address, callback, context, bindQueue, exchangeName) {// eslint-disable-line max-params
		const topic = this.resolveTopic(address);
		const exchange = exchangeName || this.listenExchangeName;

		// To make things easier on ourselves, convert the callback to a promise.
		const promiseCallback = Promise.promisify(callback, {context: context});

		return new Promise((resolve, reject) => {
			try {
				this.listeners[topic] = this.rabbit.handle({
					type: topic,
					context: this,
					handler: function (message) {
						const start = new Date().getTime();

						try {
							if (!message.body) {
								throw new Error('Missing field "body" for message!');
							}

							message.properties = message.properties || {};
							message.properties.headers = message.properties.headers || {};

							const replyTo = message.properties.replyTo;
							const requestId = message.properties.headers.requestId;
							const trace = message.properties.headers.trace;
							const correlationId = message.properties.correlationId;
							const messageId = message.properties.messageId;
							const body = message.body;
							body.$requestId = requestId;
							body.$trace = trace;
							body.$address = address;

							return promiseCallback(body)
								.timeout(this.settings.queues[0].messageTtl, 'Message handler timed out!')
								.then((reply) => {
									this.logger.debug('postmaster-general processed callback for message.', {address: address, message: body});

									// If we have an error that should be returned to the caller, log it and convert the error to a string.
									if (reply && reply.err) {
										this.logger.error('postmaster-general callback returned an error!', {address: address, message: body}, reply.err);
										reply.err = reply.err.message || reply.err.name;

										// Sometimes we may want to log the error, but not send the full message to the caller (as in the case of db credentials).
										if (reply.err && reply.cleanError) {
											reply.err = `An error of type ${reply.cleanError} occurred during processing of the message!`;
										}
									}

									if (replyTo && correlationId) {
										// Make sure messsageId and correlationId match, for backwards-compatibility.
										if (!messageId) {
											message.properties.messageId = correlationId;
										}

										message.reply(reply, {headers: {requestId: requestId, trace: trace}});
									} else {
										message.ack();
									}
								})
								.catch((err) => {
									this.logger.error('postmaster-general encountered error processing callback!', {address: address, message: body}, err);
									message.reject();
								})
								.then(() => {
									// Calculate the elapsed time.
									const elapsed = new Date().getTime() - start;
									this.handlerTimings[address] = this.handlerTimings[address] || {
										messageCount: 0,
										elapsedTime: 0,
										minElapsedTime: 0,
										maxElapsedTime: 0
									};
									this.handlerTimings[address].messageCount++;
									this.handlerTimings[address].elapsedTime += elapsed;

									if (this.handlerTimings[address].minElapsedTime > elapsed ||
										this.handlerTimings[address].minElapsedTime === 0) {
										this.handlerTimings[address].minElapsedTime = elapsed;
									}
									if (this.handlerTimings[address].maxElapsedTime < elapsed) {
										this.handlerTimings[address].maxElapsedTime = elapsed;
									}
								});
						} catch (err) {
							this.logger.error('postmaster-general encountered error processing callback!', {address: address, message: message.body}, err);
							message.reject();

							// Calculate the elapsed time.
							const elapsed = new Date().getTime() - start;
							this.handlerTimings[address] = this.handlerTimings[address] || {
								messageCount: 0,
								elapsedTime: 0,
								minElapsedTime: 0,
								maxElapsedTime: 0
							};
							this.handlerTimings[address].messageCount++;
							this.handlerTimings[address].elapsedTime += elapsed;

							if (this.handlerTimings[address].minElapsedTime > elapsed ||
								this.handlerTimings[address].minElapsedTime === 0) {
								this.handlerTimings[address].minElapsedTime = elapsed;
							}
							if (this.handlerTimings[address].maxElapsedTime < elapsed) {
								this.handlerTimings[address].maxElapsedTime = elapsed;
							}
						}
					}
				});

				// Bind the queue, if we need to.
				if (bindQueue) {
					this.rabbit.bindQueue(exchange, this.settings.queues[0].name, topic)
						.then(() => {
							resolve();
						})
						.catch((err) => {
							this.logger.error('postmaster-general encountered error while registering a listener!', {address: address}, err);
							reject(err);
						});
				} else {
					resolve();
				}
			} catch (err) {
				this.logger.error('postmaster-general encountered error while registering a listener!', {address: address}, err);
				reject(err);
			}
		});
	}

	/**
	 * Called to unbind a new listener from the queue.
	 * @param {string} address - The message address.
	 * @param {string} [exchangeName] - The exchange name.
	 */
	removeListener(address, exchangeName) {
		const topic = this.resolveTopic(address);
		const exchange = exchangeName || this.listenExchangeName;

		return new Promise((resolve, reject) => {
			try {
				const handler = this.listeners[topic];
				if (handler) {
					handler.remove();

					return this.rabbit.connections.default.connection.getChannel('control', false, 'control channel for bindings')
						.then((channel) => channel.unbindQueue(`queue:${this.settings.queues[0].name}`, exchange, topic))
						.then(() => {
							delete this.listeners[topic];

							this.logger.info('postmaster-general removed listener callback.', {address: address});
							resolve();
						})
						.catch((err) => {
							this.logger.error('postmaster-general encountered error while removing a listener!', {address: address}, err);
							reject(err);
						});
				}
				resolve();
			} catch (err) {
				this.logger.error('postmaster-general encountered error while removing a listener!', {address: address}, err);
				reject(err);
			}
		});
	}

	/**
	 * Called to stop listening to messages and prepare for shutdown.
	 */
	drainListeners() {
		return this.rabbit.stopSubscription(this.settings.queues[0].name);
	}

	/**
	 * Called to determine if the channels and queues used by postmaster-general are healthy.
	 */
	healthcheck() {
		const listenerQueueName = this.settings.queues[0].name;
		const replyQueueName = this.settings.connection.replyQueue.name;

		return Promise.all([
			this.rabbit.connections.default.connection.getChannel(`queue:${listenerQueueName}`)
				.then((channel) => channel.checkQueue(listenerQueueName)),
			this.rabbit.connections.default.connection.getChannel(`queue:${replyQueueName}`)
				.then((channel) => channel.checkQueue(replyQueueName))
		])
			.then(() => {
				return true;
			})
			.catch((err) => {
				throw new Error(`Failed to confirm postmaster-general channel! Message: ${err.message}`);
			});
	}
}

module.exports = PostmasterGeneral;

// Start up postmaster if this module is run as a script.
if (require.main === module) {
	const postmaster = new PostmasterGeneral();
	postmaster.start();
}
