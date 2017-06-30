'use strict';

/**
 * A simple library for making microservice message bus
 * calls using RabbitMQ.
 *
 * @module postmaster-general
 */
const EventEmitter = require('events');
const rabbit = require('rabbot');
const Promise = require('bluebird');
const uuid = require('uuid');
const bunyan = require('bunyan');
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
		if (typeof options.exclusive !== 'undefined') {
			listenerQueue.exclusive = options.exclusive;
		}
		if (typeof options.limit !== 'undefined') {
			listenerQueue.limit = options.limit;
		}
		if (typeof options.messageTtl !== 'undefined') {
			listenerQueue.messageTtl = options.messageTtl;
		}

		// Only allow listening on two exchanges: postmaster.topic and postmaster.dlx.
		if (options.exchange === 'postmaster.dlx') {
			this.listenExchangeName = this.settings.exchanges[1].name;
		} else {
			this.listenExchangeName = this.settings.exchanges[0].name;
		}

		// Set options for publishing.
		this.settings.connection.replyTimeout = listenerQueue.messageTtl;

		const replyQueueName = options.replyQueue || 'queue';
		this.settings.connection.replyQueue = `postmaster.reply.${replyQueueName}.${uuid.v4()}`;

		// Postmaster expects loggers to be syntactially-compatible with the excellent Bunyan library.
		this.logger = options.logger;
		if (!this.logger) {
			this.logger = bunyan.createLogger({
				name: 'postmaster-general',
				serializers: {err: bunyan.stdSerializers.err}
			});

			// Default log level to info.
			this.logger.level(options.logLevel || 'info');
		}

		// Store a reference to rabbot.
		this.rabbit = rabbit;

		// We want to automatically send all unhandled messages and errors to the dead letter queue.
		this.rabbit.rejectUnhandled();

		// Add event listeners to log events from rabbot.
		this.rabbit.on('failed', () => {
			this.logger.warn('postmaster-general failed to connect to RabbitMQ! Retrying...');
		});
		this.rabbit.on('unreachable', () => {
			this.logger.error('postmaster-general is unable to reach RabbitMQ!');
			this.emit('unreachable');
		});

		// Store a list of message handlers.
		this.listeners = {};

		// If set, this will cause postmaster-general to log a warning when a message takes too long to ack.
		this.settings.ackWarnTimeout = options.ackWarnTimeout;
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
				this.logger.info('postmaster-general started.');
			})
			.catch((err) => {
				this.logger.error({err: err}, 'postmaster-general failed to start!');
				throw err;
			});
	}

	/**
	 * Called to stop the PostmasterGeneral instance.
	 */
	stop() {
		this.logger.info('Shutting down postmaster-general...');
		return this.rabbit.shutdown().then(() => {
			this.logger.info('postmaster-general shutdown successfully.');
		})
			.catch((err) => {
				this.logger.warn({err: err}, 'postmaster-general encountered error when trying to shutdown!');
			});
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

		this.logger.trace({address: address, message: message}, `Sent fire-and-forget message.`);
		return this.rabbit.publish(exchange, options).catch((err) => {
			this.logger.error({err: err, address: address, message: message}, `postmaster-general failed to send fire-and-forget message!`);
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

		// If we have the ack timer enabled, start it up.
		let ackTimeout = this.settings.ackWarnTimeout ? setTimeout(() => {
			this.logger.warn({address: address, message: message}, 'postmaster-general hit the ACK timeout for a publisher request!');
		}, this.settings.ackWarnTimeout) : null;

		// If we want a reply, call request.
		this.logger.trace({address: address, message: message}, `postmaster-general sent RPC call.`);
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
				this.logger.error({err: err, address: address, message: message}, `postmaster-general failed to send RPC call!`);
				throw err;
			})
			.then((body) => {
				if (ackTimeout) {
					clearTimeout(ackTimeout);
				}
				return body;
			});
	}

	/**
	 * Called to bind a new listener to the queue.
	 * @param {string} address - The message address.
	 * @param {function} callback - The callback function that handles the message.
	 * @param {object} [context] - The object to use as the "this" context during execution of the callback function.
	 */
	addListener(address, callback, context) {
		const topic = this.resolveTopic(address);

		return new Promise((resolve, reject) => {
			try {
				this.listeners[topic] = this.rabbit.handle({
					type: topic,
					context: this,
					handler: function (message) {
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

						// To make things easier on ourselves, convert the callback to a promise.
						const promiseCallback = Promise.promisify(callback, {context: context});
						const body = message.body;
						body.$requestId = requestId;
						body.$trace = trace;

						// If we have the ack timer enabled, start it up.
						let ackTimeout = this.settings.ackWarnTimeout ? setTimeout(() => {
							this.logger.warn({address: address, message: message}, 'postmaster-general hit the ACK timeout for a listener!');
						}, this.settings.ackWarnTimeout) : null;

						return promiseCallback(body)
							.then((reply) => {
								this.logger.trace({address: address, message: message}, 'postmaster-general processed callback for message.');

								// If we have an error that should be returned to the caller, log it and convert the error to a string.
								if (reply && reply.err) {
									this.logger.error({err: reply.err, address: address, message: message}, 'postmaster-general callback returned an error!');
									reply.err = reply.err.message || reply.err.name;
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
								this.logger.error({err: err, address: address, message: message}, 'postmaster-general encountered error processing callback!');
								message.reject();
							})
							.then(() => {
								if (ackTimeout) {
									clearTimeout(ackTimeout);
								}
							});
					}
				});

				// Add a sanity-check error handler for the listener, in case an exception is thrown that's not caught in the promise.
				this.listeners[topic].catch((err, message) => {
					this.logger.error({err: err, address: address, message: message}, 'postmaster-general encountered error processing callback!');
					message.reject();
				});

				resolve();
			} catch (err) {
				this.logger.error({err: err, address: address}, 'postmaster-general encountered error while registering a listener!');
				reject(err);
			}
		});
	}

	/**
	 * Called to unbind a new listener from the queue.
	 * @param {string} address - The message address.
	 */
	removeListener(address) {
		const topic = this.resolveTopic(address);

		return new Promise((resolve, reject) => {
			try {
				const handler = this.listeners[topic];
				if (handler) {
					handler.remove();
					delete this.listeners[topic];
					this.logger.info({address: address}, 'postmaster-general removed listener callback.');
				}
				resolve();
			} catch (err) {
				this.logger.error({err: err, address: address}, 'postmaster-general encountered error while removing a listener!');
				reject(err);
			}
		});
	}
}

module.exports = PostmasterGeneral;

// Start up postmaster if this module is run as a script.
if (require.main === module) {
	const postmaster = new PostmasterGeneral();
	postmaster.start();
}
