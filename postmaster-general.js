'use strict';

/**
 * A simple library for making microservice message bus
 * calls using RabbitMQ.
 *
 * @module postmaster-general
 */
const rabbit = require('rabbot');
const _ = require('lodash');
const Promise = require('bluebird');
const uuid = require('uuid');
const bunyan = require('bunyan');
const defaults = require('./defaults');

class PostmasterGeneral {
	/**
	 * Constructor function for the PostmasterGeneral object.
	 * @param {string} uri
	 * @param {object} [options]
	 */
	constructor(queueName, options) {
		options = options || {};
		this.settings = defaults;

		// Set uri, or default to localhost.
		if (!_.isUndefined(options.uri)) {
			this.settings.uri = options.uri;
		}

		// Set options for listening.
		const listenerQueue = this.settings.queues[0];
		listenerQueue.name = queueName;

		if (!_.isUndefined(options.durable)) {
			listenerQueue.durable = options.durable;
		}
		if (!_.isUndefined(options.autoDelete)) {
			listenerQueue.autoDelete = options.autoDelete;
		}
		if (!_.isUndefined(options.exclusive)) {
			listenerQueue.exclusive = options.exclusive;
		}
		if (!_.isUndefined(options.limit)) {
			listenerQueue.limit = options.limit;
		}

		// Set options for publishing.
		if (!_.isUndefined(options.publishTimeout)) {
			this.settings.exchanges[0].publishTimeout = options.publishTimeout;
		}
		const replyQueueName = options.replyQueue || 'queue';
		this.settings.replyQueue = {name: `postmaster.reply.${replyQueueName}.${uuid.v4()}`};

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
		this.rabbit.on('connected', () => {
			this.logger.info('postmaster-general started.');
		});
		this.rabbit.on('failed', () => {
			this.logger.warn('postmaster-general failed to connect to RabbitMQ! Retrying...');
		});
		this.rabbit.on('unreachable', () => {
			this.logger.error('postmaster-general is unable to reach RabbitMQ!');
			this.emit('unreachable', this);
		});

		// Store a list of message handlers.
		this.listeners = {};
	}

	/**
	 * Called to start the PostmasterGeneral instance.
	 */
	start() {
		this.logger.info('Starting postmaster-general...');
		return this.rabbit.configure(this.settings)
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
	 * Publishes a message to the specified address, optionally returning a callback.
	 * @param {string} address - The message address.
	 * @param {object} message - The message data.
	 * @param {object} [args] - Optional arguments, including "requestId", "replyRequired", and "trace".
	 * @returns {Promise} - Promise returning the message response, if one is requested.
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

		// If we want a reply, call request.
		if (args.replyRequired) {
			this.logger.trace({address: address, message: message}, `postmaster-general sent RPC call.`);
			return this.rabbit.request(exchange, options)
				.catch((err) => {
					this.logger.error({err: err, address: address, message: message}, `postmaster-general failed to send RPC call!`);
					throw err;
				});
		}

		this.logger.trace({address: address, message: message}, `Sent fire-and-forget message.`);
		return this.rabbit.publish(exchange, options).catch((err) => {
			this.logger.error({err: err, address: address, message: message}, `postmaster-general failed to send fire-and-forget message!`);
			throw err;
		});
	}

	/**
	 * Called to bind a new listener to the queue.
	 */
	addListener(address, callback, context) {
		const topic = this.resolveTopic(address);

		this.listeners[address] = this.rabbit.handle({
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

				// To make things easier on ourselves, convert the callback to a promise.
				const promiseCallback = Promise.promisify(callback, {context: context});
				return promiseCallback({message: message.body, $requestId: requestId, $trace: trace})
					.then((reply) => {
						this.logger.trace({address: address, message: message}, 'postmaster-general processed callback for message.');

						if (replyTo) {
							message.reply(reply, {headers: {requestId: requestId, trace: trace}});
						} else {
							message.ack();
						}
					})
					.catch((err) => {
						this.logger.error({err: err, address: address, message: message}, 'postmaster-general encountered error processing callback!');
						message.reject();
					});
			}
		});

		// Add a sanity-check error handler for the listener, in case an exception is thrown that's not caught in the promise.
		this.listeners[address].catch((err, message) => {
			this.logger.error({err: err, address: address, message: message}, 'postmaster-general encountered error processing callback!');
			message.reject();
		});
	}

	/**
	 * Called to unbind a new listener from the queue.
	 */
	removeListener(address) {
		const handler = this.listeners[address];
		if (handler) {
			handler.remove();
			delete this.listeners[address];
			this.logger.info({address: address}, 'postmaster-general removed listener callback.');
		}
	}
}

module.exports = PostmasterGeneral;

// Start up postmaster if this module is run as a script.
if (require.main === module) {
	const postmaster = new PostmasterGeneral();
	postmaster.start();
}
