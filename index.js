'use strict';

/**
 * A simple library for making microservice message bus
 * calls using RabbitMQ via amqplib.
 * https://www.npmjs.com/package/amqplib
 * @module lib/postmaster-general
 */

const EventEmitter = require('events');
const amqp = require('amqp-connection-manager');
const log4js = require('log4js');
const Promise = require('bluebird');
const uuidv4 = require('uuid/v4');
const Publisher = require('publisher');
const TopicConsumer = require('topic-consumer');
const defaults = require('./defaults');

class PostmasterGeneral extends EventEmitter {
	/**
	 * Constructor function for the PostmasterGeneral object.
	 * @param {object} [options]
	 */
	constructor(options) {
		super();

		this.connection = null;
		this.publisher = null;
		this.consumers = null;

		this.shuttingDown = false;
		this.replyHandlers = {};
		this.consuming = false;
		this.topology = { exchanges: defaults.exchanges, queues: {}, bindings: {} };
		this.createChannel = null;

		// Set options and defaults.
		options = options || {};
		this.connectRetryDelay = typeof options.connectRetryDelay === 'undefined' ? defaults.connectRetryDelay : options.connectRetryDelay;
		this.connectRetryLimit = typeof options.connectRetryLimit === 'undefined' ? defaults.connectRetryLimit : options.connectRetryLimit;
		this.deadLetterExchange = options.deadLetterExchange || defaults.deadLetterExchange;
		this.defaultExchange = defaults.exchanges.topic;
		this.handlerTimingResetInterval = options.handlerTimingResetInterval || defaults.handlerTimingResetInterval;
		this.heartbeat = typeof options.heartbeat === 'undefined' ? defaults.heartbeat : options.heartbeat;
		this.publishRetryDelay = typeof options.publishRetryDelay === 'undefined' ? defaults.publishRetryDelay : options.publishRetryDelay;
		this.publishRetryLimit = typeof options.publishRetryLimit === 'undefined' ? defaults.publishRetryLimit : options.publishRetryLimit;
		this.removeListenerRetryDelay = typeof options.removeListenerRetryDelay === 'undefined' ? defaults.removeListenerRetryDelay : options.removeListenerRetryDelay;
		this.removeListenerRetryLimit = typeof options.removeListenerRetryLimit === 'undefined' ? defaults.removeListenerRetryLimit : options.removeListenerRetryLimit;
		this.replyTimeout = this.publishRetryDelay * this.publishRetryLimit * 2;
		this.queuePrefix = options.queuePrefix || defaults.queuePrefix;
		this.shutdownTimeout = options.shutdownTimeout || defaults.shutdownTimeout;
		this.url = options.url || defaults.url;

		// Configure the logger.
		log4js.configure({
			appenders: { out: { type: 'stdout' } },
			categories: { default: { appenders: ['out'], level: options.logLevel ? options.logLevel : defaults.logLevel } },
			pm2: options.pm2,
			pm2InstanceVar: options.pm2InstanceVar
		});
		this.logger = log4js.getLogger('postmaster-general');
		this.logger.level = options.logLevel ? options.logLevel : defaults.logLevel;
	}

	/**
	 * Accessor property for retrieving the number of messages being handled, subscribed and replies.
	 */
	get outstandingMessageCounts() {
		const counts = {
			publisher: this.publisher.outstandingMessageCount
		};

		for (const key of Object.keys(this.consumers)) {
			counts[key] = this.consumers[key].outstandingMessageCount;
		}

		return counts;
	}

	/**
	 * Accessor property for getting the current handler timings.
	 */
	get handlerTimings() {
		const handlerTimings = {};
		for (const key of Object.keys(this.consumers)) {
			if (this.consumers[key].timings) {
				handlerTimings[key] = this.consumers[key].timings;
			}
		}
		return handlerTimings;
	}

	/**
	 * Called to resolve the RabbitMQ topic key corresponding to an address.
	 * @param {String} pattern
	 */
	resolveTopic(pattern) {
		return pattern.replace(/:/g, '.');
	}

	/**
	 * Called to connect to RabbitMQ and build default channels.
	 */
	async connect() {
		this.logger.debug(`Connecting to RabbitMQ...`);

		// Build the connection.
		this.connection = amqp.connect([this.url], {
			heartbeatIntervalInSeconds: this.heartbeat / 1000
		});
		this.connection.on('connect', (connection, url) => {
			this.logger.debug(`Connected to RabbitMQ. url: ${url}`);
		});
		this.connection.on('disconnect', (err) => {
			if (err) {
				this.logger.error(`Lost connection to RabbitMQ. err: ${err.message}`);
			} else {
				this.logger.debug('Disconnected from RabbitMQ.');
			}
		});

		// Build the publisher.
		const options = {
			queue: {
				prefix: this.queuePrefix,
				expires: this.heartbeat
			},
			defaultExchange: this.defaultExchange,
			replyTimeout: this.replyTimeout
		};
		this.publisher = new Publisher(this.connection, options);
		this.publisher.on('error', (err, data) => this.emit('error', err, { name: data.name }));
		this.publisher.on('unhandledReply', (data) => this.logger.debug(`Received unhandled reply. correlationId: ${data.correlationId} body: ${JSON.stringify(body)}`));
		await this.publisher.waitForConnect();
	}

	/**
	 * Called to add a new listener for a message.
	 * @param {string} pattern - The pattern to match.
	 * @param {function} - The callback function.
	 * @param {object} [options] - Consumer and queue options.
	 */
	async addListener(pattern, callback, options) {
		const topic = this.resolveTopic(pattern);

		this.logger.debug(`Adding a listener for topic: ${topic}...`);

		// Build the queue options.
		options = options || {};
		options.queue = options.queue || {};
		options.queue.prefix = options.queue.prefix || this.queuePrefix;
		options.queue.deadLetterExchange = options.queue.deadLetterExchange || this.deadLetterExchange;

		// Build the exchange options.
		options.exchange = options.exchange || {};
		options.exchange.name = options.exchange.name || this.defaultExchange;
		options.exchange.type = 'topic';

		// Build misc options.
		options.handlerTimingResetInterval = options.handlerTimingResetInterval || this.handlerTimingResetInterval;

		// Create and start the consumer.
		const consumer = new TopicConsumer(this.connection, topic, callback, options);
		consumer.on('error', (err, data) => this.emit('error', err, { name: data.name }));
		consumer.on('ackError', (data) => this.emit('error', err, { name: data.name }));
		
		if (this.consuming) {
			await consumer.startConsuming();
		}
		this.consumers[`${options.queue.prefix}.${topic}`];
	}

	/**
	 * Called to remove a listener. Note that this call DOES NOT delete any queues
	 * or exchanges. It is recommended that these constructs be made to auto-delete or expire
	 * if they are not intended to be persistent.
	 * @param {string} pattern - The pattern to match.
	 * @param {string} [prefix] - The queue prefix to match.
	 * @returns {promise}
	 */
	async removeListener(pattern, prefix) {
		const topic = this.resolveTopic(pattern);

		this.logger.debug(`Removing a listener for topic: ${topic}...`);

		const queueName = `${(prefix || this.queuePrefix)}.${topic}`;

		if (this.consumers[queueName]) {
			if (this.consuming) {
				await this.consumers[queueName].stopConsuming();
			}
			await this.consumers[queueName].close();
			delete this.consumers[queueName];
		}
	}

	/**
	 * Called to start consuming incoming messages from all consumer channels.
	 * @returns {promise}
	 */
	async startConsuming() {
		this.logger.debug('Starting consumers...');
		
		this.consuming = true;

		await Promise.all([
			this.publisher.startConsuming(),
			Promise.map(Object.keys(this.consumers), async (key) => {
				await this.consumers[key].startConsuming();
			})
		]);
	}

	/**
	 * Called to stop consuming incoming messages from all channels.
	 * @returns {promise}
	 */
	async stopConsuming() {
		this.logger.debug('Stopping consumers...');

		this.consuming = false;

		await Promise.all([
			this.publisher.stopConsuming(),
			Promise.map(Object.keys(this.consumers), async (key) => {
				await this.consumers[key].stopConsuming();
			})
		]);
	}

	/**
	 * Called to gracefully shutdown the RabbitMQ connections.
	 */
	async shutdown() {
		this.logger.debug('Shutting down...');

		await this.stopConsuming();

		await Promise.all([
			this.publisher.close(),
			Promise.map(Object.keys(this.consumers), async (key) => {
				await this.consumers[key].close();
			})
		]);
		await this.connection.close();

		this.logger.debug('Shutdown completed successfully.');
	}

	

	/**
	 * Called to send a reply to a message.
	 * @param {object} msg - The RabbitMQ message to acknowledge.
	 * @fires Consumer#replyError
	 * @returns {promise}
	 */
	async reply(topic, msg, reply) {
		try {
			msg.properties = msg.properties || {};
			msg.properties.headers = msg.properties.headers || {};

			if (msg.properties.replyTo && msg.properties.correlationId) {
				reply = reply || {};
				const options = {
					contentType: 'application/json',
					contentEncoding: 'utf8',
					messageId: msg.properties.messageId,
					correlationId: msg.properties.correlationId,
					timestamp: new Date().getTime(),
					replyTo: msg.properties.replyTo,
					type: msg.properties.type,
					headers: msg.properties.headers
				};
				await this.replyChannel.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(reply)), options);
			}
		} catch (err) {
			/**
			 * ACK error event.
			 * @event Consumer#replyError
			 * @type {object}
			 * @property {string} topic - The message's routing key.
			 * @property {string} messageId - The unique id of the message.
			 * @property {string} correlationId - The correlation id of the message.
			 * @property {string} replyTo - The return address of the message.
			 * @property {string} err - The error message.
			 */
			this.emit('replyError', {
				topic: topic,
				messageId: msg.properties.messageId,
				correlationId: msg.properties.correlationId,
				replyTo: msg.properties.replyTo,
				err: err.message
			});
		}
	}
}

module.exports = PostmasterGeneral;
