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
const defaults = require('./defaults');

class PostmasterGeneral extends EventEmitter {
	/**
	 * Constructor function for the PostmasterGeneral object.
	 * @param {object} [options]
	 */
	constructor(options) {
		super();

		// Set initial state values.
		this._connectTimeout = null;
		this._connection = null;
		this._connecting = false;
		this._shuttingDown = false;
		this._channels = {};
		this._handlerTimingsTimeout = null;
		this._topologyCheckTimeout = null;
		this._topologyCheckInterval = null;
		this._replyConsumerTag = null;
		this._replyHandlers = {};
		this._shouldConsume = false;
		this._topology = { exchanges: defaults.exchanges, queues: {}, bindings: {} };
		this._createChannel = null;

		// Set options and defaults.
		options = options || {};
		this._connectTimeout = options.connectTimeout || defaults.connectTimeout;
		this._connectRetryDelay = typeof options.connectRetryDelay === 'undefined' ? defaults.connectRetryDelay : options.connectRetryDelay;
		this._connectRetryLimit = typeof options.connectRetryLimit === 'undefined' ? defaults.connectRetryLimit : options.connectRetryLimit;
		this._deadLetterExchange = options.deadLetterExchange || defaults.deadLetterExchange;
		this._defaultExchange = defaults.exchanges.topic;
		this._handlerTimingResetInterval = options.handlerTimingResetInterval || defaults.handlerTimingResetInterval;
		this._heartbeat = typeof options.heartbeat === 'undefined' ? defaults.heartbeat : options.heartbeat;
		this._publishRetryDelay = typeof options.publishRetryDelay === 'undefined' ? defaults.publishRetryDelay : options.publishRetryDelay;
		this._publishRetryLimit = typeof options.publishRetryLimit === 'undefined' ? defaults.publishRetryLimit : options.publishRetryLimit;
		this._removeListenerRetryDelay = typeof options.removeListenerRetryDelay === 'undefined' ? defaults.removeListenerRetryDelay : options.removeListenerRetryDelay;
		this._removeListenerRetryLimit = typeof options.removeListenerRetryLimit === 'undefined' ? defaults.removeListenerRetryLimit : options.removeListenerRetryLimit;
		this._replyTimeout = this._publishRetryDelay * this._publishRetryLimit * 2;
		this._queuePrefix = options.queuePrefix || defaults.queuePrefix;
		this._shutdownTimeout = options.shutdownTimeout || defaults.shutdownTimeout;
		this._topologyCheckInterval = options.topologyCheckInterval || defaults.topologyCheckInterval;
		this._url = options.url || defaults.url;

		// Configure the logger.
		log4js.configure({
			appenders: { out: { type: 'stdout' } },
			categories: { default: { appenders: ['out'], level: options.logLevel ? options.logLevel : defaults.logLevel } },
			pm2: options.pm2,
			pm2InstanceVar: options.pm2InstanceVar
		});
		this._logger = log4js.getLogger('postmaster-general');
		this._logger.level = options.logLevel ? options.logLevel : defaults.logLevel;
	}

	/**
	 * Accessor property for retrieving the number of messages being handled, subscribed and replies.
	 */
	get outstandingMessageCount() {
		const listenerCount = Object.keys(this._channels.consumers).reduce((sum, key) => {
			return sum + this._channels.consumers[key].outstandingMessages.size;
		}, 0);

		return listenerCount + Object.keys(this._replyHandlers).length;
	}

	/**
	 * Accessor property for getting the current handler timings.
	 */
	get handlerTimings() {
		const handlerTimings = {};
		for (const key of Object.keys(this._channels.consumers)) {
			if (this._channels.consumers[key].timings) {
				handlerTimings[key] = this._channels.consumers[key].timings;
			}
		}
		return handlerTimings;
	}

	/**
	 * Called to resolve the RabbitMQ topic key corresponding to an address.
	 * @param {String} pattern
	 */
	_resolveTopic(pattern) {
		return pattern.replace(/:/g, '.');
	}

	/**
	 * Called to connect to RabbitMQ and build default channels.
	 */
	connect() {
		// Build the connection.
		this._connection = amqp.connect([this._url], {
			heartbeatIntervalInSeconds: this._heartbeat / 1000
		});
		this._connection.on('connect', (connection, url) => {
			this._logger.debug(`Connected to RabbitMQ. url: ${url}`);
		});
		this._connection.on('disconnect', (err) => {
			if (err) {
				this._logger.error(`Lost connection to RabbitMQ. err: ${err.message}`);
			} else {
				this._logger.debug('Disconnected from RabbitMQ.');
			}
		});

		// Build the reply channel.
		const replyQueueName = `postmaster.reply.${this._queuePrefix}.${uuidv4()}`;
		this._channels.consumers[replyQueueName] = {
			channel: this._connection.createChannel({
				json: true,
				setup: (channel) => {
					return channel.assertQueue(replyQueueName, { expires: this._heartbeat });
				}
			}),
			options: {
				noAck: true
			},
			isReply: true
		};
	}

	async nackMessageAndReply(queueName, msg, pattern, reply, noAck) { // eslint-disable-line max-params
		try {
			this._logger.debug(`Attempting to acknowledge message: ${pattern} messageId: ${msg.properties.messageId}`);

			const topic = this._resolveTopic(pattern);
			if (this._handlers[topic] && this._handlers[topic].outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
				if (!noAck) {
					this.channel.nack(msg, false, false);
				}
				if (msg.properties.replyTo && msg.properties.correlationId) {
					reply = reply || 'An unknown error occurred during processing!';
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
					this._channels.replyPublish.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify({ err: reply })), options);
				}
				this._handlers[topic].outstandingMessages.delete(`${pattern}_${msg.properties.messageId}`);
			} else {
				this._logger.warn(`Skipping message nack due to connection failure! message: ${pattern} messageId: ${msg.properties.messageId}`);
			}
		} catch (err) {
			this._logger.warn(`Failed to nack a message! message: ${pattern} messageId: ${msg.properties.messageId} err: ${err.message}`);
		}
	}

	async addListener(pattern, callback, options) {
		options = options || {};

		const topic = this._resolveTopic(pattern);

		if (this._shouldConsume) {
			const binding = this._topology.bindings[`${queueName}_${options.exchange.name}`];
			const consumerTag = await this._channels.consumers[binding.queue].consume(binding.queue, this._handlers[binding.topic].callback.bind(this), binding.options);
			this._handlers[binding.topic].consumerTag = consumerTag.consumerTag;
			this._logger.debug(`Starting consuming from queue: ${binding.queue}...`);
		}

		this._logger.debug(`Finished adding a listener for message: ${pattern}.`);
	}

	/**
	 * Called to start consuming incoming messages from all consumer channels.
	 * @returns {Promise} Promise that resolves when all consumers have begun consuming.
	 */
	startConsuming() {
		this._shouldConsume = true;

		this._logger.debug('Starting up consumers...');

		this._resetHandlerTimings();

		return Promise.map(Object.keys(this._channels.consumers), (key) => {
			const consumer = this._channels.consumers[key];
			return consumer.channel.addSetup(async (channel) => {
				const result = await channel.consume(key, consumer.callback.bind(this), consumer.options);
				consumer.consumerTag = result.consumerTag;
			});
		});
	}

	/**
	 * Called to stop consuming incoming messages from all channels.
	 * @param {Boolean} [awaitReplies] If truthy, this function will skip cancelling the reply consumer.
	 * @returns {Promise} Promise that resolves when all consumers have stopped consuming.
	 */
	async stopConsuming(awaitReplies) {
		this._shouldConsume = false;

		this._logger.debug('Cancelling consumers...');

		if (this._replyConsumerTag && !awaitReplies) {
			await this._channels.consumers[this._topology.queues.reply.name].cancel(this._replyConsumerTag);
			this._replyConsumerTag = null;
			this._logger.debug(`Stopped consuming from queue ${this._topology.queues.reply.name}...`);
		}

		return Promise.map(Object.keys(this._channels.consumers), async (key) => {
			const consumer = this._channels.consumers[key];
			if (consumer.consumerTag) {
				const consumerTag = JSON.parse(JSON.stringify(consumer.consumerTag));
				delete consumer.consumerTag;
				await consumer.channel.cancel(consumerTag);
				this._logger.debug(`Stopped consuming from queue ${key}...`);
			}
		});
	}

	/**
	 * Called to gracefully shutdown the RabbitMQ connection while allowing outstanding messages to process.
	 */
	async shutdown() {
		const shutdownRetryDelay = 1000;
		const retryLimit = this._shutdownTimeout / shutdownRetryDelay;
		let retryAttempts = 0;

		if (this._shuttingDown) {
			return;
		}

		const attempt = async () => {
			retryAttempts++;
			this._shuttingDown = true;

			this._logger.debug('Attempting shutdown...');

			if (this.outstandingMessageCount > 0 && retryAttempts < retryLimit) {
				this._logger.debug(`Unable to shutdown while processing ${this.outstandingMessageCount} messages! Will retry...`);
				await Promise.delay(shutdownRetryDelay);
				return attempt();
			}

			await this.stopConsuming();

			for (const key of Object.keys(this._channels)) {
				this._channels[key].close();
			}
			if (this._connection) {
				this._connection.close();
			}

			if (this._handlerTimingsTimeout) {
				clearTimeout(this._handlerTimingsTimeout);
			}

			this._logger.debug('Shutdown completed successfully!');
			this.emit('close');
		};

		await this.stopConsuming(true);
		return attempt();
	}

	/**
	 * Called to handle a reply to an RPC-style message.
	 * @param {Object} msg The RabbitMQ message.
	 */
	_handleReply(msg) {
		msg = msg || {};

		let body;
		try {
			body = (msg.content || '{}').toString();
			body = JSON.parse(body);
		} catch (err) {
			body.err = 'Failed to parse message body due to invalid JSON!';
		}

		msg.properties = msg.properties || {};
		msg.properties.headers = msg.properties.headers || {};

		this._logger.debug(`Attempting to handle incoming reply. correlationId: ${msg.properties.correlationId || 'undefined'}`);

		if (!msg.properties.correlationId || !this._replyHandlers[msg.properties.correlationId] || msg.properties.replyTo !== this._topology.queues.reply.name) {
			this._logger.warn(`Reply handler received an invalid reply! correlationId: ${msg.properties.correlationId}`);
		} else {
			if (body.err) {
				this._replyHandlers[msg.properties.correlationId](new Error(body.err));
			} else {
				this._replyHandlers[msg.properties.correlationId](null, body);
			}
			delete this._replyHandlers[msg.properties.correlationId];
			this._logger.debug(`Finished processing reply. correlationId: ${msg.properties.correlationId || 'undefined'}`);
		}
	}

	/**
	 * Called to remove a listener. Note that this call DOES NOT delete any queues
	 * or exchanges. It is recommended that these constructs be made to auto-delete or expire
	 * if they are not intended to be persistent.
	 * @param {String} pattern The pattern to match.
	 * @param {String} [exchange] The name of the exchange to remove the binding.
	 * @param {String} [prefix] The queue prefix to match.
	 * @returns {Promise} Promise that resolves when the listener has been removed.
	 */
	async removeRabbitMQListener(pattern, exchange, prefix) {
		let attempts = 0;

		const topic = this._resolveTopic(pattern);
		exchange = exchange || this._defaultExchange.name;
		const queueName = (prefix || this._queuePrefix) + '.' + topic;

		const attempt = async (skipIncrement) => {
			if (!skipIncrement) {
				attempts++;
			}

			this._logger.debug(`Attempting to remove a listener for message: ${pattern}...`);

			if (this._connecting) {
				this._logger.debug(`Cannot remove a listener for message: ${pattern} while reconnecting! Will retry in ${this._removeListenerRetryDelay} ms...`);
				await Promise.delay(this._removeListenerRetryDelay);
				return attempt(true);
			}

			try {
				if (this._channels.consumers[queueName]) {
					this._logger.debug(`Cancelling consumer for message: ${pattern}...`);
					if (this._handlers[topic].consumerTag) {
						await this._channels.consumers[queueName].cancel(this._handlers[topic].consumerTag);
					}
					this._handlers[topic].outstandingMessages.clear();
					await this._channels.consumers[queueName].close();
					delete this._channels.consumers[queueName];
				}
				delete this._handlers[topic];
				delete this._topology.bindings[`${queueName}_${exchange}`];
				this._logger.debug(`Finished removing listener for message: ${pattern}.`);
			} catch (err) {
				if (attempts < this._removeListenerRetryLimit) {
					await Promise.delay(this._removeListenerRetryDelay);
					return attempt();
				}
				throw err;
			}
		};

		return attempt();
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
