'use strict';

/**
 * A simple library for making microservice message bus
 * calls using RabbitMQ via amqplib.
 * https://www.npmjs.com/package/amqplib
 * @module lib/postmaster-general
 */

const EventEmitter = require('events');
const amqp = require('amqplib');
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
		this._connection = null;
		this._connecting = false;
		this._shuttingDown = false;
		this._channels = {};
		this._handlers = {};
		this._handlerTimingsTimeout = null;
		this._replyConsumerTag = null;
		this._replyHandlers = {};
		this._shouldConsume = false;
		this._topology = { exchanges: defaults.exchanges, queues: {}, bindings: {} };
		this._createChannel = null;

		// Set options and defaults.
		options = options || {};
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

		// Configure reply queue topology.
		// Reply queue belongs only to this instance, but we want it to survive reconnects. Thus, we set an expiration for the queue.
		const replyQueueName = `postmaster.reply.${this._queuePrefix}.${uuidv4()}`;
		const replyQueueExpiration = (this._connectRetryDelay * this._connectRetryLimit) + (60000 * this._connectRetryLimit);
		this._topology.queues = { reply: { name: replyQueueName, options: { noAck: true, expires: replyQueueExpiration } } };
	}

	/**
	 * Accessor property for retrieving the number of messages being handled, subscribed and replies.
	 */
	get outstandingMessageCount() {
		const listenerCount = Object.keys(this._handlers).reduce((sum, key) => {
			return sum + this._handlers[key].outstandingMessages.size;
		}, 0);

		return listenerCount + Object.keys(this._replyHandlers).length;
	}

	/**
	 * Accessor property for getting the current handler timings.
	 */
	get handlerTimings() {
		const handlerTimings = {};
		for (const key of Object.keys(this._handlers)) {
			const handler = this._handlers[key];
			if (handler.timings) {
				handlerTimings[key] = handler.timings;
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
	 * Called to connect to RabbitMQ and build all channels.
	 * @returns {Promise}
	 */
	async connect() {
		let connectionAttempts = 0;

		const attemptConnect = async () => {
			this._logger.debug('Attempting to connect to RabbitMQ...');

			connectionAttempts++;
			this._connecting = true;

			// We always want to start on a clean-slate when we connect.
			// Cancel outstanding messages, clear all consumers, and reset the connection.
			try {
				this._logger.debug('Closing any existing connections...');
				this._replyConsumerTag = null;
				for (const key of Object.keys(this._handlers)) {
					delete this._handlers[key].consumerTag;
					if (this._handlers[key].outstandingMessages) {
						this._handlers[key].outstandingMessages.clear();
					}
				}
				await this._connection.close();
			} catch (err) {}

			const reconnect = async (err) => {
				try {
					if (!this._connecting && !this._shuttingDown) {
						this._logger.warn(`Lost RabbitMQ connection and will try to reconnect! err: ${err.message}`);
						await attemptConnect();
						await this._assertTopology();
						if (this._shouldConsume) {
							await this.startConsuming();
						}
						this._logger.warn('Restored RabbitMQ connection successfully!');
					}
				} catch (err) {
					const errMessage = `Unable to re-establish RabbitMQ connection! err: ${err.message}`;
					this._logger.error(errMessage);
					this.emit('error', new Error(errMessage));
				}
			};

			try {
				this._logger.debug('Acquiring RabbitMQ connection...');
				this._connection = await amqp.connect(this._url, { heartbeat: this._heartbeat });
				this._connection.on('error', reconnect.bind(this));

				this._createChannel = async () => {
					const channel = await this._connection.createChannel();
					channel.on('error', reconnect.bind(this));
					return channel;
				};

				this._logger.debug('Initializing channels...');
				this._channels = await Promise.props({
					publish: this._createChannel(),
					replyPublish: this._createChannel(),
					topology: this._createChannel(),
					consumers: Promise.reduce(Object.keys(this._topology.bindings), async (consumerMap, key) => {
						const binding = this._topology.bindings[key];
						consumerMap[binding.queue] = await this._createChannel();
						if (binding.options && binding.options.prefetch) {
							await consumerMap[binding.queue].prefetch(binding.options.prefetch);
						}
						return consumerMap;
					}, {})
				});
				this._channels.consumers[this._topology.queues.reply.name] = await this._createChannel();

				connectionAttempts = 0;
				this._connecting = false;
				this._logger.debug('Finished connecting to RabbitMQ!');
			} catch (err) {
				if (connectionAttempts < this._connectRetryLimit) {
					this._logger.warn('Failed to establish RabbitMQ connection! Retrying...');
					await Promise.delay(this._connectRetryDelay);
					return attemptConnect();
				}
				this._logger.error(`Failed to establish RabbitMQ connection after ${connectionAttempts} attempts! err: ${err.message}`);
				throw err;
			}
		};

		await attemptConnect();
		await this._assertTopology();
	}

	/**
	 * Called to safely shutdown the RabbitMQ connection while allowing outstanding messages to process.
	 */
	async shutdown() {
		const shutdownRetryDelay = 1000;
		const retryLimit = this._shutdownTimeout / shutdownRetryDelay;
		let retryAttempts = 0;

		const attempt = async () => {
			retryAttempts++;
			this._shuttingDown = true;

			this._logger.debug('Attempting shutdown...');

			if ((this._connecting || this.outstandingMessageCount > 0) && retryAttempts < retryLimit) {
				if (this._connecting) {
					this._logger.debug('Unable to shutdown while attempting reconnect! Will retry...');
				} else {
					this._logger.debug(`Unable to shutdown while processing ${this.outstandingMessageCount} messages! Will retry...`);
				}
				await Promise.delay(shutdownRetryDelay);
				return attempt();
			}

			try {
				await this.stopConsuming();
			} catch (err) {}

			try {
				await this._connection.close();
			} catch (err) {}

			if (this._handlerTimingsTimeout) {
				clearTimeout(this._handlerTimingsTimeout);
			}

			this._logger.debug('Shutdown completed successfully!');
		};

		try {
			await this.stopConsuming(true);
		} catch (err) {}

		return attempt();
	}

	/**
	 * Asserts an exchange on RabbitMQ, adding it to the list of known topology.
	 * @param {String} name The name of the exchange.
	 * @param {String} type The type of exchange.
	 * @param {Object} [options] Various exchange options.
	 * @returns {Promise} Promise that resolves when the exchange has been asserted.
	 */
	async assertExchange(name, type, options) {
		this._logger.debug(`Asserting exchange name: ${name} type: ${type} options: ${JSON.stringify(options)}`);
		options = options || {};
		await this._channels.topology.assertExchange(name, type, options);
		this._topology.exchanges[name] = { name, type, options };
	}

	/**
	 * Asserts a queue on RabbitMQ, adding it to the list of known topology.
	 * @param {String} name The name of the queue.
	 * @param {Object} [options] Various queue options.
	 * @returns {Promise} Promise that resolves when the queue has been asserted.
	 */
	async assertQueue(name, options) {
		this._logger.debug(`Asserting queue name: ${name} options: ${JSON.stringify(options)}`);
		options = options || {};
		await this._channels.topology.assertQueue(name, options);
		this._topology.queues[name] = { name, options };
	}

	/**
	 * Binds a queue to an exchange, recording the binding in the topology definition.
	 * @param {String} queue The name of the queue to bind.
	 * @param {String} exchange The exchange to bind.
	 * @param {String} topic The routing key to bind.
	 * @param {Object} [options] Various binding options.
	 * @returns {Promise} Promise that resolves when the binding is complete.
	 */
	async assertBinding(queue, exchange, topic, options) {
		this._logger.debug(`Asserting binding queue: ${queue} exchange: ${exchange} topic: ${topic} options: ${JSON.stringify(options)}`);
		options = options || {};
		await this._channels.topology.bindQueue(queue, exchange, topic, options);
		this._topology.bindings[`${queue}_${exchange}`] = { queue, exchange, topic, options };
	}

	/**
	 * Called to assert any RabbitMQ topology after a successful connection is established.
	 * @returns {Promise} Promise resolving when all defined topology has been confirmed.
	 */
	async _assertTopology() {
		const topologyPromises = [];

		this._logger.debug('Asserting pre-defined topology...');

		// Assert exchanges.
		for (const key of Object.keys(this._topology.exchanges)) {
			const exchange = this._topology.exchanges[key];
			this._logger.debug(`Asserting exchange name: ${exchange.name} type: ${exchange.type} options: ${JSON.stringify(exchange.options)}...`);
			topologyPromises.push(this._channels.topology.assertExchange(exchange.name, exchange.type, exchange.options));
		}

		// Assert consumer queues.
		for (const key of Object.keys(this._topology.queues)) {
			const queue = this._topology.queues[key];
			this._logger.debug(`Asserting queue name: ${queue.name} options: ${JSON.stringify(queue.options)}...`);
			topologyPromises.push(this._channels.topology.assertQueue(queue.name, queue.options));
		}

		// Await all assertions before asserting bindings.
		await Promise.all(topologyPromises);

		// Bind listeners.
		await Promise.map(Object.keys(this._topology.bindings), (key) => {
			const binding = this._topology.bindings[key];
			this._logger.debug(`Asserting binding queue: ${binding.queue} exchange: ${binding.exchange} topic: ${binding.topic} options: ${JSON.stringify(binding.options)}...`);
			return this._channels.topology.bindQueue(binding.queue, binding.exchange, binding.topic, binding.options);
		});

		this._logger.debug('Finished asserting defined topology!');
	}

	/**
	 * Called to start consuming incoming messages from all consumer channels.
	 * @returns {Promise} Promise that resolves when all consumers have begun consuming.
	 */
	async startConsuming() {
		this._shouldConsume = true;

		this._logger.debug('Starting up consumers...');

		this._resetHandlerTimings();

		// Since the reply queue isn't bound to an exchange, we need to handle it separately.
		if (this._topology.queues.reply) {
			const replyQueue = this._topology.queues.reply;
			this._replyConsumerTag = await this._channels.consumers[replyQueue.name].consume(replyQueue.name, this._handleReply.bind(this), replyQueue.options);
			this._replyConsumerTag = this._replyConsumerTag.consumerTag;
			this._logger.debug(`Starting consuming from reply queue: ${replyQueue.name}...`);
		}

		await Promise.map(Object.keys(this._topology.bindings), async (key) => {
			const binding = this._topology.bindings[key];
			const consumerTag = await this._channels.consumers[binding.queue].consume(binding.queue, this._handlers[binding.topic].callback.bind(this), binding.options);
			this._handlers[binding.topic].consumerTag = consumerTag.consumerTag;
			this._logger.debug(`Starting consuming from queue: ${binding.queue}...`);
		});

		this._logger.debug('Finished starting all consumers!');
	}

	/**
	 * Called to stop consuming incoming messages from all channels.
	 * @param {Boolean} [awaitReplies] If truthy, this function will skip cancelling the reply consumer.
	 * @returns {Promise} Promise that resolves when all consumers have stopped consuming.
	 */
	async stopConsuming(awaitReplies) {
		this._shouldConsume = false;

		this._logger.debug('Starting to cancel all consumers...');

		this._resetHandlerTimings();

		if (this._replyConsumerTag && !awaitReplies) {
			await this._channels.consumers[this._topology.queues.reply.name].cancel(this._replyConsumerTag);
			this._replyConsumerTag = null;
			this._logger.debug(`Stopped consuming from queue ${this._topology.queues.reply.name}...`);
		}

		await Promise.map(Object.keys(this._topology.bindings), async (key) => {
			const binding = this._topology.bindings[key];
			if (this._handlers[binding.topic] && this._handlers[binding.topic].consumerTag) {
				const consumerTag = JSON.parse(JSON.stringify(this._handlers[binding.topic].consumerTag));
				delete this._handlers[binding.topic].consumerTag;
				await this._channels.consumers[binding.queue].cancel(consumerTag);
				this._logger.debug(`Stopped consuming from queue ${binding.queue}...`);
			}
		});

		this._logger.debug('Finished consumer shutdown!');
	}

	/**
	 * A "safe", promise-based method for acknowledging messages that is guaranteed to resolve.
	 * @param {String} queueName The queue name of the channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to acknowledge.
	 * @param {String} pattern The routing key of the message.
	 * @param {Object} [reply] The request body of the response message to send.
	 * @param {Boolean} [noAck] If truthy, skip the ack.
	 * @returns {Promise} Promise that resolves when the message is acknowledged.
	 */
	async _ackMessageAndReply(queueName, msg, pattern, reply, noAck) { // eslint-disable-line max-params
		try {
			this._logger.debug(`Attempting to acknowledge message: ${pattern} messageId: ${msg.properties.messageId}`);

			const topic = this._resolveTopic(pattern);
			if (this._handlers[topic] && this._handlers[topic].outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
				if (!noAck) {
					this._channels.consumers[queueName].ack(msg);
				}
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
					this._channels.replyPublish.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(reply)), options);
				}
				this._handlers[topic].outstandingMessages.delete(`${pattern}_${msg.properties.messageId}`);
			} else {
				this._logger.warn(`Skipping message ack due to connection failure! message: ${pattern} messageId: ${msg.properties.messageId}`);
			}
		} catch (err) {
			this._logger.warn(`Failed to ack a message! message: ${pattern} messageId: ${msg.properties.messageId} err: ${err.message}`);
		}
	}

	/**
	 * A "safe", promise-based method for nacking messages that is guaranteed to resolve.
	 * Nacked messages will not be requeued.
	 * @param {String} queueName The queue name of the channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to nack.
	 * @param {String} pattern The routing key of the message.
	 * @param {String} [reply] The error message to end in reply.
	 * @param {Boolean} [noAck] If truthy, skip the ack.
	 * @returns {Promise} Promise that resolves when the message is nacked.
	 */
	async _nackMessageAndReply(queueName, msg, pattern, reply, noAck) { // eslint-disable-line max-params
		try {
			this._logger.debug(`Attempting to acknowledge message: ${pattern} messageId: ${msg.properties.messageId}`);

			const topic = this._resolveTopic(pattern);
			if (this._channels.consumers[queueName] && this._handlers[topic] && this._handlers[topic].outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
				if (!noAck) {
					this._channels.consumers[queueName].nack(msg, false, false);
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

	/**
	 * A "safe", promise-based method for rejecting messages that is guaranteed to resolve.
	 * Rejected messages will be requeued for retry.
	 * @param {String} queueName The queueName of the channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to reject.
	 * @param {String} pattern The routing key of the message.
	 * @param {Boolean} [noAck] If truthy, skip the nack.
	 * @returns {Promise} Promise that resolves when the message is rejected.
	 */
	async _rejectMessage(queueName, msg, pattern, noAck) {
		try {
			this._logger.debug(`Attempting to acknowledge message: ${pattern} messageId: ${msg.properties.messageId}`);

			const topic = this._resolveTopic(pattern);
			if (this._channels.consumers[queueName] && this._handlers[topic] && this._handlers[topic].outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
				if (!noAck) {
					this._channels.consumers[queueName].reject(msg);
				}
				this._handlers[topic].outstandingMessages.delete(`${pattern}_${msg.properties.messageId}`);
			} else {
				this._logger.warn(`Skipping message rejection due to connection failure! message: ${pattern} messageId: ${msg.properties.messageId}`);
			}
		} catch (err) {
			this._logger.warn(`Failed to reject a message! message: ${pattern} messageId: ${msg.properties.messageId} err: ${err.message}`);
		}
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
	 * Updates the timing data for message handler callbacks.
	 * @param {String} pattern The pattern to record timing data for.
	 * @param {Date} start The time at which the handler started execution.
	 */
	_setHandlerTiming(pattern, start) {
		this._logger.debug(`Setting handler timings for message: ${pattern}`);
		const elapsed = new Date().getTime() - start;
		this._handlers[pattern].timings = this._handlers[pattern].timings || {
			messageCount: 0,
			elapsedTime: 0,
			minElapsedTime: 0,
			maxElapsedTime: 0
		};
		this._handlers[pattern].timings.messageCount++;
		this._handlers[pattern].timings.elapsedTime += elapsed;

		if (this._handlers[pattern].timings.minElapsedTime > elapsed ||
			this._handlers[pattern].timings.minElapsedTime === 0) {
			this._handlers[pattern].timings.minElapsedTime = elapsed;
		}
		if (this._handlers[pattern].timings.maxElapsedTime < elapsed) {
			this._handlers[pattern].timings.maxElapsedTime = elapsed;
		}
	}

	/**
	 * Resets the handler timings to prevent unbounded accumulation of stale data.
	 */
	_resetHandlerTimings() {
		this._logger.debug('Resetting handler timings.');
		for (const key of Object.keys(this._handlers)) {
			delete this._handlers[key].timings;
		}

		// If we're not manually managing the timing refresh, schedule the next timeout.
		if (this._handlerTimingResetInterval) {
			this._logger.debug(`Setting next timing refresh in ${this._handlerTimingResetInterval} ms.`);
			this._handlerTimingsTimeout = setTimeout(() => {
				this._resetHandlerTimings();
			}, this._handlerTimingResetInterval);
		}
	}

	/**
	 * Adds a new listener for the specified pattern, asserting any associated topology.
	 * @param {String} pattern The pattern to bind to.
	 * @param {Function} callback The callback function to handle messages. This function MUST return a promise!
	 * @param {Object} [options] Additional options for queues, exchanges, and binding.
	 * @returns {Promise} A promise that resolves when the listener has been added.
	 */
	async addRabbitMQListener(pattern, callback, options) {
		this._logger.debug(`Starting to add a listener for message: ${pattern}...`);
		options = options || {};

		const topic = this._resolveTopic(pattern);

		// Configure queue options.
		options.queue = options.queue || {};
		options.queue.deadLetterExchange = options.deadLetterExchange || this._deadLetterExchange;
		options.queue.deadLetterRoutingKey = topic;

		const queueName = (options.queue.prefix || this._queuePrefix) + '.' + topic;

		// Configure exchange options
		options.exchange = options.exchange || this._defaultExchange;

		// Grab a channel and assert the topology.
		this._logger.debug(`Asserting listener topology for message: ${pattern}...`);
		if (!this._channels.consumers[queueName]) {
			this._channels.consumers[queueName] = await this._createChannel();
		}
		await this.assertExchange(options.exchange.name, options.exchange.type, options.exchange);
		await this.assertQueue(queueName, options.queue);
		await this.assertBinding(queueName, options.exchange.name, topic, options.binding);

		options.binding = options.binding || {};
		if (options.binding.prefetch) {
			await this._channels.consumers[queueName].prefetch(options.binding.prefetch);
		}

		// Define the callback handler.
		this._handlers[topic] = { outstandingMessages: new Set() };
		this._handlers[topic].callback = async (msg) => {
			const start = new Date().getTime();

			try {
				msg.properties = msg.properties || {};
				msg.properties.headers = msg.properties.headers || {};
				msg.properties.messageId = msg.properties.messageId || msg.properties.correlationId;
				msg.properties.correlationId = msg.properties.correlationId || msg.properties.messageId;

				this._logger.debug(`Handling incoming message: ${pattern} messageId: ${msg.properties.messageId}...`);

				this._handlers[topic].outstandingMessages.add(`${pattern}_${msg.properties.messageId}`);

				let body = (msg.content || '{}').toString();
				body = JSON.parse(body);

				const reply = await callback(body, msg.properties.headers);
				await this._ackMessageAndReply(queueName, msg, pattern, reply, options.binding.noAck);
				this._setHandlerTiming(topic, start);
				this._logger.debug(`Finished handling incoming message: ${pattern} messageId: ${msg.properties.messageId}.`);
			} catch (err) {
				this._logger.error(`Message handler failed and cannot retry! message: ${pattern} err: ${err.message}`);
				await this._nackMessageAndReply(queueName, msg, pattern, err.message, options.binding.noAck);
				this._setHandlerTiming(topic, start);
			}
		};

		if (this._shouldConsume) {
			const binding = this._topology.bindings[`${queueName}_${options.exchange.name}`];
			const consumerTag = await this._channels.consumers[binding.queue].consume(binding.queue, this._handlers[binding.topic].callback.bind(this), binding.options);
			this._handlers[binding.topic].consumerTag = consumerTag.consumerTag;
			this._logger.debug(`Starting consuming from queue: ${binding.queue}...`);
		}

		this._logger.debug(`Finished adding a listener for message: ${pattern}.`);
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
	 * Publishes a fire-and-forget message that doesn't wait for an explicit response.
	 * @param {String} routingKey The routing key to attach to the message.
	 * @param {Object} [message] The message data to publish.
	 * @param {Object} [options] Optional publishing options.
	 * @returns {Promise} A promise that resolves when the message is published or publishing has failed.
	 */
	async publish(routingKey, message, options) {
		try {
			let publishAttempts = 0;

			// Set default publishing options.
			options = options || {};
			options.contentType = 'application/json';
			options.contentEncoding = 'utf8';
			options.messageId = options.messageId || uuidv4();
			options.timestamp = new Date().getTime();

			const exchange = options.exchange || this._defaultExchange.name;
			const msgBody = JSON.stringify(message || '{}');
			const msgData = Buffer.from(msgBody);

			const attempt = async (skipIncrement) => {
				this._logger.debug(`Attempting to publish fire-and-forget message: ${routingKey}...`);
				if (!skipIncrement) {
					publishAttempts++;
				}

				if (this._connecting) {
					this._logger.debug(`Unable to publish fire-and-forget message: ${routingKey} while reconnecting. Will retry...`);
					await Promise.delay(this._publishRetryDelay);
					return attempt(true);
				}

				try {
					const published = this._channels.publish.publish(exchange, this._resolveTopic(routingKey), msgData, options);
					if (published) {
						publishAttempts = 0;
					} else {
						throw new Error(`Publish buffer full!`);
					}
				} catch (err) {
					if (publishAttempts < this._publishRetryLimit) {
						this._logger.debug(`Failed to publish fire-and-forget message and will retry! message: ${routingKey} err: ${err.message}`);
						await Promise.delay(this._publishRetryDelay);
						return attempt();
					}
					throw err;
				}
			};

			await attempt();
		} catch (err) {
			this._logger.error(`Failed to publish a fire-and-forget message! message: ${routingKey} err: ${err.message}`);
		}
	}

	/**
	 * Publishes an RPC-style message that waits for a response.
	 * @param {String} routingKey The routing key to attach to the message.
	 * @param {Object} [message] The message data to publish.
	 * @param {Object} [options] Optional publishing options.
	 * @returns {Promise} A promise that resolves when the message is successfully published and a reply is received.
	 */
	async request(routingKey, message, options) {
		let publishAttempts = 0;

		// Set default publishing options.
		options = options || {};
		options.contentType = 'application/json';
		options.contentEncoding = 'utf8';
		options.messageId = options.messageId || uuidv4();
		options.correlationId = options.correlationId || options.messageId;
		options.replyTo = this._topology.queues.reply.name;
		options.timestamp = new Date().getTime();

		const exchange = options.exchange || this._defaultExchange.name;
		const msgData = Buffer.from(JSON.stringify(message || '{}'));

		const attempt = async (skipIncrement) => {
			this._logger.debug(`Attempting to publish RPC message: ${routingKey}...`);
			if (!skipIncrement) {
				publishAttempts++;
			}

			if (this._connecting) {
				this._logger.debug(`Unable to publish RPC message: ${routingKey} while reconnecting. Will retry...`);
				await Promise.delay(this._publishRetryDelay);
				return attempt(true);
			}

			try {
				const published = this._channels.publish.publish(exchange, this._resolveTopic(routingKey), msgData, options);
				if (published) {
					publishAttempts = 0;
				} else {
					throw new Error(`Publish buffer full!`);
				}

				return new Promise((resolve, reject) => {
					this._replyHandlers[options.correlationId] = (err, data) => {
						if (err) {
							reject(err);
						} else {
							resolve(data);
						}
					};
				}).timeout(this._replyTimeout);
			} catch (err) {
				if (publishAttempts < this._publishRetryLimit) {
					this._logger.debug(`Failed to publish RPC message and will retry! message: ${routingKey} err: ${err.message}`);
					await Promise.delay(this._publishRetryDelay);
					return attempt();
				}
				this._logger.debug(`Failed to publish RPC message: ${routingKey} err: ${err.message}`);
				throw err;
			}
		};

		return attempt();
	}
}

module.exports = PostmasterGeneral;
