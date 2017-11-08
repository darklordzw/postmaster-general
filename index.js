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
		this._channels = {};
		this._handlers = {};
		this._handlerTimingsTimeout = null;
		this._replyConsumerTag = null;
		this._replyHandlers = {};
		this._shouldConsume = false;
		this._topology = { exchanges: defaults.exchanges };
		this._createChannel = null;

		// Set options and defaults.
		options = options || {};
		this._connectRetryDelay = typeof options.connectRetryDelay === 'undefined' ? defaults.connectRetryDelay : options.connectRetryDelay;
		this._connectRetryLimit = typeof options.connectRetryLimit === 'undefined' ? defaults.connectRetryLimit : options.connectRetryLimit;
		this._consumerPrefetch = typeof options.consumerPrefetch === 'undefined' ? defaults.consumerPrefetch : options.consumerPrefetch;
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
		this._url = typeof options.url || defaults.url;

		// Configure the logger.
		this._logger = options.logger;
		if (!this._logger) {
			this._logger = log4js.getLogger();
			this._logger.level = process.env.DEBUG ? 'debug' : 'info';
		}

		// Configure reply queue topology.
		// Reply queue belongs only to this instance, but we want it to survive reconnects. Thus, we set an expiration for the queue.
		const replyQueueName = `postmaster.reply.${this._queuePrefix}.${uuidv4()}`;
		const replyQueueExpiration = (this._connectRetryDelay * this._connectRetryLimit) + (60000 * this._connectRetryLimit);
		this._topology.queues = { reply: { name: replyQueueName, noAck: true, expires: replyQueueExpiration } };
	}

	/**
	 * Accessor property for retrieving the number of messages being handled, subscribed and replies.
	 */
	get outstandingMessageCount() {
		const listenerCount = this._handlers.keys().reduce((sum, key) => {
			if (this._handlers[key].outstandingMessages) {
				return sum + this._handlers[key].outstandingMessages.size();
			}
			return sum;
		});

		return listenerCount + this._replyHandlers.keys().length;
	}

	/**
	 * Accessor property for getting the current handler timings.
	 */
	get handlerTimings() {
		const handlerTimings = {};
		for (const key of this._handlers.keys()) {
			const handler = this._handlers[key];
			if (handler.timings) {
				handlerTimings[key] = handler.timings;
			}
		}
		return handlerTimings;
	}

	/**
	 * Called to resolve the AMQP topic key corresponding to an address.
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
			connectionAttempts++;
			this._connecting = true;

			// We always want to start on a clean-slate when we connect.
			// Cancel outstanding messages, clear all consumers, and reset the connection.
			try {
				this._replyConsumerTag = null;
				for (const key of this._handlers.keys()) {
					delete this._handlers[key].consumerTag;
					if (this._handlers[key].outstandingMessages) {
						this._handlers[key].outstandingMessages.clear();
					}
				}
				await this._connection.close();
			} catch (err) {}

			const reconnect = async (err) => {
				try {
					if (!this._connecting) {
						this._logger.warn(`postmaster-general lost AMQP connection and will try to reconnect! err: ${err.message}`);
						await attemptConnect();
						await this._assertTopology();
						if (this._shouldConsume) {
							await this.startConsuming();
						}
						this._logger.warn('postmaster-general restored AMQP connection successfully!');
					}
				} catch (err) {
					this.emit('error', new Error(`postmaster-general was unable to re-establish AMQP connection! err: ${err.message}`));
				}
			};

			try {
				this._connection = await amqp.connect(this._url, { heartbeat: this._heartbeat });
				this._connection.on('error', reconnect);

				this._createChannel = async () => {
					const channel = await this._connection.createChannel();
					channel.on('error', reconnect);
					return channel;
				};

				this._channels = await Promise.props({
					publish: this._createChannel(),
					replyPublish: this._createChannel(),
					topology: this._createChannel(),
					consumers: Promise.reduce(this._topology.queues.keys(), async (consumerMap, key) => {
						const queue = this._topology.queues[key];
						consumerMap[queue.name] = await this._createChannel();
					})
				});

				connectionAttempts = 0;
				this._connecting = false;
			} catch (err) {
				if (connectionAttempts < this._connectRetryLimit) {
					this._logger.warn('postmaster-general failed to establish AMQP connection! Retrying...');
					await Promise.delay(this._connectRetryDelay);
					return attemptConnect();
				}
				this._logger.error(`postmaster-general failed to establish AMQP connection after ${connectionAttempts} attempts! err: ${err.message}`);
				throw err;
			}
		};

		this._logger.info('Starting postmaster-general connection...');
		await attemptConnect();
		this._logger.info('postmaster-general connection established!');
		await this._assertTopology();
	}

	/**
	 * Called to safely shutdown the AMQP connection while allowing outstanding messages to process.
	 */
	async shutdown() {
		const shutdownRetryDelay = 1000;
		const retryLimit = this._shutdownTimeout / shutdownRetryDelay;
		let retryAttempts = 0;

		const attempt = async () => {
			retryAttempts++;

			if ((this._connecting || this.outstandingMessageCount > 0) && retryAttempts < retryLimit) {
				await Promise.delay(shutdownRetryDelay);
				return attempt();
			}

			try {
				await this._connection.close();
			} catch (err) {}

			this._logger.info('postmaster-general shutdown successfully!');
		};

		this._logger.info('postmaster-general shutting down...');

		try {
			await this.stopConsuming();
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
		this._logger.debug(`postmaster-general asserting exchange name: ${name} type: ${type} options: ${JSON.stringify(options)}`);
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
		this._logger.debug(`postmaster-general asserting queue name: ${name} options: ${JSON.stringify(options)}`);
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
		this._logger.debug(`postmaster-general asserting binding queue: ${queue} exchange: ${exchange} topic: ${topic} options: ${JSON.stringify(options)}`);
		await this._channels.topology.bindQueue(queue, exchange, topic, options);
		this._topology.bindings[`${queue}_${exchange}`] = { queue, exchange, topic, options };
	}

	/**
	 * Called to assert any RabbitMQ topology after a successful connection is established.
	 * @returns {Promise} Promise resolving when all defined topology has been confirmed.
	 */
	async _assertTopology() {
		const topologyPromises = [];

		// Assert exchanges.
		for (const key of this._topology.exchanges.keys()) {
			const exchange = this._topology.exchanges[key];
			this._logger.debug(`postmaster-general asserting exchange name: ${exchange.name} type: ${exchange.type} options: ${JSON.stringify(exchange.options)}`);
			topologyPromises.push(this._channels.topology.assertExchange(exchange.name, exchange.type, exchange.options));
		}

		// Assert consumer queues.
		for (const key of this._topology.queues.keys()) {
			const queue = this._topology.queues[key];
			this._logger.debug(`postmaster-general asserting queue name: ${queue.name} options: ${JSON.stringify(queue.options)}`);
			topologyPromises.push(this._channels.topology.assertQueue(queue.name, queue.options));
		}

		// Await all assertions before asserting bindings.
		await Promise.all(topologyPromises);

		// Bind listeners.
		await Promise.map(this._topology.bindings.keys(), (key) => {
			const binding = this._topology.bindings[key];
			this._logger.debug(`postmaster-general asserting binding queue: ${binding.queue} exchange: ${binding.exchange} topic: ${binding.topic} options: ${JSON.stringify(binding.options)}`);
			return this._channels.topology.bindQueue(binding.queue, binding.exchange, binding.topic, binding.options);
		});
	}

	/**
	 * Called to start consuming incoming messages from all consumer channels.
	 * @returns {Promise} Promise that resolves when all consumers have begun consuming.
	 */
	async startConsuming() {
		this._shouldConsume = true;

		this._resetHandlerTimings();

		// Since the reply queue isn't bound to an exchange, we need to handle it separately.
		if (this._topology.queues.reply) {
			const replyQueue = this._topology.queues.reply;
			this._replyConsumerTag = await this._channels.consumers[replyQueue.name].consume(replyQueue.name, this._handleReply, replyQueue.options);
			this._logger.debug(`postmaster-general starting consuming from queue: ${replyQueue.name}`);
		}

		await Promise.map(this._topology.bindings.keys(), async (key) => {
			const binding = this._topology.bindings[key];
			const consumerTag = await this._channels.consumers[binding.queue].consume(binding.queue, this._handlers[binding.topic].callback, binding.options);
			this._handlers[binding.topic].consumerTag = consumerTag;
			this._logger.debug(`postmaster-general starting consuming from queue: ${binding.queue}`);
		});

		this._logger.info('postmaster-general started consuming on all queues!');
	}

	/**
	 * Called to stop consuming incoming messages from all channels.
	 * @param {Boolean} [cancelReplies] If truthy, this function will stop consuming from the reply channel as well as the bound listeners. Defaults to false.
	 * @returns {Promise} Promise that resolves when all consumers have stopped consuming.
	 */
	async stopConsuming(cancelReplies) {
		this._shouldConsume = false;

		this._resetHandlerTimings();

		if (this._replyConsumerTag && cancelReplies) {
			await this._channels.consumers[this._topology.queues.reply.name].cancel(this._replyConsumerTag);
			this._replyConsumerTag = null;
			this._logger.debug(`postmaster-general stopped consuming from queue ${this._topology.queues.reply.name}`);
		}

		await Promise.map(this._topology.bindings.keys(), async (key) => {
			const binding = this._topology.bindings[key];
			const consumerTag = JSON.parse(JSON.stringify(this._handlers[binding.topic].consumerTag));
			if (consumerTag) {
				delete this._handlers[binding.topic].consumerTag;
				await this._channels.consumers[binding.queue].cancel(consumerTag);
				this._logger.debug(`postmaster-general stopped consuming from queue ${binding.queue}`);
			}
		});

		this._logger.info('postmaster-general stopped consuming from all queues!');
	}

	/**
	 * A "safe", promise-based method for acknowledging messages that is guaranteed to resolve.
	 * @param {String} queueName The queue name of the channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to acknowledge.
	 * @param {String} pattern The routing key of the message.
	 * @param {Object} [reply] The request body of the response message to send.
	 * @returns {Promise} Promise that resolves when the message is acknowledged.
	 */
	async _ackMessageAndReply(queueName, msg, pattern, reply) {
		try {
			const topic = this._resolveTopic(pattern);
			if (this._handlers[topic] && this._handlers[topic].outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
				await this._channels.consumers[queueName].ack(msg);
				if (msg.properties.replyTo && msg.properties.correlationId) {
					reply = reply || {};
					const options = {
						contentType: 'application/json',
						contentEncoding: 'utf8',
						messageId: uuidv4(),
						correlationId: msg.properties.correlationId,
						timestamp: new Date().getTime()
					};
					await this._channels.replyPublish.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(reply)), options);
				}
				this._handlers[topic].outstandingMessages.delete(`${pattern}_${msg.properties.messageId}`);
			} else {
				this._logger.warn(`postmaster-general skipping message ack due to connection failure! message: ${pattern} messageId: ${msg.properties.messageId}`);
			}
		} catch (err) {
			this._logger.warn(`postmaster-general failed to ack a message! message: ${pattern} messageId: ${msg.properties.messageId} err: ${err.message}`);
		}
	}

	/**
	 * A "safe", promise-based method for nacking messages that is guaranteed to resolve.
	 * Nacked messages will not be requeued.
	 * @param {String} queueName The queue name of the channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to nack.
	 * @param {String} pattern The routing key of the message.
	 * @param {String} [reply] The error message to end in reply.
	 * @returns {Promise} Promise that resolves when the message is nacked.
	 */
	async _nackMessageAndReply(queueName, msg, pattern, reply) {
		try {
			const topic = this._resolveTopic(pattern);
			if (this._channels.consumers[queueName] && this._handlers[topic] && this._handlers[topic].outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
				await this._channels.consumers[queueName].nack(msg, false, false);
				if (msg.properties.replyTo && msg.properties.correlationId) {
					reply = reply || 'An unknown error occurred during processing!';
					const options = {
						contentType: 'application/json',
						contentEncoding: 'utf8',
						messageId: uuidv4(),
						correlationId: msg.properties.correlationId,
						timestamp: new Date().getTime()
					};
					await this._channels.replyPublish.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify({ err: reply })), options);
				}
				this._handlers[topic].outstandingMessages.delete(`${pattern}_${msg.properties.messageId}`);
			} else {
				this._logger.warn(`postmaster-general skipping message nack due to connection failure! message: ${pattern} messageId: ${msg.properties.messageId}`);
			}
		} catch (err) {
			this._logger.warn(`postmaster-general failed to nack a message! message: ${pattern} messageId: ${msg.properties.messageId} err: ${err.message}`);
		}
	}

	/**
	 * A "safe", promise-based method for rejecting messages that is guaranteed to resolve.
	 * Rejected messages will be requeued for retry.
	 * @param {String} queueName The queueName of the channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to reject.
	 * @param {String} pattern The routing key of the message.
	 * @returns {Promise} Promise that resolves when the message is rejected.
	 */
	async _rejectMessage(queueName, msg, pattern) {
		try {
			const topic = this._resolveTopic(pattern);
			if (this._channels.consumers[queueName] && this._handlers[topic] && this._handlers[topic].outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
				await this._channels.consumers[queueName].reject(msg);
				this._handlers[topic].outstandingMessages.delete(`${pattern}_${msg.properties.messageId}`);
			} else {
				this._logger.warn(`postmaster-general skipping message rejection due to connection failure! message: ${pattern} messageId: ${msg.properties.messageId}`);
			}
		} catch (err) {
			this._logger.warn(`postmaster-general failed to reject a message! message: ${pattern} messageId: ${msg.properties.messageId} err: ${err.message}`);
		}
	}

	/**
	 * Called to handle a reply to an RPC-style message.
	 * @param {Object} msg The RabbitMQ message.
	 */
	_handleReply(msg) {
		let body;
		try {
			body = (msg.content || '{}').toString();
			body = JSON.parse(body);
		} catch (err) {
			body.err = 'postmaster-general failed to parse message body due to invalid JSON!';
		}

		msg.properties = msg.properties || {};
		msg.properties.headers = msg.properties.headers || {};

		if (!msg.properties.correlationId || !this._replyHandlers[msg.properties.correlationId] || msg.properties.replyTo !== this._topology.queues.reply.name) {
			this._logger.warn(`postmaster-general reply handler received an invalid reply! correlationId: ${msg.properties.correlationId}`);
		} else {
			if (body.err) {
				this._replyHandlers[msg.properties.correlationId](new Error(body.err));
			} else {
				this._replyHandlers[msg.properties.correlationId](null, body);
			}
			delete this._replyHandlers[msg.properties.correlationId];
		}
	}

	/**
	 * Updates the timing data for message handler callbacks.
	 * @param {String} pattern The pattern to record timing data for.
	 * @param {Date} start The time at which the handler started execution.
	 */
	_setHandlerTiming(pattern, start) {
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
		for (const key of this._handlers.keys()) {
			delete this._handlers[key].timings;
		}

		// If we're not manually managing the timing refresh, schedule the next timeout.
		if (this._handlerTimingResetInterval) {
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
	async addListener(pattern, callback, options) {
		options = options || {};

		// Configure queue options.
		options.queue = options.queue || {};
		options.queue.deadLetterExchange = options.deadLetterExchange || this._deadLetterExchange;
		options.queue.deadLetterRoutingKey = pattern;

		const topic = this._resolveTopic(pattern);
		const queueName = (options.queue.prefix || this._queuePrefix) + '.' + topic;

		// Configure exchange options
		options.exchange = options.exchange || this._defaultExchange;

		// Grab a channel and assert the topology.
		if (!this._channels.consumers[queueName]) {
			this._channels.consumers[queueName] = await this._createChannel();
		}
		await this.assertExchange(options.exchange.name, options.exchange.type, options.exchange);
		await this.assertQueue(queueName, options.queue);
		await this.assertBinding(queueName, options.exchange.name, topic, options.binding);

		// Define the callback handler.
		this._handlers[topic] = { outstandingMessages: new Set() };
		this._handlers[topic].callback = async (msg) => {
			const start = new Date().getTime();

			try {
				msg.properties = msg.properties || {};
				msg.properties.headers = msg.properties.headers || {};
				msg.properties.messageId = msg.properties.messageId || msg.properties.correlationId;

				this._handlers[topic].outstandingMessages.add(`${pattern}_${msg.properties.messageId}`);

				let body = (msg.content || '{}').toString();
				body = JSON.parse(body);

				const reply = await callback(body);
				await this._ackMessageAndReply(queueName, msg, pattern, reply);
				this._setHandlerTiming(pattern, start);
			} catch (err) {
				this._logger.error(`postmaster-general message handler failed and cannot retry! message: ${pattern} err: ${err.message}`);
				await this._nackMessageAndReply(queueName, msg, pattern, err.message);
				this._setHandlerTiming(pattern, start);
			}
		};
	}

	/**
	 * Called to remove a listener. Note that this call DOES NOT delete any queues
	 * or exchanges. It is recommended that these constructs be made to auto-delete or expire
	 * if they are not persistent.
	 * @param {String} pattern The pattern to match.
	 * @param {String} exchange The name of the exchange to remove the binding.
	 * @param {String} [prefix] The queue prefix to match.
	 * @returns {Promise} Promise that resolves when the listener has been removed.
	 */
	async removeListener(pattern, exchange, prefix) {
		let attempts = 0;

		const topic = this._resolveTopic(pattern);
		const queueName = (prefix || this._queuePrefix) + '.' + topic;

		const attempt = async () => {
			attempts++;

			if (this._connecting) {
				await Promise.delay(this._removeListenerRetryDelay);
				return attempt();
			}

			try {
				if (this._channels.consumers[queueName]) {
					if (this._handlers[topic].consumerTag) {
						await this._channels.consumers[queueName].cancel(this._handlers[topic].consumerTag);
					}
					this._handlers[topic].outstandingMessages.clear();
					await this._channels.consumers[queueName].close();
					delete this._channels.consumers[queueName];
				}
				delete this._handlers[topic];
				delete this._topology.bindings[`${queueName}_${exchange}`];
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
		let publishAttempts = 0;

		// Set default publishing options.
		options = options || {};
		options.contentType = 'application/json';
		options.contentEncoding = 'utf8';
		options.messageId = options.messageId || uuidv4();
		options.timestamp = new Date().getTime();

		const exchange = options.exchange || this._defaultExchange.name;
		const msgData = Buffer.from(JSON.stringify(message || '{}'));

		const attempt = async (skipIncrement) => {
			if (!skipIncrement) {
				publishAttempts++;
			}

			if (this._connecting) {
				await Promise.delay(this._publishRetryDelay);
				return attempt(true);
			}

			try {
				const published = await this._channels.publish.publish(exchange, this._resolveTopic(routingKey), msgData, options);
				if (published) {
					publishAttempts = 0;
				} else {
					throw new Error(`postmaster-general failed publishing message due to full publish buffer and may retry! message: ${routingKey}`);
				}
			} catch (err) {
				if (publishAttempts < this._publishRetryLimit) {
					await Promise.delay(this._publishRetryDelay);
					return attempt();
				}
				throw err;
			}
		};

		try {
			await attempt();
		} catch (err) {
			this._logger.error(`postmaster-general failed to publish a fire-and-forget message! message: ${routingKey} err: ${err.message}`);
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
			if (!skipIncrement) {
				publishAttempts++;
			}

			if (this._connecting) {
				await Promise.delay(this._publishRetryDelay);
				return attempt();
			}

			try {
				const published = await this._channels.publish.publish(exchange, this._resolveTopic(routingKey), msgData, options);
				if (published) {
					publishAttempts = 0;
				} else {
					throw new Error(`postmaster-general failed publishing message due to full publish buffer and may retry! message: ${routingKey}`);
				}

				return new Promise((resolve, reject) => {
					this._replyHandlers[options.correlationId] = (err, data) => {
						if (err) {
							reject(err);
						} else {
							reject(data);
						}
					};
				}).timeout(this._replyTimeout);
			} catch (err) {
				if (publishAttempts < this._publishRetryLimit) {
					await Promise.delay(this._publishRetryDelay);
					return attempt();
				}
				throw err;
			}
		};

		return attempt();
	}
}

module.exports = PostmasterGeneral;
