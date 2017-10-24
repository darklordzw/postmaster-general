'use strict';

/**
 * A simple library for making microservice message bus
 * calls using RabbitMQ via amqplib.
 * https://www.npmjs.com/package/amqplib
 * @module lib/postmaster-general
 */

const EventEmitter = require('events');
const amqp = require('amqplib');
const Promise = require('bluebird');
const uuidv4 = require('uuid/v4');
const errors = require('./errors');

class PostmasterGeneral extends EventEmitter {
	/**
	 * Constructor function for the PostmasterGeneral object.
	 * @param {object} [options]
	 */
	constructor(options) {
		super();
		options = options || {};
		this._connecting = false;
		this._shouldConsume = false;
		this._connection = null;
		this._channels = null;
		this._handlers = {};
		this._replyHandlers = {};
		this._logger = null;
		this._connectRetryLimit = typeof options.connectRetryLimit === 'undefined' ? 3 : options.connectRetryLimit;
		this._publishRetryDelay = typeof options.publishRetryDelay === 'undefined' ? 1000 : options.publishRetryDelay;
		this._publishRetryLimit = typeof options.publishRetryLimit === 'undefined' ? 3 : options.publishRetryLimit;
		this._replyTimeout = typeof options.replyTimeout === 'undefined' ? 3000 : options.replyTimeout;
		this._ackRetryDelay = typeof options.ackRetryDelay === 'undefined' ? 1000 : options.ackRetryDelay;
		this._url = typeof options.url === 'undefined' ? 'localhost:5672' : options.url;
		this._defaultPublishExchange = options.exchanges[0];
		this._outstandingMessages = new Set();
	}

	get outstandingMessages() {
		return this._outstandingMessages.size;
	}

	static get errors() {
		return errors;
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

			try {
				await this._connection.close();
			} catch (err) {}

			try {
				this._connection = await amqp.connect(this._url);
				this._connection.on('error', async (err) => {
					if (!this._connecting) {
						this._logger.warn('postmaster-general lost AMQP connection and will try to reconnect! Error: ', err);
						this._outstandingMessages.clear();
						await attemptConnect();
						await this._assertTopography();
						if (this._shouldConsume) {
							await this.startConsuming();
						}
						this._logger.warn('postmaster-general restored AMQP connection successfully!');
					}
				});

				this.createChannel = async () => {
					const channel = await this._connection.createChannel();
					channel.on('error', async (err) => {
						if (!this._connecting) {
							this._logger.warn('postmaster-general encountered an AMQP channel error and will try to reconnect! Error: ', err);
							this._outstandingMessages.clear();
							await attemptConnect();
							await this._assertTopography();
							if (this._shouldConsume) {
								await this.startConsuming();
							}
							this._logger.warn('postmaster-general restored AMQP connection successfully!');
						}
					});
					return channel;
				};

				this._channels = await Promise.props({
					publish: this._connection.createConfirmChannel(),
					replyPublish: this._connection.createConfirmChannel(),
					topography: this._connection.createChannel(),
					consumers: Promise.reduce(this.topography.queues, async (consumerMap, queue) => {
						consumerMap[queue.name] = await this.createChannel();
					})
				});

				connectionAttempts = 0;
				this._connecting = false;
			} catch (err) {
				if (connectionAttempts < this._connectRetryLimit) {
					this._logger.warn('postmaster-general failed to establish AMQP connection! Retrying...');
					return attemptConnect();
				}
				this._logger.error(`postmaster-general failed to establish AMQP connection after ${connectionAttempts} attempts!`, err);
				throw err;
			}
		};

		await attemptConnect();
		await this._assertTopography();
	}

	/**
	 * Asserts an exchange on RabbitMQ, adding it to the list of known topology.
	 * @param {String} name The name of the exchange.
	 * @param {String} type The type of exchange.
	 * @param {Object} [options] Various exchange options.
	 * @returns {Promise} Promise that resolves when the exchange has been asserted.
	 */
	async assertExchange(name, type, options) {
		await this._channels.topography.assertExchange(name, type, options);
		this.topography.exchanges[name] = { name, type, options };
	}

	/**
	 * Asserts a queue on RabbitMQ, adding it to the list of known topology.
	 * @param {String} name The name of the queue.
	 * @param {Object} [options] Various queue options.
	 * @returns {Promise} Promise that resolves when the queue has been asserted.
	 */
	async assertQueue(name, options) {
		await this._channels.topography.assertQueue(name, options);
		this.topography.queues[name] = { name, options };
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
		await this._channels.topography.bindQueue(queue, exchange, topic, options);
		this.topography.bindings[`${queue}_${exchange}`] = { queue, exchange, topic, options };
	}

	/**
	 * Called to assert any RabbitMQ topology after a successful connection is established.
	 * @returns {Promise} Promise resolving when all defined topography has been confirmed.
	 */
	async _assertTopography() {
		const topographyPromises = [];

		// Assert exchanges.
		for (const key of this.topography.exchanges.keys()) {
			const exchange = this.topography.exchanges[key];
			topographyPromises.push(this._channels.topography.assertExchange(exchange.name, exchange.type, exchange.options));
		}

		// Assert consumer queues.
		for (const key of this.topography.queues.keys()) {
			const queue = this.topography.queues[key];
			topographyPromises.push(this._channels.topography.assertQueue(queue.name, queue.options));
		}

		// Await all assertions before asserting bindings.
		await Promise.all(topographyPromises);

		// Bind listeners.
		await Promise.map(this.topography.bindings.keys(), (key) => {
			const binding = this.topography.bindings[key];
			return this._channels.topography.bindQueue(binding.queue, binding.exchange, binding.topic, binding.options);
		});
	}

	/**
	 * Called to start consuming incoming messages from all consumer channels.
	 * @returns {Promise} Promise that resolves when all consumers have begun consuming.
	 */
	async startConsuming() {
		this._shouldConsume = true;

		// Since the reply queue isn't bound to an exchange, we need to handle it separately.
		if (this.topography.queues.reply) {
			const replyQueue = this.topography.queues.reply;
			this._replyConsumerTag = await this._channels.consumers[replyQueue.name].consume(replyQueue.name, this._handleReply, replyQueue.options);
		}

		await Promise.map(this.topography.bindings.keys(), async (key) => {
			const binding = this.topography.bindings[key];
			const consumerTag = await this._channels.consumers[binding.queue].consume(binding.queue, this._handlers[binding.topic].callback, binding.options);
			this._handlers[binding.topic].consumerTag = consumerTag;
		});
	}

	/**
	 * Called to stop consuming incoming messages from all channels.
	 * @param {Boolean} [cancelReplies] If truthy, this function will stop consuming from the reply channel as well as the bound listeners. Defaults to false.
	 * @returns {Promise} Promise that resolves when all consumers have stopped consuming.
	 */
	async stopConsuming(cancelReplies) {
		this._shouldConsume = false;

		if (this._replyConsumerTag && cancelReplies) {
			await this._channels.consumers[this.topography.queues.reply.name].cancel(this._replyConsumerTag);
			this._replyConsumerTag = null;
		}

		await Promise.map(this.topography.bindings.keys(), (key) => {
			const binding = this.topography.bindings[key];
			const consumerTag = this._handlers[binding.topic].consumerTag;
			delete this._handlers[binding.topic].consumerTag;
			return this._channels.consumers[binding.queue].cancel(consumerTag);
		});
	}

	/**
	 * A "safe", promise-based method for acknowledging messages that is guaranteed to resolve.
	 * @param {Object} channel The channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to acknowledge.
	 * @param {String} pattern The routing key of the message.
	 * @param {Object} [reply] The request body of the response message to send.
	 * @returns {Promise} Promise that resolves when the message is acknowledged.
	 */
	_ackMessageAndReply(channel, msg, pattern, reply) {
		const attempt = async () => {
			if (this._connecting) {
				await Promise.delay(this._ackRetryDelay);
				return attempt();
			}

			try {
				if (this._outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
					await channel.ack(msg);
					if (reply && msg.properties.replyTo && msg.properties.correlationId) {
						await this._channels.replyPublish.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(reply)));
					}
				}
			} catch (err) {
				this._logger.warn(new errors.MessageACKError(`postmaster-general failed to ack a message! message: ${pattern} err: ${err.message}`));
			}
		};

		return attempt();
	}

	/**
	 * A "safe", promise-based method for nacking messages that is guaranteed to resolve.
	 * Nacked messages will not be requeued.
	 * @param {Object} channel The channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to nack.
	 * @param {String} pattern The routing key of the message.
	 * @param {Object} [reply] The request body of the response message to send.
	 * @returns {Promise} Promise that resolves when the message is nacked.
	 */
	_nackMessageAndReply(channel, msg, pattern, reply) {
		const attempt = async () => {
			if (this._connecting) {
				await Promise.delay(this._ackRetryDelay);
				return attempt();
			}

			try {
				if (this._outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
					await channel.nack(msg, false, false);
					if (reply && msg.properties.replyTo && msg.properties.correlationId) {
						await this._channels.replyPublish.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify({ err: reply })));
					}
				}
			} catch (err) {
				this._logger.warn(new errors.MessageNACKError(`postmaster-general failed to nack a message! message: ${pattern} err: ${err.message}`));
			}
		};

		return attempt();
	}

	/**
	 * A "safe", promise-based method for rejecting messages that is guaranteed to resolve.
	 * Rejected messages will be requeued for retry.
	 * @param {Object} channel The channel the message was received on.
	 * @param {Object} msg The RabbitMQ message to reject.
	 * @param {String} pattern The routing key of the message.
	 * @returns {Promise} Promise that resolves when the message is rejected.
	 */
	_rejectMessage(channel, msg, pattern) {
		const attempt = async () => {
			if (this._connecting) {
				await Promise.delay(this._ackRetryDelay);
				return attempt();
			}

			try {
				if (this._outstandingMessages.has(`${pattern}_${msg.properties.messageId}`)) {
					await channel.reject(msg);
				}
			} catch (err) {
				this._logger.warn(new errors.MessageNACKError(`postmaster-general failed to reject a message! message: ${pattern} err: ${err.message}`));
			}
		};

		return attempt();
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
			this._logger.error(new Error(`postmaster-general failed to parse message body due to invalid JSON!`), body);
			return;
		}

		msg.properties = msg.properties || {};
		msg.properties.headers = msg.properties.headers || {};

		if (!msg.properties.correlationId || !this._replyHandlers[msg.properties.correlationId] || msg.properties.replyTo !== this.topography.queues.reply.name) {
			this._logger.warn(`postmaster-general reply handler received an invalid reply! correlationId: ${msg.properties.correlationId}`);
		} else if (body.err) {
			this._replyHandlers[msg.properties.correlationId](new Error(body.err));
		} else {
			this._replyHandlers[msg.properties.correlationId](null, body);
		}
	}

	async addListener(pattern, callback, options) {
		const topic = this._resolveTopic(pattern);
		const queueName = options.queue.prefix + '.' + topic;
		if (!this._channels.consumers[queueName]) {
			this._channels.consumers[queueName] = await this.createChannel();
		}
		await this.assertExchange(options.exchange.name, options.exchange.type, options.exchange);
		await this.assertQueue(queueName, options.queue);
		await this.assertBinding(queueName, options.exchange.name, topic, options.binding);

		this._handlers[topic] = {};
		this._handlers[topic].callback = async (msg) => {
			try {
				msg.properties = msg.properties || {};
				msg.properties.headers = msg.properties.headers || {};
				msg.properties.messageId = msg.properties.messageId || msg.properties.correlationId;

				this._outstandingMessages.add(`${pattern}_${msg.properties.messageId}`);

				let body = (msg.content || '{}').toString();
				body = JSON.parse(body);

				const requestId = msg.properties.headers.requestId;
				const trace = msg.properties.headers.trace;
				const reply = await callback(body, { requestId, trace });
				await this._ackMessageAndReply(this._channels.consumers[queueName], msg, pattern, reply);
			} catch (err) {
				if (err instanceof errors.RetryableMessageHandlerError) {
					let retryCount = msg.properties.retryCount || 0;
					const retryLimit = msg.properties.retryLimit || 0;
					if (retryCount < retryLimit) {
						this._logger.warn(`postmaster-general message handler failed! Will retry message: ${pattern}`);
						msg.properties.retryCount = retryCount++;
						await this._rejectMessage(this._channels.consumers[queueName], msg);
					}
				} else {
					this._logger.error(`postmaster-general message handler failed and cannot retry! message: ${pattern} err: `, err);
					await this._nackMessageAndReply(this._channels.consumers[queueName], msg, pattern, err.message);
				}
			}
		};
	}

	/**
	 * Publishes a fire-and-forget message that doesn't wait for an explicit response.
	 * @param {String} routingKey The routing key to attach to the message.
	 * @param {Object} message The message data to publish.
	 * @param {Object} [options] Optional publishing options.
	 * @returns {Promise} A promise that resolves when the message is successfully published.
	 */
	async publish(routingKey, message, options) {
		let publishAttempts = 0;

		// Set default publishing options.
		options = options || {};
		options.contentType = 'application/json';
		options.contentEncoding = 'utf8';
		options.messageId = options.messageId || uuidv4();
		options.timestamp = new Date().getTime();

		const exchange = options.exchange || this._defaultPublishExchange;
		const msgData = Buffer.from(JSON.stringify(message || '{}'));

		const attempt = async () => {
			publishAttempts++;

			if (this._connecting) {
				await Promise.delay(this._publishRetryDelay);
				return attempt();
			}

			try {
				const published = await this._channels.publish.publish(exchange, this._resolveTopic(routingKey), msgData, options);
				if (published) {
					publishAttempts = 0;
				} else {
					await Promise.delay(this._publishRetryDelay);
					return attempt();
				}
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

	/**
	 * Publishes an RPC-style message that waits for a response.
	 * @param {String} routingKey The routing key to attach to the message.
	 * @param {Object} message The message data to publish.
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
		options.replyTo = this.topography.queues.reply.name;
		options.timestamp = new Date().getTime();

		const exchange = options.exchange || this._defaultPublishExchange;
		const msgData = Buffer.from(JSON.stringify(message || '{}'));

		const attempt = async () => {
			publishAttempts++;

			if (this._connecting) {
				await Promise.delay(this._publishRetryDelay);
				return attempt();
			}

			try {
				const published = await this._channels.publish.publish(exchange, this._resolveTopic(routingKey), msgData, options);
				if (published) {
					publishAttempts = 0;
				} else {
					await Promise.delay(this._publishRetryDelay);
					return attempt();
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
