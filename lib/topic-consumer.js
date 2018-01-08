'use strict';

/**
 * Defines the "TopicConsumer" class, representing RabbitMQ consumers.
 * @module lib/topic-consumer
 */

const EventEmitter = require('events');

/**
 * Class representing a RabbitMQ consumer.
 */
class TopicConsumer extends EventEmitter {
	/**
	 * Constructs a TopicConsumer.
	 * @constructor
	 * @param {object} connection - The amqp-connection-manager connection object.
	 * @param {string} topic - The topic to consume.
	 * @param {function} callback - The callback to call when a message is processed.
	 * @param {object} options - Queue, exchange, and binding options.
	 */
	constructor(connection, topic, callback, options) {
		super();

		this.connection = connection;
		this.topic = topic;
		this.callback = callback;

		// Check required options.
		if (!options) {
			throw new Error('Parameter "options" must be defined.');
		}
		if (!options.queue) {
			throw new Error('Parameter "options" is missing required value "queue".');
		}
		if (!options.exchange) {
			throw new Error('Parameter "options" is missing required value "exchange".');
		}

		// Configure queue options.
		options.queue.deadLetterRoutingKey = this.topic;
		options.queue.name = `${options.queue.prefix}.${this.topic}`;

		this.queue = options.queue;
		this.exchange = options.exchange;
		this.prefetch = options.prefetch;
		this.noAck = options.noAck;
		this.noLocal = options.noLocal;
		this.exclusive = options.exclusive;
		this.priority = options.priority;
		this.handlerTimingResetInterval = options.handlerTimingResetInterval;

		this.consumeSetup = null;
		this.consumerTag = null;
		this.handlerTimingsTimeout = null;
		this.outstandingMessages = new Set();
		this.timings = {
			messageCount: 0,
			elapsedTime: 0,
			minElapsedTime: 0,
			maxElapsedTime: 0
		};

		// Create the channel.
		this.channel = this.connection.createChannel({
			json: true,
			setup: async (channel) => {
				await Promise.all([
					channel.assertQueue(this.queue.name, this.queue),
					channel.assertExchange(this.exchange.name, this.exchange.type, this.exchange),
					channel.bindQueue(this.queue.name, this.exchange.name, this.topic)
				]);

				if (typeof this.prefetch !== 'undefined') {
					await channel.prefetch(this.prefetch);
				}
			}
		});

		/**
		 * Fired when the consumer has finished connecting to RabbitMQ.
		 * @event module:lib/topic-consumer~TopicConsumer#connect
		 */
		this.channel.on('connect', () => this.emit('connect'));

		/**
		 * Fired when the consumer is unable to establish a connection to RabbitMQ.
		 * @event module:lib/topic-consumer~TopicConsumer#error
		 * @type {object}
		 * @property {error} err - The error.
		 * @property {string} name - The name of the channel.
		 */
		this.channel.on('error', (err, data) => this.emit('error', err, { name: data.name }));

		/**
		 * Fired when the consumer has closed it's connection to RabbitMQ.
		 * @event module:lib/topic-consumer~TopicConsumer#close
		 */
		this.channel.on('close', () => this.emit('close'));
	}

	/**
	 * Updates the timing data for message handler callbacks.
	 * @param {number} start - The time at which the handler started execution.
	 */
	setHandlerTiming(start) {
		const elapsed = new Date().getTime() - start;
		this.timings.messageCount++;
		this.timings.elapsedTime += elapsed;

		if (this.timings.minElapsedTime > elapsed ||
			this.timings.minElapsedTime === 0) {
			this.timings.minElapsedTime = elapsed;
		}
		if (this.timings.maxElapsedTime < elapsed) {
			this.timings.maxElapsedTime = elapsed;
		}
	}

	/**
	 * Resets the handler timings to prevent unbounded accumulation of stale data.
	 */
	resetHandlerTimings() {
		this.timings = {
			messageCount: 0,
			elapsedTime: 0,
			minElapsedTime: 0,
			maxElapsedTime: 0
		};

		if (this.handlerTimingResetInterval && this.consumeSetup) {
			this.handlerTimingsTimeout = setTimeout(() => {
				this.resetHandlerTimings();
			}, this.handlerTimingResetInterval);
		}
	}

	/**
	 * Called to acknowledge a message.
	 * @param {object} msg - The RabbitMQ message to acknowledge.
	 * @fires module:lib/topic-consumer~TopicConsumer#ackError
	 */
	ack(msg) {
		try {
			if (this.outstandingMessages.has(`${this.topic}_${msg.properties.messageId}`)) {
				if (!this.noAck) {
					this.channel.ack(msg);
				}
				this.outstandingMessages.delete(`${this.topic}_${msg.properties.messageId}`);
			}
		} catch (err) {
			/**
			 * Fired when there is an error acknowledging a message.
			 * @event module:lib/topic-consumer~TopicConsumer#ackError
			 * @type {object}
			 * @property {string} topic - The message's routing key.
			 * @property {string} messageId - The unique id of the message.
			 * @property {error} err - The error message.
			 */
			this.emit('ackError', { topic: this.topic, messageId: msg.properties.messageId, err });
		}
	}

	/**
	 * Called to nack a message without requeuing.
	 * @param {object} msg - The RabbitMQ message to acknowledge.
	 * @fires module:lib/topic-consumer~TopicConsumer#nackError
	 */
	nack(msg) {
		try {
			if (this.outstandingMessages.has(`${this.topic}_${msg.properties.messageId}`)) {
				if (!this.noAck) {
					this.channel.nack(msg, false, false);
				}
				this.outstandingMessages.delete(`${this.topic}_${msg.properties.messageId}`);
			}
		} catch (err) {
			/**
			 * Fired when there is an error NACK-ing a message.
			 * @event module:lib/topic-consumer~TopicConsumer#nackError
			 * @type {object}
			 * @property {string} topic - The message's routing key.
			 * @property {string} messageId - The unique id of the message.
			 * @property {error} err - The error.
			 */
			this.emit('nackError', { topic: this.topic, messageId: msg.properties.messageId, err });
		}
	}

	/**
	 * Called to reject a message, sending back to the queue for processing by another consumer.
	 * @param {object} msg - The RabbitMQ message to acknowledge.
	 * @fires module:lib/topic-consumer~TopicConsumer#rejectError
	 */
	reject(msg) {
		try {
			if (this.outstandingMessages.has(`${this.topic}_${msg.properties.messageId}`)) {
				if (!this.noAck) {
					this.channel.nack(msg, false, true);
				}
				this.outstandingMessages.delete(`${this.topic}_${msg.properties.messageId}`);
			}
		} catch (err) {
			/**
			 * Fired when there is an error rejecting a message.
			 * @event module:lib/topic-consumer~TopicConsumer#rejectError
			 * @type {object}
			 * @property {string} topic - The message's routing key.
			 * @property {string} messageId - The unique id of the message.
			 * @property {error} err - The error message.
			 */
			this.emit('rejectError', { topic: this.topic, messageId: msg.properties.messageId, err });
		}
	}

	/**
	 * Called to handle a consumed message.
	 * @param {object} msg - The RabbitMQ message data.
	 * @returns {promise}
	 * @fires module:lib/topic-consumer~TopicConsumer#sendReply
	 * @fires module:lib/topic-consumer~TopicConsumer#handlerError
	 */
	async handleMessage(msg) {
		const start = new Date().getTime();

		// If we have no message, the consumer was likely cancelled. Ignore.
		if (!msg) {
			return;
		}

		try {
			msg.properties = msg.properties || {};
			msg.properties.headers = msg.properties.headers || {};
			msg.properties.messageId = msg.properties.messageId || msg.properties.correlationId;

			// The "rabbot" library requires correlationId to match messageId.
			// Added here for compatibility with older postmaster-general.
			msg.properties.correlationId = msg.properties.messageId;

			// The "rabbot" library requires this as it supports streaming sequences.
			// We don't, but added here for compatibility with older postmaster-general.
			msg.properties.headers.sequence_end = true; // eslint-disable-line camelcase

			// Make sure to mark the message as outstanding.
			this.outstandingMessages.add(`${this.topic}_${msg.properties.messageId}`);

			const body = JSON.parse((msg.content || '{}').toString());
			const reply = await this.callback(body, msg.properties.headers);
			this.ack(msg);

			if (msg.properties.replyTo && msg.properties.correlationId) {
				/**
				 * Fired when a consumer has a reply to send for a message.
				 * @event module:lib/topic-consumer~TopicConsumer#sendReply
				 * @type {object}
				 * @property {string} topic - The message's routing key.
				 * @property {object} msg - The RabbitMQ message.
				 * @property {object} reply - The reply data.
				 */
				this.emit('sendReply', { topic: this.topic, msg, reply });
			}
		} catch (err) {
			this.nack(msg);

			if (msg.properties.replyTo && msg.properties.correlationId) {
				this.emit('sendReply', { topic: this.topic, msg, reply: err.message });
			}

			/**
			 * Fired when there is an error handling a message.
			 * @event module:lib/topic-consumer~TopicConsumer#handlerError
			 * @type {object}
			 * @property {string} topic - The message's routing key.
			 * @property {string} messageId - The unique id of the message.
			 * @property {error} err - The error.
			 */
			this.emit('handlerError', { topic: this.topic, messageId: msg.properties.messageId, err });
		}

		// Update the handler timings.
		this.setHandlerTiming(this.topic, start);
	}

	/**
	 * Called to start consuming from the queue.
	 * @returns {promise}
	 */
	startConsuming() {
		this.consumeSetup = async (channel) => {
			const options = {
				noAck: this.noAck,
				noLocal: this.noLocal,
				exclusive: this.exclusive,
				priority: this.priority
			};
			const result = await channel.consume(this.queue.name, this.handleMessage.bind(this), options);
			this.consumerTag = result.consumerTag;
		};

		this.resetHandlerTimings();

		return this.channel.addSetup(this.consumeSetup);
	}

	/**
	 * Called to stop consuming from the queue.
	 * @returns {promise}
	 */
	async stopConsuming() {
		if (this.consumeSetup) {
			const consumeSetup = this.consumeSetup;
			this.consumeSetup = null;
			return this.channel.removeSetup(consumeSetup, (channel) => {
				const consumerTag = JSON.parse(JSON.stringify(this.consumerTag));
				this.consumerTag = null;
				return channel.cancel(consumerTag);
			});
		}
	}

	/**
	 * Closes the underlying channel.
	 * @returns {promise}
	 */
	close() {
		return this.channel.close();
	}
}

module.exports = TopicConsumer;
