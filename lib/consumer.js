'use strict';

const EventEmitter = require('events');

class Consumer extends EventEmitter {
	constructor(connection, topic, callback, replyChannel, options) {
		super();

		options = options || {};

		// Configure queue options.
		options.queue = options.queue || {};
		options.queue.deadLetterExchange = options.deadLetterExchange || this._deadLetterExchange;
		options.queue.deadLetterRoutingKey = topic;
		options.queue.name = (options.queue.prefix || this._queuePrefix) + '.' + topic;

		// Configure exchange options.
		options.exchange = options.exchange || this._defaultExchange;

		this.connection = connection;
		this.topic = topic;
		this.queue = options.queue;
		this.exchange = options.exchange;
		this.prefetch = options.prefetch;
		this.noAck = options.noAck;
		this.isReply = options.isReply;
		this.handlerTimingResetInterval = options.handlerTimingResetInterval;
		this.handlerTimingsTimeout = null;
		this.shuttingDown = false;
		this.outstandingMessages = new Set();
		this.timings = {
			messageCount: 0,
			elapsedTime: 0,
			minElapsedTime: 0,
			maxElapsedTime: 0
		};
		this.callback = callback;
		this.channel = this.connection.createChannel({
			json: true,
			setup: async (channel) => {
				await Promise.all([
					channel.assertQueue(options.queue.name, options.queue),
					channel.assertExchange(options.exchange.name, options.exchange.type, options.exchange)
				]);
				await channel.bindQueue(options.queue.name, options.exchange.name, this.topic);

				if (this.prefetch) {
					await channel.prefetch(this.prefetch);
				}
			}
		});
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

		if (this.handlerTimingResetInterval && !this.shuttingDown) {
			this.handlerTimingsTimeout = setTimeout(() => {
				this.resetHandlerTimings();
			}, this.handlerTimingResetInterval);
		}
	}

	/**
	 * Called to acknowledge a message.
	 * @param {object} msg - The RabbitMQ message to acknowledge.
	 * @fires Consumer#ackError
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
			 * ACK error event.
			 * @event Consumer#ackError
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
	 * @fires Consumer#nackError
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
			 * NACK error event.
			 * @event Consumer#nackError
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
	 * @fires Consumer#rejectError
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
			 * Reject error event.
			 * @event Consumer#rejectError
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
	 * @fires Consumer#sendReply
	 * @fires Consumer#handlerError
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
				 * Send reply event.
				 * @event Consumer#sendReply
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
			 * Mesesage handler error event.
			 * @event Consumer#handlerError
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
}

module.exports = Consumer;
