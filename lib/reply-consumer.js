'use strict';

/**
 * Defines the "ReplyConsumer" class, representing RabbitMQ reply consumers.
 * @module lib/reply-consumer
 */

const EventEmitter = require('events');
const uuidv4 = require('uuid/v4');

/**
 * Class representing a RabbitMQ reply consumer.
 */
class ReplyConsumer extends EventEmitter {
	/**
	 * Constructs a ReplyConsumer.
	 * @constructor
	 * @param {object} connection - The amqp-connection-manager connection object.
	 * @param {object} options - Queue options.
	 */
	constructor(connection, options) {
		super();

		this.connection = connection;

		// Check required options.
		if (!options) {
			throw new Error('Parameter "options" must be defined.');
		}
		if (!options.queue) {
			throw new Error('Parameter "options" is missing required value "queue".');
		}

		// Configure queue options.
		options.queue.name = `postmaster.reply.${options.queue.prefix}.${uuidv4()}`;

		this.queue = options.queue;
		this.consumeSetup = null;
		this.consumerTag = null;

		// Create the channel.
		this.channel = this.connection.createChannel({
			json: true,
			setup: async (channel) => {
				await Promise.all([
					channel.assertQueue(this.queue.name, this.queue)
				]);
			}
		});

		/**
		 * Fired when the consumer has finished connecting to RabbitMQ.
		 * @event module:lib/reply-consumer~ReplyConsumer#connect
		 */
		this.channel.on('connect', () => this.emit('connect'));

		/**
		 * Fired when the consumer is unable to establish a connection to RabbitMQ.
		 * @event module:lib/reply-consumer~ReplyConsumer#error
		 * @type {object}
		 * @property {error} err - The error.
		 * @property {string} name - The name of the channel.
		 */
		this.channel.on('error', (err, data) => this.emit('error', err, { name: data.name }));

		/**
		 * Fired when the consumer has closed it's connection to RabbitMQ.
		 * @event module:lib/reply-consumer~ReplyConsumer#close
		 */
		this.channel.on('close', () => this.emit('close'));
	}

	/**
	 * Called to handle a consumed message.
	 * @param {object} msg - The RabbitMQ message data.
	 * @returns {promise}
	 * @fires module:lib/reply-consumer~ReplyConsumer#handleReply
	 * @fires module:lib/reply-consumer~ReplyConsumer#unhandledReply
	 */
	async handleMessage(msg) {
		msg = msg || {};
		msg.properties = msg.properties || {};
		msg.properties.headers = msg.properties.headers || {};

		let body;
		try {
			body = JSON.parse((msg.content || '{}').toString());
		} catch (err) {
			body = { err: 'Failed to parse message body due to invalid JSON!' };
		}

		if (msg.properties.correlationId && msg.properties.replyTo === this.queue.name) {
			if (body.err) {
				/**
				 * Fired when a reply is received and ready to be processed.
				 * @event module:lib/reply-consumer~ReplyConsumer#handleReply
				 * @type {object}
				 * @property {object} correlationId - The correlationId of the response.
				 * @property {object} body - The body of the response.
				 * @property {error} [err] - An optional error returned by the response.
				 */
				this.emit('handleReply', { correlationId: msg.properties.correlationId, body, err: new Error(body.err) });
			} else {
				this.emit('handleReply', { correlationId: msg.properties.correlationId, body });
			}
		} else {
			/**
			 * Fired when the consumer receieves a reply that it didn't expect.
			 * @event module:lib/reply-consumer~ReplyConsumer#unhandledReply
			 * @type {object}
			 * @property {object} correlationId - The correlationId of the response.
			 * @property {object} body - The body of the response.
			 */
			this.emit('unhandledReply', { correlationId: msg.properties.correlationId, body });
		}
	}

	/**
	 * Called to start consuming from the queue.
	 * @returns {promise}
	 */
	startConsuming() {
		this.consumeSetup = async (channel) => {
			const result = await channel.consume(this.queue.name, this.handleMessage.bind(this), { noAck: true });
			this.consumerTag = result.consumerTag;
		};

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

module.exports = ReplyConsumer;
