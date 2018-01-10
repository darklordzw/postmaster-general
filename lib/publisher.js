'use strict';

/**
 * Defines the "Publisher" class, representing RabbitMQ publish-reply functionality.
 * @module lib/publisher
 */

const EventEmitter = require('events');
const Promise = require('bluebird');
const uuidv4 = require('uuid/v4');

/**
 * Class representing a RabbitMQ reply consumer.
 */
class Publisher extends EventEmitter {
	/**
	 * Constructs a Publisher.
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
		if (!options.defaultExchange) {
			throw new Error('Parameter "options" is missing required property "defaultExchange".')
		}
		if (!options.replyTimeout) {
			throw new Error('Parameter "options" is missing required property "replyTimeout".')
		}

		// Configure queue options.
		options.queue.name = `postmaster.reply.${options.queue.prefix}.${uuidv4()}`;

		this.queue = options.queue;
		this.defaultExchange = options.defaultExchange;
		this.replyTimeout = options.replyTimeout;
		this.responseHandlers = {};

		this.consumeSetup = null;
		this.consumerTag = null;

		// Create the channels.
		this.responseChannel = this.connection.createChannel({
			setup: async (channel) => {
				await Promise.all([
					channel.assertQueue(this.queue.name, this.queue)
				]);
			}
		});
		this.publishChannel = this.connection.createChannel({ json: true });

		/**
		 * Fired when the consumer is unable to establish a connection to RabbitMQ.
		 * @event module:lib/publisher~Publisher#error
		 * @type {object}
		 * @property {error} err - The error.
		 * @property {string} name - The name of the channel.
		 */
		this.responseChannel.on('error', (err, data) => this.emit('error', err, { name: data.name }));
		this.publishChannel.on('error', (err, data) => this.emit('error', err, { name: data.name }));
	}

	/**
	 * Accessor property for retrieving the number of messages being handled.
	 */
	get outstandingMessageCount() {
		return Object.keys(this.responseHandlers).length
	}

	/**
	 * Waits for the channel to complete setup.
	 * @returns {promise}
	 */
	async waitForConnect() {
		await Promise.all([
			this.responseChannel.waitForConnect(),
			this.publishChannel.waitForConnect()
		]);
	}

	/**
	 * Publishes a fire-and-forget message that doesn't wait for an explicit response.
	 * @param {string} topic - The routing key to attach to the message.
	 * @param {object} [message] - The message data to publish.
	 * @param {object} [options] - Optional publishing options.
	 * @returns {promise}
	 */
	async publish(topic, message, options) {
		// Set default publishing options.
		options = options || {};
		options.contentType = 'application/json';
		options.contentEncoding = 'utf8';
		options.messageId = options.messageId || uuidv4();
		options.timestamp = new Date().getTime();

		const exchange = options.exchange || this.defaultExchange;
		const body = message || '{}';

		await this.publishChannel.publish(exchange, topic, body, options);
	}

	/**
	 * Publishes an RPC-style message that waits for a response.
	 * @param {string} topic - The routing key to attach to the message.
	 * @param {object} [message] - The message data to publish.
	 * @param {object} [options] - Optional publishing options.
	 * @returns {promise}
	 */
	async request(topic, message, options) {
		// Set default publishing options.
		options = options || {};
		options.contentType = 'application/json';
		options.contentEncoding = 'utf8';
		options.messageId = options.messageId || uuidv4();
		options.correlationId = options.correlationId || options.messageId;
		options.replyTo = this.replyQueue;
		options.timestamp = new Date().getTime();

		const exchange = options.exchange || this.defaultExchange;
		const body = message || '{}';

		await this.publishChannel.publish(exchange, topic, body, options);
		return new Promise((resolve, reject) => {
			this.responseHandlers[options.correlationId] = (err, data) => {
				if (err) {
					reject(err);
				} else {
					resolve(data);
				}
			};
		}).timeout(this.replyTimeout);
	}

	/**
	 * Called to handle a consumed message.
	 * @param {object} msg - The RabbitMQ message data.
	 * @fires module:lib/publisher~Publisher#unhandledReply
	 */
	handleMessage(msg) {
		msg = msg || {};
		msg.properties = msg.properties || {};
		msg.properties.headers = msg.properties.headers || {};

		let body;
		try {
			body = JSON.parse((msg.content || '{}').toString());
		} catch (err) {
			body = { err: 'Failed to parse message body due to invalid JSON!' };
		}

		if (msg.properties.correlationId && msg.properties.replyTo === this.queue.name && this.responseHandlers[msg.properties.correlationId]) {
			if (body.err) {
				this.responseHandlers[msg.properties.correlationId](new Error(body.err));
			} else {
				this.responseHandlers[msg.properties.correlationId](null, body);
			}
			delete this.responseHandlers[msg.properties.correlationId];
		} else {
			/**
			 * Fired when the consumer receieves a reply that it didn't expect.
			 * @event module:lib/publisher~Publisher#unhandledReply
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
	async startConsuming() {
		this.consumeSetup = async (channel) => {
			const result = await channel.consume(this.queue.name, this.handleMessage.bind(this), { noAck: true });
			this.consumerTag = result.consumerTag;
		};

		await this.responseChannel.addSetup(this.consumeSetup);
	}

	/**
	 * Called to stop consuming from the queue.
	 * @returns {promise}
	 */
	async stopConsuming() {
		if (this.consumeSetup) {
			const consumeSetup = this.consumeSetup;
			this.consumeSetup = null;
			await this.responseChannel.removeSetup(consumeSetup, async (channel) => {
				const consumerTag = JSON.parse(JSON.stringify(this.consumerTag));
				this.consumerTag = null;
				await channel.cancel(consumerTag);
			});
		}
	}

	/**
	 * Closes the underlying channel.
	 * @returns {promise}
	 */
	async close() {
		await Promise.all([
			this.responseChannel.close(),
			this.publishChannel.close()
		]);
	}
}

module.exports = Publisher;
