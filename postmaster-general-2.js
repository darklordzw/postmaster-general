'use strict';

/**
 * A simple library for making microservice message bus
 * calls using RabbitMQ via amqplib.
 * https://www.npmjs.com/package/amqplib
 * @module postmaster-general
 */

const EventEmitter = require('events');
const amqp = require('amqplib');
// Const uuidv4 = require('uuid/v4');
const Promise = require('bluebird');

function delay(t) {
	return new Promise((resolve) => {
		setTimeout(resolve, t);
	});
}

class PostmasterGeneral extends EventEmitter {
	/**
	 * Constructor function for the PostmasterGeneral object.
	 * @param {object} [options]
	 */
	constructor(options) {
		super();
		this.options = options || {};
		this.connecting = false;
		this.shouldConsume = false;
		this.connection = null;
		this.channels = null;
		this.handlers = {};
		this.logger = null;

		this.defaultPublishExchange = this.options.exchanges[0];
	}

	/**
	 * Called to resolve the AMQP topic key corresponding to an address.
	 * @param {string} address
	 */
	resolveTopic(address) {
		return address.replace(/:/g, '.');
	}

	/**
	 * Called to connect to RabbitMQ and build all channels.
	 * @returns {Promise}
	 */
	async connect() {
		let connectionAttempts = 0;

		const attemptConnect = async () => {
			connectionAttempts++;
			this.connecting = true;

			try {
				await this.connection.close();
			} catch (err) {}

			try {
				this.connection = await amqp.connect(this.options.url);
				this.connection.on('error', async (err) => {
					if (!this.connecting) {
						this.logger.warn('postmaster-general lost AMQP connection and will try to reconnect! Error: ', err);
						await attemptConnect();
						await this.assertTopography();
						if (this.shouldConsume) {
							await this.startConsuming();
						}
						this.logger.warn('postmaster-general restored AMQP connection successfully!');
					}
				});

				this.channels = await Promise.props({
					publish: this.connection.createConfirmChannel(),
					replyPublish: this.connection.createConfirmChannel(),
					replyConsume: this.connection.createChannel(),
					consumers: Promise.reduce(this.options.queues, async (consumerMap, queue) => {
						const channel = await this.connection.createChannel();
						channel.on('error', async (err) => {
							if (!this.connecting) {
								this.logger.warn('postmaster-general encountered an AMQP channel error and will try to reconnect! Error: ', err);
								await attemptConnect();
								await this.assertTopography();
								if (this.shouldConsume) {
									await this.startConsuming();
								}
								this.logger.warn('postmaster-general restored AMQP connection successfully!');
							}
						});
						consumerMap[queue.name] = channel;
					})
				});

				connectionAttempts = 0;
				this.connecting = false;
			} catch (err) {
				if (connectionAttempts < this.options.connection.retryLimit) {
					this.logger.warn('postmaster-general failed to establish AMQP connection! Retrying...');
					return attemptConnect();
				}
				this.logger.error(`postmaster-general failed to establish AMQP connection after ${connectionAttempts} attempts!`, err);
				throw err;
			}
		};

		await attemptConnect();
		await this.assertTopography();
	}

	/**
	 * Called to assert any RabbitMQ topology after a successful connection is established.
	 * @returns {Promise}
	 */
	async assertTopography() {
		// Assert exchanges.
		await Promise.map(this.options.exchanges, (exchange) => {
			return this.channels.publish.assertExchange(exchange.name, exchange.type, exchange.options);
		});

		// Assert reply queue.
		const queuePromises = [];
		queuePromises.push(await this.channels.replyConsume.assertQueue(this.options.reply.name, this.options.reply.options));

		// Assert consumer queues.
		let queue;
		for (queue of this.options.queues) {
			queuePromises.push(this.channels.consumers[queue.name].assertQueue(queue.name, queue.options));
		}
		await Promise.all(queuePromises);

		// Bind listeners.
		return Promise.map(this.options.binding, (binding) => {
			return this.channels.consumers[binding.queue].bindQueue(binding.queue, binding.exchange, this.resolveTopic(binding.pattern), binding.options);
		});
	}

	async startConsuming() {
		this.shouldConsume = true;

		this.replyConsumerTag = await this.channels.replyConsume.consume(this.options.reply.name, this.handleReply, this.options.reply.consumerOptions);

		return Promise.map(this.options.binding, async (binding) => {
			const consumerTag = await this.channels.consumers[binding.queue].consume(binding.queue, this.handlers[binding.pattern].callback, binding.consumerOptions);
			this.handlers[binding.pattern].consumerTag = consumerTag;
		});
	}

	async stopConsuming() {
		this.shouldConsume = false;

		return Promise.all([
			this.channels.replyConsume.cancel(this.replyConsumerTag),
			Promise.map(this.options.binding, (binding) => {
				return this.channels.consumers[binding.queue].cancel(this.handlers[binding.pattern].consumerTag);
			})
		]);
	}

	async handleReply(msg) {
		let body;
		try {
			body = (msg.content || '{}').toString();
			body = JSON.parse(body);
		} catch (err) {
			this.logger.error(new Error(`postmaster-general failed to parse message body due to invalid JSON!`), body);
			return;
		}

		msg.properties = msg.properties || {};
		msg.properties.headers = msg.properties.headers || {};

		const requestId = msg.properties.headers.requestId;
		const trace = msg.properties.headers.trace;
		const messageId = msg.properties.messageId;

		if (!msg.properties.correlationId || !this.replyHandlers[msg.properties.correlationId] || msg.properties.replyTo !== this.options.reply.name) {
			throw new Error('TODO: throw legit error');
		}

		return this.replyHandlers[msg.properties.correlationId](body);
	}

	async addHandler(pattern, callback) {
		this.handlers[pattern] = async (msg) => {
			let body;
			try {
				body = (msg.content || '{}').toString();
				body = JSON.parse(body);
			} catch (err) {
				this.logger.error(new Error(`postmaster-general failed to parse message body due to invalid JSON!`), body);
				return;
			}

			msg.properties = msg.properties || {};
			msg.properties.headers = msg.properties.headers || {};

			const requestId = msg.properties.headers.requestId;
			const trace = msg.properties.headers.trace;
			const messageId = msg.properties.messageId;

			const reply = await callback(body);
			if (msg.properties.replyTo && msg.properties.correlationId) {
				await this.channels.replyPublish.sendToQueue(msg.properties.replyTo, Buffer.from(JSON.stringify(reply)));
			}

			// TODO: Figure out good way to reference queue from handler.
			// TODO: Be sure not to call ack, nack, or reject on messages when the channel has gone down while processing!!!!
			await this.channels.consumers[queue].ack(msg);
		};
	}

	async publish(address, message, options) {
		let publishAttempts = 0;
		const exchange = options.exchange || this.defaultPublishExchange;
		const msgData = Buffer.from(JSON.stringify(message || '{}'));

		const attemptPublish = async () => {
			publishAttempts++;

			if (this.connecting) {
				await delay(this.options.publishRetryDelay);
				return attemptPublish();
			}

			try {
				const published = await this.channels.publish.publish(exchange, this.resolveTopic(address), msgData, options);
				if (published) {
					publishAttempts = 0;
				} else {
					await delay(this.options.publishRetryDelay);
					return attemptPublish();
				}
			} catch (err) {
				if (publishAttempts < this.options.publishRetryLimit) {
					await delay(this.options.publishRetryDelay);
					return attemptPublish();
				}
				throw err;
			}
		};

		return attemptPublish();
	}
}

module.exports = PostmasterGeneral;
