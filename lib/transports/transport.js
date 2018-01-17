'use strict';

/**
 * Base class of all transport modules.
 * @module lib/transports/transport
 */

const EventEmitter = require('events');
const _ = require('lodash');
const defaults = require('../../defaults');

/**
 * Base class of all transports.
 * @extends EventEmitter
 */
class Transport extends EventEmitter {
	/**
	 * Constructor for the Transport object.
	 * @param {object} [options] - Optional settings.
	 * @param {number} [options.timingsResetInterval] - How frequently should the transport clear its timing metrics, in milliseconds.
	 */
	constructor(options) {
		super();
		options = options || {};

		if (!_.isUndefined(options.timingsResetInterval) && !_.isNumber(options.timingsResetInterval)) {
			throw new TypeError('"options.timingsResetInterval" should be a number.');
		}

		/**
		 * A mapping of topics to timing metrics.
		 * @type {object}
		 */
		this.timings = {};

		/**
		 * Tracks whether or not the transport is currently listening.
		 * @type {boolean}
		 */
		this.listening = false;

		/**
		 * How frequently should the transport clear its timing metrics, in milliseconds.
		 * @type {number}
		 */
		this.timingsResetInterval = options.timingsResetInterval || defaults.timingsResetInterval;

		this.timingsTimeout = null;
	}

	/**
	 * Connects the transport from to any services it needs to function.
	 * @returns {Promise}
	 */
	connect() {
		return Promise.resolve();
	}

	/**
	 * Disconnects the transport from any services it references.
	 * @returns {Promise}
	 */
	disconnect() {
		return new Promise((resolve) => {
			this.listening = false;
			if (this.timingsTimeout) {
				clearTimeout(this.timingsTimeout);
			}
			resolve();
		});
	}

	/**
	 * Processes a routing key into a format appropriate for the transport type.
	 * @param {string} routingKey - The routing key to convert.
	 * @returns {string}
	 */
	resolveTopic(routingKey) {
		if (!_.isString(routingKey)) {
			throw new TypeError('"routingKey" should be a string.');
		}
		return routingKey;
	}

	/**
	 * Adds a new message handler.
	 * @param {string} routingKey - The routing key of the message to handle.
	 * @param {function} callback - The function to call when a new message is received.
	 * @param {object} [options] - Optional params for configuring the handler.
	 * @param {object} [options.correlationId] - Optional marker used for tracing requests through the system.
	 * @param {object} [options.initiator] - Optional marker used for identifying the user who generated the initial request.
	 * @returns {Promise}
	 */
	addListener(routingKey, callback, options) {
		return new Promise((resolve) => {
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!_.isFunction(callback)) {
				throw new TypeError('"callback" should be a function that returns a Promise.');
			}
			if (!_.isUndefined(options.correlationId) && !_.isString(options.correlationId)) {
				throw new TypeError('"options.correlationId" should be a string.');
			}
			if (!_.isUndefined(options.initiator) && !_.isString(options.initiator)) {
				throw new TypeError('"options.initiator" should be a string.');
			}

			options = options || {};
			const topic = this.resolveTopic(routingKey);

			resolve((msg) => {
				const start = new Date().getTime();
				return callback(msg, { correlationId: options.correlationId, initiator: options.initiator })
					.then((res) => {
						this.recordTiming(topic, start);
						return res;
					})
					.catch((err) => {
						this.recordTiming(topic, start);
						throw err;
					});
			});
		});
	}

	/**
	 * Logs timing data for message handlers.
	 * @param {string} topic - The processed routing key of the message to time.
	 * @param {number} start - The timestamp at which the message handler started processing.
	 * @private
	 */
	recordTiming(topic, start) {
		if (!_.isString(topic)) {
			throw new TypeError('"topic" should be a string.');
		}
		if (!_.isNumber(start)) {
			throw new TypeError('"start" should be a number.');
		}

		const elapsed = new Date().getTime() - start;
		this.timings[topic] = this.timings[topic] || {
			messageCount: 0,
			elapsedTime: 0,
			minElapsedTime: 0,
			maxElapsedTime: 0
		};
		this.timings[topic].messageCount++;
		this.timings[topic].elapsedTime += elapsed;

		if (this.timings[topic].minElapsedTime > elapsed ||
			this.timings[topic].minElapsedTime === 0) {
			this.timings[topic].minElapsedTime = elapsed;
		}
		if (this.timings[topic].maxElapsedTime < elapsed) {
			this.timings[topic].maxElapsedTime = elapsed;
		}
	}

	/**
	 * Called periodically to reset handler timings to keep the data sample fresh.
	 * @private
	 */
	resetTimings() {
		this.timings = {};
		if (this.listening) {
			this.timingsTimeout = setTimeout(() => this.resetTimings(), this.timingsResetInterval);
		}
	}

	/**
	 * Deletes a message handler.
	 * @param {string} routingKey - The routing key of the handler to remove.
	 * @returns {Promise}
	 */
	removeListener(routingKey) {
		return new Promise((resolve) => {
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}

			const topic = this.resolveTopic(routingKey);
			delete this.timings[topic];

			resolve();
		});
	}

	/**
	 * Starts listening to messages.
	 * @returns {Promise}
	 */
	listen() {
		return new Promise((resolve) => {
			this.listening = true;
			resolve();
		});
	}

	/**
	 * Publishes a fire-and-forget message that is not expected to return a meaningful response.
	 * @param {string} routingKey - The routing key to attach to the message.
	 * @param {object} [message] - The message data to publish.
	 * @param {object} [options] - Optional publishing options.
	 * @param {object} [options.correlationId] - Optional marker used for tracing requests through the system.
	 * @param {object} [options.initiator] - Optional marker used for identifying the user who generated the initial request.
	 * @returns {Promise}
	 * @throws {TypeError}
	 * @throws {RequestError}
	 */
	publish(routingKey, message, options) { // eslint-disable-line no-unused-vars
		return new Promise((resolve) => {
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!_.isUndefined(options.correlationId) && !_.isString(options.correlationId)) {
				throw new TypeError('"options.correlationId" should be a string.');
			}
			if (!_.isUndefined(options.initiator) && !_.isString(options.initiator)) {
				throw new TypeError('"options.initiator" should be a string.');
			}
			resolve();
		});
	}

	/**
	 * Publishes an RPC-style message that waits for a response.
	 * @param {string} routingKey - The routing key to attach to the message.
	 * @param {object} [message] - The message data to publish.
	 * @param {object} [options] - Optional publishing options.
	 * @param {object} [options.correlationId] - Optional marker used for tracing requests through the system.
	 * @param {object} [options.initiator] - Optional marker used for identifying the user who generated the initial request.
	 * @returns {Promise}
	 * @throws {TypeError}
	 * @throws {RequestError}
	 */
	request(routingKey, message, options) { // eslint-disable-line no-unused-vars
		return new Promise((resolve) => {
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!_.isUndefined(options.correlationId) && !_.isString(options.correlationId)) {
				throw new TypeError('"options.correlationId" should be a string.');
			}
			if (!_.isUndefined(options.initiator) && !_.isString(options.initiator)) {
				throw new TypeError('"options.initiator" should be a string.');
			}
			resolve();
		});
	}
}

module.exports = Transport;

/**
 * Fired when the transport is disconnected unexpectedly.
 * @event module:lib/transports/transport~Transport#disconnected
 * @type {object}
 */

 /**
 * Fired when the transport is able to reconnect.
 * @event module:lib/transports/transport~Transport#reconnected
 * @type {object}
 */

 /**
 * Fired when the transport is unable to re-establish a connection.
 * @event module:lib/transports/transport~Transport#connectionError
 * @type {object}
 * @property {Error} err - The error that caused the disconnect.
 */
