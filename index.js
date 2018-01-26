'use strict';

/**
 * A simple library for making microservice message calls over a variety of transports.
 * @module index
 */

const EventEmitter = require('events');
const _ = require('lodash');
const Transport = require('postmaster-general-core').Transport;

/**
 * The postmaster-general microservice messaging library.
 * @extends EventEmitter
 */
class PostmasterGeneral extends EventEmitter {
	/**
	 * Constructor function for the PostmasterGeneral object.
	 * @param {object} [options] - Optional settings.
	 * @param {Transport} [options.publishTransport] - The core Transport object used to send fire-and-forget messages.
	 * @param {Transport} [options.requestTransport] - The core Transport object used to send RPC messages.
	 * @param {Transport} [options.fafListenerTransport] - The core Transport object used to receive fire-and-forget messages.
	 * @param {Transport} [options.rpcListenerTransport] - The core Transport object used to listen to RPC messages.
	 */
	constructor(options) {
		super();

		options = options || {};

		if (!_.isUndefined(options.publishTransport) && !(options.publishTransport instanceof Transport)) {
			throw new TypeError('"options.publishTransport" should be a Transport.');
		}
		if (!_.isUndefined(options.requestTransport) && !(options.requestTransport instanceof Transport)) {
			throw new TypeError('"options.requestTransport" should be a Transport.');
		}
		if (!_.isUndefined(options.fafListenerTransport) && !(options.fafListenerTransport instanceof Transport)) {
			throw new TypeError('"options.fafListenerTransport" should be a Transport.');
		}
		if (!_.isUndefined(options.rpcListenerTransport) && !(options.rpcListenerTransport instanceof Transport)) {
			throw new TypeError('"options.rpcListenerTransport" should be a Transport.');
		}

		this.transports = {};

		if (options.publishTransport) {
			this.transports.publish = options.publishTransport;
		}
		if (options.requestTransport) {
			this.transports.request = options.requestTransport;
		}
		if (options.fafListenerTransport) {
			this.transports.fafListener = options.fafListenerTransport;
		}
		if (options.rpcListenerTransport) {
			this.transports.rpcListener = options.rpcListenerTransport;
		}
	}

	/**
	 * Accessor property that returns timing data for all transports.
	 */
	get handlerTimings() {
		const handlerTimings = {};

		for (const key of Object.keys(this.transports)) {
			if (this.transports[key]) {
				Object.assign(handlerTimings, this.transports[key].timings);
			}
		}

		return handlerTimings;
	}

	/**
	 * Connects all transports.
	 * @returns {Promise}
	 */
	connect() {
		const promises = [];

		for (const key of Object.keys(this.transports)) {
			if (this.transports[key]) {
				promises.push(this.transports[key].connect());
			}
		}

		if (promises.length > 0) {
			return Promise.all(promises);
		}
		return Promise.reject(new Error('No Transports were configured.'));
	}

	/**
	 * Disconnects all transports
	 * @returns {Promise}
	 */
	disconnect() {
		const promises = [];

		for (const key of Object.keys(this.transports)) {
			if (this.transports[key]) {
				promises.push(this.transports[key].disconnect());
			}
		}

		if (promises.length > 0) {
			return Promise.all(promises);
		}
		return Promise.resolve();
	}

	/**
	 * Adds a new RPC message handler.
	 * @param {string} routingKey - The routing key of the message to handle.
	 * @param {function} callback - The function to call when a new message is received.
	 * @param {object} [options] - Optional params for configuring the handler.
	 * @returns {Promise}
	 */
	addRequestListener(routingKey, callback, options) {
		options = options || {};

		return new Promise((resolve) => {
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!_.isFunction(callback)) {
				throw new TypeError('"callback" should be a function that returns a Promise.');
			}
			if (!this.transports.rpcListener) {
				throw new Error('Cannot add a new RPC listener, no RPC Transport has been configured.');
			}
			resolve();
		})
		.then(() => this.transports.rpcListener.addMessageListener(routingKey, callback, options));
	}

	/**
	 * Adds a new fire-and-forget message handler.
	 * @param {string} routingKey - The routing key of the message to handle.
	 * @param {function} callback - The function to call when a new message is received.
	 * @param {object} [options] - Optional params for configuring the handler.
	 * @returns {Promise}
	 */
	addFireAndForgetListener(routingKey, callback, options) {
		options = options || {};

		return new Promise((resolve) => {
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!_.isFunction(callback)) {
				throw new TypeError('"callback" should be a function that returns a Promise.');
			}
			if (!this.transports.fafListener) {
				throw new Error('Cannot add a new fire-and-forget listener, no fire-and-forget Transport has been configured.');
			}
			resolve();
		})
		.then(() => this.transports.fafListener.addMessageListener(routingKey, callback, options));
	}

	/**
	 * Deletes an RPC message handler.
	 * @param {string} routingKey - The routing key of the handler to remove.
	 * @returns {Promise}
	 */
	removeRequestListener(routingKey) {
		return new Promise((resolve) => {
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!this.transports.rpcListener) {
				throw new Error('Cannot remove an RPC listener, no RPC Transport has been configured.');
			}
			resolve();
		})
		.then(() => this.transports.rpcListener.removeMessageListener(routingKey));
	}

	/**
	 * Deletes a fire-and-forget message handler.
	 * @param {string} routingKey - The routing key of the handler to remove.
	 * @returns {Promise}
	 */
	removeFireAndForgetListener(routingKey) {
		return new Promise((resolve) => {
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!this.transports.fafListener) {
				throw new Error('Cannot remove a fire-and-forget listener, no fire-and-forget Transport has been configured.');
			}
			resolve();
		})
		.then(() => this.transports.fafListener.removeMessageListener(routingKey));
	}

	/**
	 * Starts listening to messages.
	 * @returns {Promise}
	 */
	listen() {
		const promises = [];

		for (const key of Object.keys(this.transports)) {
			if (this.transports[key] && (key === 'rpcListener' || key === 'fafListener')) {
				promises.push(this.transports[key].listen());
			}
		}

		if (promises.length > 0) {
			return Promise.all(promises);
		}
		return Promise.reject(new Error('No Transports were configured.'));
	}

	/**
	 * Publishes a fire-and-forget message that is not expected to return a meaningful response.
	 * @param {string} routingKey - The routing key to attach to the message.
	 * @param {object} [message] - The message data to publish.
	 * @param {object} [options] - Optional publishing options.
	 * @param {object} [options.correlationId] - Optional marker used for tracing requests through the system.
	 * @param {object} [options.initiator] - Optional marker used for identifying the user who generated the initial request.
	 * @returns {Promise}
	 */
	publish(routingKey, message, options) { // eslint-disable-line no-unused-vars
		return new Promise((resolve) => {
			options = options || {};
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!_.isUndefined(options.correlationId) && !_.isString(options.correlationId)) {
				throw new TypeError('"options.correlationId" should be a string.');
			}
			if (!_.isUndefined(options.initiator) && !_.isString(options.initiator)) {
				throw new TypeError('"options.initiator" should be a string.');
			}
			if (!this.transports.publish) {
				throw new Error('Cannot publish message, no publish Transport has been configured.');
			}
			resolve();
		})
		.then(() => this.transports.publish.publish(routingKey, message, options));
	}

	/**
	 * Publishes an RPC-style message that waits for a response.
	 * This base class implementation resolves to the correlationId of the message, either passed or generated.
	 * @param {string} routingKey - The routing key to attach to the message.
	 * @param {object} [message] - The message data to publish.
	 * @param {object} [options] - Optional publishing options.
	 * @param {object} [options.correlationId] - Optional marker used for tracing requests through the system.
	 * @param {object} [options.initiator] - Optional marker used for identifying the user who generated the initial request.
	 * @returns {Promise}
	 */
	request(routingKey, message, options) { // eslint-disable-line no-unused-vars
		return new Promise((resolve) => {
			options = options || {};
			if (!_.isString(routingKey)) {
				throw new TypeError('"routingKey" should be a string.');
			}
			if (!_.isUndefined(options.correlationId) && !_.isString(options.correlationId)) {
				throw new TypeError('"options.correlationId" should be a string.');
			}
			if (!_.isUndefined(options.initiator) && !_.isString(options.initiator)) {
				throw new TypeError('"options.initiator" should be a string.');
			}
			if (!this.transports.request) {
				throw new Error('Cannot send request, no request Transport has been configured.');
			}
			resolve();
		})
		.then(() => this.transports.request.request(routingKey, message, options));
	}
}

module.exports = PostmasterGeneral;
