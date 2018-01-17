'use strict';

/**
 * An HTTP transport module for postmaster-general.
 * @module lib/transports/http
 */

const querystring = require('querystring');
const express = require('express');
const rp = require('request-promise');
const rpErrors = require('request-promise/errors');
const Transport = require('./transport');
const errors = require('./errors');

class HTTPTransport extends Transport {
	constructor(options) {
		super();
		options = options || {};
		this.port = options.port;
		this.app = express();
		this.server = null;
		this.router = express.router();

		this.app.use((req, res, next) => {
			this.router(req, res, next);
		});
	}

	/**
	 * Disconnects the transport from any services it references.
	 * @returns {Promise}
	 */
	disconnect() {
		return new Promise((resolve) => {
			this.server.close(() => {
				resolve();
			});
		});
	}

	/**
	 * Starts listening to messages.
	 * @returns {Promise}
	 */
	listen() {
		return new Promise((resolve) => {
			this.server = this.app.listen(this.port, () => {
				resolve();
			});
		});
	}

	/**
	 * Processes a routing key into a format appropriate for the transport type.
	 * @param {string} routingKey - The routing key to convert.
	 * @returns {string}
	 */
	resolveTopic(routingKey) {
		return routingKey.replace(/:/g, '/');
	}

	/**
	 * Adds a new message handler.
	 * @param {string} routingKey - The routing key of the message to handle.
	 * @param {Function} callback - The function to call when a new message is received.
	 * @param {object} [options] - Optional params for configuring the handler.
	 * @returns {Promise}
	 */
	addListener(routingKey, callback, options) {
		return new Promise((resolve) => {
			options = options || {};
			options.httpMethod = (options.httpMethod || 'get').toLowerCase();

			const topic = this.resolveTopic(routingKey);
			const handler = callback;

			switch (options.httpMethod) {
				case 'get':
					this.router.get(topic, handler);
					break;
				case 'post':
					this.router.post(topic, handler);
					break;
				case 'put':
					this.router.put(topic, handler);
					break;
				case 'delete':
					this.router.delete(topic, handler);
					break;
				case 'all':
					this.router.all(topic, handler);
					break;
				default:
					throw new TypeError(`${options.httpMethod} is an unsupported method.`);
			}

			resolve();
		});
	}

	/**
	 * Deletes a message handler.
	 * @param {string} routingKey - The routing key of the handler to remove.
	 * @returns {Promise}
	 * @see {https://github.com/expressjs/express/issues/2596}
	 */
	removeListener(routingKey) {
		return new Promise((resolve) => {
			const topic = this.resolveTopic(routingKey);
			const newRouter = express.router();

			for (const handler of this.router.stack) {
				const method = handler.route.method.toLowerCase();
				for (const layer of handler.route.stack) {
					if (layer.path !== topic) {
						switch (method) {
							case 'get':
								newRouter.get(layer.path, layer.handle);
								break;
							case 'post':
								this.router.post(layer.path, layer.handle);
								break;
							case 'put':
								this.router.put(layer.path, layer.handle);
								break;
							case 'delete':
								this.router.delete(layer.path, layer.handle);
								break;
							default:
								newRouter.all(layer.path, layer.handle);
								break;
						}
					}
				}
			}

			this.router = newRouter;
			resolve();
		});
	}

	/**
	 * Publishes a fire-and-forget message that is not expected to return a meaningful response.
	 * In the case of this HTTP transport, this is just a passthrough to "request".
	 * @param {string} routingKey - The routing key to attach to the message.
	 * @param {object} [message] - The message data to publish.
	 * @param {object} [options] - Optional publishing options.
	 * @returns {Promise}
	 */
	publish(routingKey, message, options) {
		return this.request(routingKey, message, options);
	}

	/**
	 * Publishes an RPC-style message that waits for a response.
	 * @param {string} routingKey - The routing key to attach to the message.
	 * @param {object} [message] - The message data to publish.
	 * @param {object} [options] - Optional publishing options.
	 * @returns {Promise}
	 */
	request(routingKey, message, options) {
		return new Promise((resolve) => {
			const topic = this.resolveTopic(routingKey);
			message = message || {};
			options = options || {};

			const reqSettings = {
				uri: `${options.httpProtocol || 'http'}://${topic}`,
				method: options.httpMethod || 'GET',
				headers: options.headers,
				json: typeof options.json === 'undefined' ? true : options.json
			};

			if (reqSettings.httpMethod === 'GET') {
				reqSettings.qs = querystring.stringify(message);
			} else {
				reqSettings.body = message;
			}

			resolve(rp(reqSettings)
				.catch(rpErrors.StatusCodeError, (reason) => {
					switch (reason.statusCode) {
						case 400:
							throw new errors.InvalidMessageError(reason.error.message, reason.response);
						case 401:
							throw new errors.UnauthorizedError(reason.error.message, reason.response);
						case 403:
							throw new errors.ForbiddenError(reason.error.message, reason.response);
						case 404:
							throw new errors.NotFoundError(reason.error.message, reason.response);
						default:
							throw new errors.ResponseProcessingError(reason.error.message, reason.response);
					}
				})
				.catch((err) => {
					throw new errors.RequestError(err);
				}));
		});
	}
}

module.exports = HTTPTransport;
