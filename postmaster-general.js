'use strict';
/**
 * A simple wrapper for the excellent "seneca-amqp-transport"
 * plugin (https://github.com/senecajs/seneca-amqp-transport) that
 * abstracts the Seneca implementations and adds "fire-and-forget"
 * support for events.
 *
 * @module postmaster-general
 */
const Seneca = require('seneca');
const _ = require('lodash');
const Defaults = require('./defaults');

function InvalidParameterException(message) {
	this.message = message;
	this.name = 'InvalidParameterException';
}

function AMQPListenerException(message) {
	this.message = message;
	this.name = 'AMQPListenerException';
}

module.exports =
	class AMQPSenecaClient {
		constructor(options) {
			this.options = options || Defaults.amqp;

			this.clientPins = _.castArray(this.options.clientPins || []);
			this.listenerPins = _.castArray(this.options.listenerPins || []);

			if (!this.options.queue) {
				throw new InvalidParameterException('Missing required option \'queue\'. Expecting a string.');
			}
			this.queue = this.options.queue;

			this.amqpUrl = this.options.url || 'amqp://localhost';
		}

		init(cb) {
			this.seneca = Seneca().use('./lib/seneca-amqp-transport');
			this.seneca.ready((err) => {
				if (err) {
					return cb(err);
				}
				// Init the client if pins were passed. We need to do this before the listeners are defined.
				if (this.clientPins.length > 0) {
					this.publisher = this.seneca.client({
						type: 'amqp',
						pin: this.clientPins,
						url: this.amqpUrl
					});
				}
				return cb();
			});
		}

		addRecipient(pin, recipient) {
			this.seneca.add(pin, recipient);
			return this;
		}

		send(pin, data, callback) {
			if (!pin) {
				throw new InvalidParameterException('Missing required parameter \'pin\'. Expecting a string.');
			}
			data = data || {};

			// Need to set a default to prevent act_not_found issue.
			data.default$ = {};

			if (callback) {
				return this.publisher.act(pin, data, callback);
			}

			// If no callback was provided, use "fire-and-forget" calling.
			// skipReply$ indicates to Seneca that a reply can be skipped.
			data.skipReply$ = true;
			return this.publisher.act(pin, data);
		}

		listen() {
			if (!this.seneca) {
				throw new AMQPListenerException('Attempted to call listen() without first calling init().');
			}
			return this.seneca.listen({
				type: 'amqp',
				pin: this.listenerPins,
				name: this.queue,
				url: this.amqpUrl
			});
		}

		close() {
			if (this.seneca) {
				this.seneca.close();
			}
		}
	};
