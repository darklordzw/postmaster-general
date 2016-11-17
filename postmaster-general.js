'use strict';
/**
 * A simple wrapper for the excellent "seneca-amqp-transport"
 * plugin (https://github.com/senecajs/seneca-amqp-transport) that
 * abstracts the Seneca implementations and adds "fire-and-forget"
 * support for events.
 *
 * @module postmaster-general
 */
const Defaults = require('./defaults');
const Seneca = require('seneca');
const _ = require('lodash');

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
        this.options = options || Defaults;

        if (!this.options.pins) {
          throw new InvalidParameterException('Missing required option \'pins\'. Expecting an array of strings.');
        }
        this.pins = _.castArray(this.options.pins);

        if (!this.options.queue) {
          throw new InvalidParameterException('Missing required option \'queue\'. Expecting a string.');
        }
        this.queue = this.options.queue;

        if (!this.options.amqp.url) {
          throw new InvalidParameterException('Missing required option \'amqp.url\'. Expecting a string.');
        }
        this.amqpUrl = this.options.amqp.url;
      }

      addRecipient(pin, recipient) {
        if (!this.listener) {
          // Init listener.
          this.listener = Seneca()
            .use('./lib/seneca-amqp-transport');
        }
        this.listener.add(pin, recipient);
        return this;
      }

      send(pin, data, callback) {
        if (!pin) {
          throw new InvalidParameterException('Missing required parameter \'pin\'. Expecting a string.');
        }
        data = data || {};

        if (!this.publisher) {
          // Init publisher.
          this.publisher = Seneca()
            .use('./lib/seneca-amqp-transport')
            .client({
              type: 'amqp',
              pin: this.pins,
              url: this.amqpUrl
            });
        }

        if (callback) {
          return this.publisher.act(pin, data, callback);
        }

        // If no callback was provided, use "fire-and-forget" calling.
        // fatal$ tells Seneca.js that we shouldn't die if no response is sent.
        // skipReply$ indicates to the listener that a reply can be skipped.
        data.fatal$ = false;
        data.skipReply$ = true;
        return this.publisher.act(pin, data);
      }

      listen() {
        if (!this.listener) {
          throw new AMQPListenerException('Attempted to call listen() without binding any responders.');
        }
        return this.listener.listen({
          type: 'amqp',
          pin: this.pins,
          name: this.queue,
          url: this.amqpUrl
        });
      }
  };
