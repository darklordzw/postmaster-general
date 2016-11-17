'use strict';
/**
 * @module postmaster-general
 */
const Seneca = require('seneca');
const _ = require('lodash');

module.exports =
    class AMQPSenecaClient {
      constructor(options) {
        // Check required params.
        this.options = options || {};
        if (!this.options.pins) {
          throw 'Missing required parameter \'pins\'. Expecting an array of strings.';
        }
        this.pins = _.castArray(this.options.pins);
        if (!this.options.queue) {
          throw 'Missing required parameter \'queue\'. Expecting a string.';
        }
        this.queue = _.castArray(this.options.queue);

        // Init listener.
        this.listener = Seneca()
          .use('./lib/seneca-amqp-transport');

        // Init publisher.
        this.publisher = Seneca()
          .use('./lib/seneca-amqp-transport')
          .client({
            type: 'amqp',
            pin: this.pins,
            url: process.env.AMQP_URL
          });
      }

      addRecipient(pin, recipient) {
        this.listener.add(pin, recipient);
        return this;
      }

      send(pin, data, callback) {
        if (!pin) {
          throw 'Missing required parameter \'pin\'. Expecting a string.';
        }
        data = data || {};
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
        return this.listener.listen({
          type: 'amqp',
          pin: this.pins,
          name: this.queue,
          url: process.env.AMQP_URL
        });
      }
  };
