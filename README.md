# postmaster-general
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://github.com/darklordzw/postmaster-general/blob/master/LICENSE.md) [![Build Status](https://travis-ci.org/darklordzw/postmaster-general.svg?branch=master)](https://travis-ci.org/darklordzw/postmaster-general) [![Coverage Status](https://coveralls.io/repos/github/darklordzw/postmaster-general/badge.svg?branch=master)](https://coveralls.io/github/darklordzw/postmaster-general?branch=master)

Dead-simple, ready-to-use, promise-based Node.js library for microservice communication over [AMQP][1] using [amqplib][3].

## Features
postmaster-general strives to be unopinionated, serving as a thin wrapper around [amqplib][3] that sets some sensible defaults and encourages best-practices, including:

* Dedicated channels for publishing, consuming, and asserting topology.
* Single-purpose listener queues, no mixing messages.
* Built-in RPC support.
* Automatic reconnect on connection failure and channel error.

## Install

Note: This library makes heavy use of the async/await keywords, and requires Node.js 7+.

```sh
npm install --save postmaster-general
```

## Usage
The following snippet showcases basic usage.

```js
const PostmasterGeneral = require('postmaster-general');

const postmaster = new PostmasterGeneral({ logLevel: 'debug' });

const printGreeting = async (message) => {
	console.log('[action:get_greeting] received');
	return { greeting: 'Hello, ' + message.name };
};

// Start the Postmaster instance.
postmaster.connect()
	.then(() => postmaster.addRabbitMQListener('action:get_greeting', printGreeting))
	// Add a listener callback.
	.then(() => postmaster.startConsuming())
	// Publish a fire-and-forget message.
	.then(() => postmaster.publish('action:get_greeting', { name: 'Bob' }))
	// Publish a message with a callback.
	.then(() => postmaster.request('action:get_greeting', { name: 'Steve' }))
	// Handle the callback.
	.then((res) => {
		console.log(res.greeting);
	})
	// Shut everything down.
	.then(() => postmaster.shutdown())
	.catch((err) => {
		console.log(err.message);
	})
	.then(() => {
		process.exit();
	});

```

### Options
The following options may be overridden in the constructor:

```json
{
	"connectRetryDelay": 1000,
	"connectRetryLimit": 3,
	"deadLetterExchange": "postmaster.dlx",
	"handlerTimingResetInterval": 30000,
	"heartbeat": 30,
	"logLevel": "warn",	
	"pm2": false,
	"pm2InstanceVar": "NODE_APP_INSTANCE",
	"publishRetryDelay": 1000,
	"publishRetryLimit": 3,
	"queuePrefix": "postmaster.queue",
	"removeListenerRetryDelay": 1000,
	"removeListenerRetryLimit": 3,
	"shutdownTimeout": 30000,
	"url": "amqp://guest:guest@localhost:5672"
}
```

## License
Licensed under the [MIT][2] license.

[1]: https://www.amqp.org/ 
[2]: ./LICENSE.md
[3]: https://github.com/squaremo/amqp.node
[4]: https://www.rabbitmq.com/tutorials/tutorial-five-javascript.html