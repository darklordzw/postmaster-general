﻿# postmaster-general
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://github.com/darklordzw/postmaster-general/blob/master/LICENSE.md) [![Build Status](https://travis-ci.org/darklordzw/postmaster-general.svg?branch=master)](https://travis-ci.org/darklordzw/postmaster-general) [![Coverage Status](https://coveralls.io/repos/github/darklordzw/postmaster-general/badge.svg?branch=master)](https://coveralls.io/github/darklordzw/postmaster-general?branch=master)

Dead-simple, ready-to-use, promise-based Node.js library, for microservice communication over [AMQP][1] using [amqplib][3].

This library attempts to provide a simple, ready-to-go microservice message bus with default settings based on production usage.

## Install

```sh
npm install --save postmaster-general
```

## Usage
The following snippet showcases basic usage.

```js
const PostmasterGeneral = require('../postmaster-general');

const postmaster = new PostmasterGeneral('pub-sub');

// Start the Postmaster instance.
postmaster.addListener('action:get_greeting', function (message, done) {
	console.log('[action:get_greeting] received');
	return done(null, {
		greeting: 'Hello, ' + message.name
	});
})
	// Add a listener callback.
	.then(() => postmaster.start())
	// Publish a fire-and-forget message.
	.then(() => postmaster.publish('action:get_greeting', {name: 'Bob'}))
	// Publish a message with a callback.
	.then(() => postmaster.request('action:get_greeting', {name: 'Steve'}))
	// Handle the callback.
	.then((res) => {
		console.log(res.greeting);
	});

```

### Wildcards
postmaster-general supports the [default AMQP wildcards for topic routes][4].


## License
Licensed under the [MIT][2] license.

[1]: https://www.amqp.org/ 
[2]: ./LICENSE.md
[3]: https://github.com/squaremo/amqp.node
[4]: https://www.rabbitmq.com/tutorials/tutorial-five-javascript.html