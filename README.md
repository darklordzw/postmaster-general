# postmaster-general
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://github.com/darklordzw/postmaster-general/blob/master/LICENSE.md)

Simple, promise-based Node.js library, for microservice communication over [AMQP][1] using [rabbot][3].

Note: Version 1.0.0 of this library represents a big departure from version 0.2.0. Be sure to check out
the examples to make sure you're migrating properly.

## Install

```sh
npm install --save postmaster-general
```

## Usage
The following snippet showcases basic usage.

```js
const PostmasterGeneral = require('../postmaster-general').PostmasterGeneral;

const postmaster = new PostmasterGeneral('pub-sub');

// Start the Postmaster instance.
postmaster.start()
	// Add a listener callback.
	.then(() => postmaster.addListener('action:get_greeting', function (message, done) {
		console.log('[action:get_greeting] received');
		return done(null, {
			greeting: 'Hello, ' + message.name
		});
	}))
	// Publish a fire-and-forget message.
	.then(() => postmaster.publish('action:get_greeting', {name: 'Bob'}))
	// Publish a message with a callback.
	.then(() => postmaster.publish('action:get_greeting', {name: 'Steve'}, {replyRequired: true}))
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
[3]: https://github.com/arobson/rabbot
[4]: https://www.rabbitmq.com/tutorials/tutorial-five-python.html