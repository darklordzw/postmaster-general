# postmaster-general
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://github.com/darklordzw/postmaster-general/blob/master/LICENSE.md)

Simple, promise-based Node.js library, for microservice communication over [AMQP][1].
Supports both "fire-and-forget" and RPC calling patterns.

## Install

```sh
npm install --save postmaster-general
```

## Usage
The following snippet showcases basic usage.

```js
const PostmasterGeneral = require('../postmaster-general').Postmaster;
const postmaster = new PostmasterGeneral("pub-sub");

// Start the Postmaster instance.
postmaster.start()
	.then(() => {
		// Register listeners.
		return postmaster.addListener('action:get_greeting', function (message, done) {
			console.log('[action:get_greeting] received');
			return done(null, {
				greeting: 'Hello, ' + message.name
			});
		});
	})
	.then(() => {
		// Publish a fire-and-forget message.
		return postmaster.publish('action:get_greeting', {
			name: 'Bob'
		});
	})
	.then(() => {
		// Publish a message with a callback.
		return postmaster.publish('action:get_greeting', {
			name: 'Steve'
		}, true)
			.then((res) => {
				console.log(res.greeting);
			});
	});
```

## License
Licensed under the [MIT][2] license.

[1]: https://www.amqp.org/ 
[2]: ./LICENSE.md
