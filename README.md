# postmaster-general
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://github.com/darklordzw/postmaster-general/blob/master/LICENSE.md) [![Build Status](https://travis-ci.org/darklordzw/postmaster-general.svg?branch=master)](https://travis-ci.org/darklordzw/postmaster-general) [![Coverage Status](https://coveralls.io/repos/github/darklordzw/postmaster-general/badge.svg?branch=master)](https://coveralls.io/github/darklordzw/postmaster-general?branch=master)

Simple, promise-based Node.js library for microservice communication over a variety of transport protocols.

## Transport Plugins
postmaster-general functions using a variety of transport plugins that should be installed as peer-dependencies.

* [HTTP][2]
* [Amazon Web Services SNS & SQS][3]

## Install

```sh
npm install --save postmaster-general
```

## Usage
The following snippet showcases basic usage.

```js
const PostmasterGeneral = require('postmaster-general');
const HTTPTransport = require('postmaster-general-http-transport');

const transport = new HTTPTransport();
const postmaster = new PostmasterGeneral({
	requestTransport: transport,
	publishTransport: transport
});

const printGreeting = (message) => {
	console.log('[action:get_greeting] received');
	return Promise.resolve({ greeting: 'Hello, ' + message.name });
};

// Start the Postmaster instance.
postmaster.connect()
	.then(() => postmaster.addRequestListener('action:get_greeting', printGreeting))
	.then(() => postmaster.listen())
	.then(() => postmaster.request('action:get_greeting', { name: 'Steve' }))
	.then((res) => {
		// Handle the response.
		console.log(res.greeting);
	})
	.then(() => postmaster.disconnect())
	.catch((err) => {
		console.log(err.message);
	});

```

### Options
See the "docs" folder for documentation.

## License
Licensed under the [MIT][1] license.

[1]: ./LICENSE.md
[2]: https://github.com/darklordzw/postmaster-general-http-transport
[3]: https://github.com/darklordzw/postmaster-general-aws-transport
[4]: ./docs