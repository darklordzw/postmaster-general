# postmaster-general
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://github.com/darklordzw/postmaster-general/blob/master/LICENSE.md)

Simple Node.js library, based on [seneca-amqp-transport][3], for microservice communication over [AMQP][1] using [Seneca.js][2].
Supports both "fire-and-forget" and RPC calling patterns.

## Install

```sh
npm install --save postmaster-general
```

## Usage
The following snippets showcase the most basic usage examples.

### Listener

```js
var PostmasterGeneral = require('postmaster-general');
var options = { queue: 'app.js.queue', listenerPins: ['action:get_greeting'] };
var postmaster = new PostmasterGeneral(options);

postmaster.init((err) => {
  if (err) {
    console.error(err);
    return;
  }
  postmaster.addRecipient('action:get_greeting', function (message, done) {
    console.log('[action:get_greeting] received');
    return done(null, {
        greeting: 'Hello, ' + message.name
    });
  });

  postmaster.listen();
});
```

### Publisher

```js
var PostmasterGeneral = require('postmaster-general');
var options = { queue: 'app.js.queue', clientPins: ['action:get_greeting'] };
var postmaster = new PostmasterGeneral(options);

postmaster.init((err) => {
  if (err) {
    console.error(err);
    return;
  }

  // fire-and-forget publish
  postmaster.send('action:get_greeting', {
    name: 'Bob'
  });

  // publish with expected response.
  postmaster.send('action:get_greeting', {
    name: 'Steve'
  }, (err, res) => {
    if (err) {
      throw err;
      }
      console.log(res.greeting);
  });
});
```

## License
Licensed under the [MIT][4] license.

[1]: https://www.amqp.org/ 
[2]: http://senecajs.org/
[3]: https://github.com/senecajs/seneca-amqp-transport/
[4]: ./LICENSE.md
