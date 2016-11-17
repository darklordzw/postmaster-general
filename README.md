# postmaster-general
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://github.com/darklordzw/postmaster-general/blob/master/LICENSE.md)

Simple Node.js library for microservice communication over [AMQP][1] using [Seneca.js][2] and [seneca-amqp-transport][3].

## Install

```sh
npm install --save postmaster-general
```

## Usage
The following snippets showcase the most basic usage examples.

### Listener

```js
var PostmasterGeneral = require('postmaster-general');
var options = { queue: 'app.js.queue', pins: ['action:get_time', 'level:*', 'proc:status'] };
var postmaster = new PostmasterGeneral(options);

postmaster.addRecipient('action:get_time', function (message, done) {
    console.log(`[action:get_time] Action ${message.id} received`);
    return done(null, {
        pid: process.pid,
        time: 'Current time is ' + Date.now() + 'ms'
    });
});

postmaster.listen();
```

### Publisher

```js
var PostmasterGeneral = require('postmaster-general');
var options = { queue: 'app.js.queue', pins: ['action:get_time', 'level:*', 'proc:status'] };
var postmaster = new PostmasterGeneral(options);

// fire-and-forget publish
postmaster.send('action:get_time', {
    id: Math.floor(Math.random() * 91) + 10,
});

// publish with expected response.
postmaster.send('action:get_time', {
    id: Math.floor(Math.random() * 91) + 10
}, (err, res) => {
    if (err) {
        throw err;
    }
    console.log(res);
});
```

## License
Licensed under the [MIT][4] license.

[1]: https://www.amqp.org/ 
[2]: http://senecajs.org/
[3]: https://github.com/senecajs/seneca-amqp-transport/
[4]: ./LICENSE.md
