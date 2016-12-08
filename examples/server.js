'use strict';

var PostmasterGeneral = require('../postmaster-general');
var options = { queue: 'app.js.queue', listenerPins: ['action:get_greeting'], clientPins: ['action:get_greeting'] };
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