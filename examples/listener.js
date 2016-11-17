'use strict';

var PostmasterGeneral = require('../postmaster-general');
var options = { queue: 'app.js.queue', pins: ['action:get_greeting'] };
var postmaster = new PostmasterGeneral(options);

postmaster.addRecipient('action:get_greeting', function (message, done) {
    console.log('[action:get_greeting] received');
    return done(null, {
        greeting: 'Hello, ' + message.name
    });
});

postmaster.listen();