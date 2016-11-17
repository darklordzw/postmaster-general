'use strict';

var PostmasterGeneral = require('../postmaster-general');
var options = { queue: 'app.js.queue', pins: ['action:get_greeting'] };
var postmaster = new PostmasterGeneral(options);

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