'use strict';

var PostmasterGeneral = require('../postmaster-general');
var options = require('../defaults');

options.queue = 'app.js.queue';
options.pins = ['action:get_time', 'level:*', 'proc:status'];

var client = new PostmasterGeneral(options);

client.send('action:get_time', {
    id: Math.floor(Math.random() * 91) + 10,
});

client.send('action:get_time', {
    id: Math.floor(Math.random() * 91) + 10
}, (err, res) => {
    if (err) {
        throw err;
    }
    console.log(res);
});