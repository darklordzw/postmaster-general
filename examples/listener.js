'use strict';

var PostmasterGeneral = require('../postmaster-general');
var options = require('../defaults');

options.queue = 'app.js.queue';
options.pins = ['action:get_time', 'level:*', 'proc:status'];

var client = new PostmasterGeneral(options);

client.addRecipient('action:get_time', function (message, done) {
        console.log(`[action:get_time] Action ${message.id} received`);
        return done(null, {
            pid: process.pid,
            time: 'Current time is ' + Date.now() + 'ms'
        });
    })
    .addRecipient('level:log', function (message, done) {
        console[message.level](`[level:log] Action ${message.id} wants to log: ${message.text}`);
        return done(null, {
            pid: process.pid,
            status: `Message ${message.id} logged successfully`
        });
    })
    .addRecipient('proc:status', function (message, done) {
        console.log(`[action:status] Action ${message.id} received`);
        return done(null, {
            pid: process.pid,
            status: `Process ${process.pid} status: OK`
        });
    })
    .listen();