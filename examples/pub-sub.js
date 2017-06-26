'use strict';

const PostmasterGeneral = require('../postmaster-general');

const postmaster = new PostmasterGeneral('pub-sub');

// Start the Postmaster instance.
postmaster.addListener('action:get_greeting', function (message, done) {
	console.log('[action:get_greeting] received');
	return done(null, {
		greeting: 'Hello, ' + message.name
	});
})
	// Add a listener callback.
	.then(() => postmaster.start())
	// Publish a fire-and-forget message.
	.then(() => postmaster.publish('action:get_greeting', {name: 'Bob'}))
	// Publish a message with a callback.
	.then(() => postmaster.request('action:get_greeting', {name: 'Steve'}))
	// Handle the callback.
	.then((res) => {
		console.log(res.greeting);
	});
