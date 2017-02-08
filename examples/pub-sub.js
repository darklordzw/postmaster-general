'use strict';

const PostmasterGeneral = require('../postmaster-general').PostmasterGeneral;

const postmaster = new PostmasterGeneral('pub-sub');

// Start the Postmaster instance.
postmaster.start()
	// Add a listener callback.
	.then(() => postmaster.addListener('action:get_greeting', function (message, done) {
		console.log('[action:get_greeting] received');
		return done(null, {
			greeting: 'Hello, ' + message.name
		});
	}))
	// Publish a fire-and-forget message.
	.then(() => postmaster.publish('action:get_greeting', {name: 'Bob'}, null))
	// Publish a message with a callback.
	.then(() => postmaster.publish('action:get_greeting', {name: 'Steve'}, null, true))
	// Handle the callback.
	.then((res) => {
		console.log(res.greeting);
	});
