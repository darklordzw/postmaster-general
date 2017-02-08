'use strict';

const PostmasterGeneral = require('../postmaster-general').Postmaster;
const postmaster = new PostmasterGeneral();

// Start the Postmaster instance.
postmaster.start()
	.then(() => {
		// Register listeners.
		return postmaster.addListener('action:get_greeting', function (message, done) {
			console.log('[action:get_greeting] received');
			return done(null, {
				greeting: 'Hello, ' + message.name
			});
		});
	})
	.then(() => {
		// Publish a fire-and-forget message.
		return postmaster.publish('action:get_greeting', {
			name: 'Bob'
		});
	})
	.then(() => {
		// Publish a message with a callback.
		return postmaster.publish('action:get_greeting', {
			name: 'Steve'
		}, true)
			.then((res) => {
				console.log(res.greeting);
			});
	});
