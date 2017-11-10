'use strict';

const PostmasterGeneral = require('../index');

const postmaster = new PostmasterGeneral({ logLevel: 'debug' });

const printGreeting = async (message) => {
	console.log('[action:get_greeting] received');
	return { greeting: 'Hello, ' + message.name };
};

// Start the Postmaster instance.
postmaster.connect()
	.then(() => postmaster.addListener('action:get_greeting', printGreeting))
	// Add a listener callback.
	.then(() => postmaster.startConsuming())
	// Publish a fire-and-forget message.
	.then(() => postmaster.publish('action:get_greeting', { name: 'Bob' }))
	// Publish a message with a callback.
	.then(() => postmaster.request('action:get_greeting', { name: 'Steve' }))
	// Handle the callback.
	.then((res) => {
		console.log(res.greeting);
	})
	// Shut everything down.
	.then(() => postmaster.shutdown());
