'use strict';

var PostmasterGeneral = require('../postmaster-general');

var options = {queue: 'app.js.queue', clientPins: ['action:get_greeting']};
var postmaster = new PostmasterGeneral(options);

postmaster.init((err) => {
	if (err) {
		console.error(err);
		return;
	}

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
