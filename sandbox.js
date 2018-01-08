const amqp = require('amqp-connection-manager');

// Create a new connection manager
const connection = amqp.connect(['amqp://localhost']);

// Ask the connection manager for a ChannelWrapper.  Specify a setup function to run every time we reconnect
// to the broker.
const channelWrapper = connection.createChannel({
	json: true,
	setup: async (channel) => {
		// `channel` here is a regular amqplib `ConfirmChannel`.
		await channel.assertQueue('rxQueueName', { durable: true });
		await channel.consume('rxQueueName', onMessage);
	}
});

async function onMessage(data) {
	const message = JSON.parse(data.content.toString());
	console.log('subscriber: got message', message);
	channelWrapper.ack(data);
	try {
		channelWrapper.ack(data);
		channelWrapper.ack(data);
		channelWrapper.ack(data);
	} catch (err) {
		console.log(`caught error: ${err.message}`);
	}
}

// Send some messages to the queue.  If we're not currently connected, these will be queued up in memory
// until we connect.  Note that `sendToQueue()` and `publish()` return a Promise which is fulfilled or rejected
// when the message is actually sent (or not sent.)
channelWrapper.waitForConnect()
.then(() => channelWrapper.sendToQueue('rxQueueName', { hello: 'world' }));
