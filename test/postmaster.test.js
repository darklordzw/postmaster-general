/* eslint import/no-unassigned-import: 'off' */
/* eslint max-nested-callbacks: 'off' */
'use strict';

const chai = require('chai');
const dirtyChai = require('dirty-chai');
const sinon = require('sinon');
const Promise = require('bluebird');
const PostmasterGeneral = require('../index');
const defaults = require('../defaults.json');

/* This sets up the Chai assertion library. "should" and "expect"
initialize their respective assertion properties. The "use()" functions
load plugins into Chai. "dirtyChai" just allows assertion properties to
use function call syntax ("calledOnce()" vs "calledOnce"). It makes them more
acceptable to the linter. */
const expect = chai.expect;
chai.should();
chai.use(dirtyChai);

describe('constructor:', () => {
	it('should properly initialize settings from defaults', () => {
		const postmaster = new PostmasterGeneral({ logLevel: 'off' });
		expect(postmaster._connection).to.be.null();
		postmaster._connecting.should.be.false();
		postmaster._shuttingDown.should.be.false();
		postmaster._channels.should.be.empty();
		postmaster._handlers.should.be.empty();
		expect(postmaster._handlerTimingsTimeout).to.be.null();
		expect(postmaster._replyConsumerTag).to.be.null();
		postmaster._replyHandlers.should.be.empty();
		postmaster._shouldConsume.should.be.false();
		postmaster._topology.should.not.be.empty();
		postmaster._topology.exchanges.should.not.be.empty();
		postmaster._topology.queues.should.not.be.empty();
		expect(postmaster._createChannel).to.be.null();
		postmaster._connectRetryDelay.should.equal(defaults.connectRetryDelay);
		postmaster._connectRetryLimit.should.equal(defaults.connectRetryLimit);
		postmaster._deadLetterExchange.should.equal(defaults.deadLetterExchange);
		postmaster._defaultExchange.should.equal(defaults.exchanges.topic);
		postmaster._handlerTimingResetInterval.should.equal(defaults.handlerTimingResetInterval);
		postmaster._heartbeat.should.equal(defaults.heartbeat);
		postmaster._publishRetryDelay.should.equal(defaults.publishRetryDelay);
		postmaster._publishRetryLimit.should.equal(defaults.publishRetryLimit);
		postmaster._removeListenerRetryDelay.should.equal(defaults.removeListenerRetryDelay);
		postmaster._removeListenerRetryLimit.should.equal(defaults.removeListenerRetryLimit);
		postmaster._replyTimeout.should.equal(postmaster._publishRetryDelay * postmaster._publishRetryLimit * 2);
		postmaster._queuePrefix.should.equal(defaults.queuePrefix);
		postmaster._shutdownTimeout.should.equal(defaults.shutdownTimeout);
		postmaster._url.should.equal(defaults.url);
		expect(postmaster._logger).to.exist();
		expect(postmaster._topology.queues.reply).to.exist();
		postmaster._topology.queues.reply.options.should.not.be.empty();
		postmaster._topology.queues.reply.options.noAck.should.be.true();
		postmaster._topology.queues.reply.options.expires.should.equal((postmaster._connectRetryDelay * postmaster._connectRetryLimit) + (60000 * postmaster._connectRetryLimit));
	});
});

describe('_resolveTopic:', () => {
	it('should use a topic name starting with the action prefix', () => {
		const postmaster = new PostmasterGeneral({ logLevel: 'off' });
		const topic = postmaster._resolveTopic('role:create');
		topic.should.contain('role.');
	});
});

describe('outstandingMessageCount:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	it('should return 0 if there are no outstanding messages', () => {
		postmaster.outstandingMessageCount.should.equal(0);
	});

	it('should count reply handlers', () => {
		postmaster._replyHandlers.test = 'dummy value';
		postmaster.outstandingMessageCount.should.equal(1);
	});

	it('should count oustanding messages from listeners', () => {
		postmaster._handlers.test = { outstandingMessages: new Set() };
		postmaster._handlers.test.outstandingMessages.add('test');
		postmaster.outstandingMessageCount.should.equal(1);
	});

	it('should sum replies and oustanding messages from listeners', () => {
		postmaster._replyHandlers.test = 'dummy value';
		postmaster._handlers.test = { outstandingMessages: new Set() };
		postmaster._handlers.test.outstandingMessages.add('test');
		postmaster.outstandingMessageCount.should.equal(2);
	});
});

describe('handlerTimings:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	it('should return an empty object if there are no timings', () => {
		postmaster.handlerTimings.should.be.empty();
	});

	it('should return proper timings if set', () => {
		postmaster._handlers.test = { timings: { dummyKey: 'dummyValue' } };
		Object.keys(postmaster.handlerTimings).length.should.equal(1);
	});
});

describe('connect:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off', shutdownTimeout: 1000 });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should resolve when successfully connected', async () => {
		await postmaster.connect();
		expect(postmaster._connection).to.not.be.null();
		await postmaster._channels.topology.checkExchange(postmaster._defaultExchange.name);
	});

	it('should reject if unable to connect', async () => {
		postmaster._url = 'bad url';
		try {
			await postmaster.connect();
		} catch (err) {
			return;
		}
		throw new Error('connect() failed to reject when unable to connect!');
	}).timeout(5000);

	it('should set the _createChannel function', async () => {
		expect(postmaster._createChannel).to.not.exist();
		await postmaster.connect();
		expect(postmaster._createChannel).to.exist();
	});

	it('should properly create all channels', async () => {
		postmaster._channels.should.be.empty();
		await postmaster.connect();
		postmaster._channels.should.not.be.empty();
		expect(postmaster._channels.publish).to.exist();
		expect(postmaster._channels.replyPublish).to.exist();
		expect(postmaster._channels.topology).to.exist();
		expect(postmaster._channels.consumers).to.exist();
		expect(postmaster._channels.consumers[postmaster._topology.queues.reply.name]).to.exist();
	});

	it('should try to assert topology', async () => {
		const spy = sinon.spy(postmaster._assertTopology);
		await postmaster.connect();
		spy.should.have.been.calledOnce; // eslint-disable-line no-unused-expressions
	});

	it('should close existing connections before reconnecting', async () => {
		await postmaster.connect();
		const spy = sinon.spy(postmaster._connection.close);
		await postmaster.connect();
		spy.should.have.been.calledOnce; // eslint-disable-line no-unused-expressions
	});

	it('should reconnect if the connection is lost due to channel error', async () => {
		await postmaster.connect();
		try {
			await postmaster._channels.topology.checkExchange('invalid exchange');
		} catch (err) {}
		await Promise.delay(1000);
		expect(postmaster._connection).to.not.be.null();
		await postmaster._channels.topology.checkExchange(postmaster._defaultExchange.name);
	}).timeout(5000);

	it('should emit an error event if the connection is lost and cannot be recovered', async () => {
		await postmaster.connect();
		postmaster._url = 'bad url';
		const onError = (err) => {
			expect(err.message).to.exist();
		};
		const spy = sinon.spy(onError);
		postmaster.on('error', onError);
		try {
			await postmaster._channels.topology.checkExchange('invalid exchange');
		} catch (err) {}
		await Promise.delay(3000);
		spy.should.have.been.calledOnce; // eslint-disable-line no-unused-expressions
	}).timeout(5000);
});

describe('shutdown:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should resolve when successfully shutdown', async () => {
		await postmaster.connect();
		await postmaster.shutdown();
	});

	it('should stop consuming prior to shutdown', async () => {
		await postmaster.connect();
		const spy = sinon.spy(postmaster.stopConsuming);
		await postmaster.shutdown();
		spy.should.have.been.calledOnce; // eslint-disable-line no-unused-expressions
	});

	it('should retry if called while outstanding messages are processing', async () => {
		await postmaster.connect();
		postmaster._replyHandlers.test = 'dummy value';
		const shutdownPromise = postmaster.shutdown();
		expect(postmaster._connection).to.not.be.null();
		await postmaster._channels.topology.checkExchange(postmaster._defaultExchange.name);
		delete postmaster._replyHandlers.test;
		await Promise.delay(1000);
		try {
			await postmaster._channels.topology.checkExchange(postmaster._defaultExchange.name);
		} catch (err) {
			return shutdownPromise;
		}
		throw new Error('Failed to retry shutdown!');
	}).timeout(5000);
});

describe('assertExchange:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should assert the exchange and store the topology entry', async () => {
		await postmaster.connect();
		expect(postmaster._topology.exchanges.aeTest).to.not.exist();
		await postmaster.assertExchange('aeTest', 'topic');
		expect(postmaster._topology.exchanges.aeTest).to.exist();
	});
});

describe('assertQueue:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should assert the queue and store the topology entry', async () => {
		await postmaster.connect();
		expect(postmaster._topology.queues.aqTest).to.not.exist();
		await postmaster.assertQueue('aqTest');
		expect(postmaster._topology.queues.aqTest).to.exist();
	});
});

describe('assertBinding:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should assert the binding and store the topology entry', async () => {
		await postmaster.connect();
		expect(postmaster._topology.bindings.aqTest_aeTest).to.not.exist();
		await postmaster.assertBinding('aqTest', 'aeTest', 'test.topic');
		expect(postmaster._topology.bindings.aqTest_aeTest).to.exist();
	});
});

describe('assertTopology:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should assert predefined exchanges', async () => {
		postmaster._topology.exchanges.atExchange = { name: 'atExchange' };
		const spy = sinon.spy(postmaster.assertExchange);
		await postmaster.connect();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
		await postmaster._channels.topology.checkExchange('atExchange');
	});

	it('should assert predefined queues', async () => {
		postmaster._topology.queues.atQueue = { name: 'atQueue' };
		const spy = sinon.spy(postmaster.assertQueue);
		await postmaster.connect();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
		await postmaster._channels.topology.checkQueue('atQueue');
	});

	it('should assert predefined bindings', async () => {
		postmaster._topology.bindings.atQueue_atExchange = { queue: 'atQueue', exchange: 'atExchange', topic: 'atTestTopic' }; // eslint-disable-line camelcase
		const spy = sinon.spy(postmaster.assertBinding);
		await postmaster.connect();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
	});
});

describe('startConsuming:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should reset handler timings', async () => {
		await postmaster.connect();
		const spy = sinon.spy(postmaster._resetHandlerTimings);
		await postmaster.startConsuming();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
	});

	it('should start consuming on the reply queue', async () => {
		await postmaster.connect();
		const spy = sinon.spy(postmaster._channels.consumers[postmaster._topology.queues.reply.name].consume);
		expect(postmaster._replyConsumerTag).to.not.exist();
		await postmaster.startConsuming();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
		expect(postmaster._replyConsumerTag).to.exist();
	});

	it('should start consuming on the listener queues', async () => {
		await postmaster.connect();
		await postmaster.addRabbitMQListener('sctest', () => {
			return Promise.resolve();
		});
		const spy = sinon.spy(postmaster._channels.consumers['postmaster.queue.sctest'].consume);
		expect(postmaster._handlers.sctest.consumerTag).to.not.exist();
		await postmaster.startConsuming();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
		expect(postmaster._handlers.sctest.consumerTag).to.exist();
	});
});

describe('stopConsuming:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should reset handler timings', async () => {
		await postmaster.connect();
		await postmaster.startConsuming();
		const spy = sinon.spy(postmaster._resetHandlerTimings);
		await postmaster.stopConsuming();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
	});

	it('should stop consuming on the reply queue', async () => {
		await postmaster.connect();
		await postmaster.startConsuming();
		const spy = sinon.spy(postmaster._channels.consumers[postmaster._topology.queues.reply.name].cancel);
		expect(postmaster._replyConsumerTag).to.exist();
		await postmaster.stopConsuming();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
		expect(postmaster._replyConsumerTag).to.not.exist();
	});

	it('should stop consuming on the listener queues', async () => {
		await postmaster.connect();
		await postmaster.addRabbitMQListener('sctest', () => {
			return Promise.resolve();
		});
		await postmaster.startConsuming();
		const spy = sinon.spy(postmaster._channels.consumers['postmaster.queue.sctest'].cancel);
		expect(postmaster._handlers.sctest.consumerTag).to.exist();
		await postmaster.stopConsuming();
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
		expect(postmaster._handlers.sctest.consumerTag).to.not.exist();
	});
});

describe('removeRabbitMQListener:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should cancel a consumer before removing', async () => {
		await postmaster.connect();
		await postmaster.addRabbitMQListener('rltest', async (msg) => {
			if (msg.data === 'test') {
				return true;
			}
			return false;
		});
		await postmaster.startConsuming();
		const spy = sinon.spy(postmaster._channels.consumers['postmaster.queue.rltest'].cancel);
		const spy2 = sinon.spy(postmaster._channels.consumers['postmaster.queue.rltest'].close);
		expect(postmaster._handlers.rltest.consumerTag).to.exist();
		expect(postmaster._channels.consumers['postmaster.queue.rltest']).to.exist();
		expect(postmaster._handlers.rltest).to.exist();
		expect(postmaster._topology.bindings[`postmaster.queue.rltest_${postmaster._defaultExchange.name}`]).to.exist();
		await postmaster.removeRabbitMQListener('rltest');
		spy.should.have.been.called; // eslint-disable-line no-unused-expressions
		spy2.should.have.been.called; // eslint-disable-line no-unused-expressions
		expect(postmaster._channels.consumers['postmaster.queue.rltest']).to.not.exist();
		expect(postmaster._handlers.rltest).to.not.exist();
		expect(postmaster._topology.bindings[`postmaster.queue.rltest_${postmaster._defaultExchange.name}`]).to.not.exist();
	});
});

describe('publish:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should send a fire-and-forget message', async () => {
		await postmaster.connect();
		let received = false;
		await postmaster.addRabbitMQListener('pubtest', (msg) => {
			if (msg.data === 'test') {
				received = true;
			}
			return Promise.resolve();
		});
		await postmaster.startConsuming();
		received.should.be.false();
		await postmaster.publish('pubtest', { data: 'test' });
		await Promise.delay(100);
		received.should.be.true();
	});

	it('should resolve even if connection is down', async () => {
		await postmaster.publish('pubtest', { data: 'test' });
	}).timeout(5000);

	it('should hold messages until connection is established', async () => {
		postmaster._connecting = true;
		const pubPromise = postmaster.publish('pubtest', { data: 'test' });
		await postmaster.connect();
		let received = false;
		await postmaster.addRabbitMQListener('pubtest', (msg) => {
			if (msg.data === 'test') {
				received = true;
			}
			return Promise.resolve();
		});
		await postmaster.startConsuming();
		received.should.be.false();
		await pubPromise;
		await Promise.delay(100);
		received.should.be.true();
	});
});

describe('request:', () => {
	let postmaster;

	beforeEach(() => {
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				await postmaster.shutdown();
			}
		} catch (err) {}
	});

	it('should send an RPC message', async () => {
		await postmaster.connect();
		await postmaster.addRabbitMQListener('pubtest', async (msg) => {
			if (msg.data === 'test') {
				return true;
			}
			return false;
		});
		await postmaster.startConsuming();
		const received = await postmaster.request('pubtest', { data: 'test' });
		received.should.be.true();
	});

	it('should hold messages until connection is established', async () => {
		postmaster._connecting = true;
		const pubPromise = postmaster.request('pubtest', { data: 'test' });
		await postmaster.connect();
		await postmaster.addRabbitMQListener('pubtest', async (msg) => {
			if (msg.data === 'test') {
				return true;
			}
			return false;
		});
		await postmaster.startConsuming();
		const received = await pubPromise;
		received.should.be.true();
	});
});
