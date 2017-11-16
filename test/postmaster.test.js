/* eslint import/no-unassigned-import: 'off' */
/* eslint max-nested-callbacks: 'off' */
'use strict';

const chai = require('chai');
const dirtyChai = require('dirty-chai');
const sinon = require('sinon');
const uuidv4 = require('uuid/v4');
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
		postmaster._consumerPrefetch.should.equal(defaults.consumerPrefetch);
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
		postmaster = new PostmasterGeneral({ logLevel: 'off' });
	});

	afterEach(async () => {
		try {
			if (postmaster._connection) {
				postmaster.shutdown();
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
				postmaster.shutdown();
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

	it('should retry if called while attempting to connect', async () => {
		await postmaster.connect();
		postmaster._connecting = true;
		const shutdownPromise = postmaster.shutdown();
		expect(postmaster._connection).to.not.be.null();
		await postmaster._channels.topology.checkExchange(postmaster._defaultExchange.name);
		postmaster._connecting = false;
		await Promise.delay(1000);
		try {
			await postmaster._channels.topology.checkExchange(postmaster._defaultExchange.name);
		} catch (err) {
			return shutdownPromise;
		}
		throw new Error('Failed to retry shutdown!');
	}).timeout(5000);

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

// describe('publisher functions:', () => {
// 	let postmaster;
// 	let sandbox;

// 	before(() => {
// 		postmaster = new PostmasterGeneral(uuidv4());
// 		return postmaster.start();
// 	});

// 	beforeEach(() => {
// 		sandbox = sinon.sandbox.create();
// 	});

// 	after(() => {
// 		return postmaster.stop();
// 	});

// 	afterEach(() => {
// 		sandbox.restore();
// 	});

// 	describe('request:', () => {
// 		it('should timeout if not response is sent is true', function (done) {
// 			// Default timeout is 60 seconds, wait for it.
// 			postmaster.timeout(65 * 1000);

// 			// Setup spies
// 			const spyResolveTopic = sandbox.spy(postmaster, 'resolveTopic');
// 			const spyPublish = sandbox.spy(postmaster.rabbit, 'request');

// 			// Publish the message
// 			postmaster.request('role:create', { max: 100, min: 25 })
// 				.then(() => {
// 					done('Should have timed out while waiting on a reply!');
// 				})
// 				.catch(() => {
// 					try {
// 						expect(spyResolveTopic.calledOnce).to.be.ok; // eslint-disable-line no-unused-expressions
// 						expect(spyPublish.called).to.be.ok; // eslint-disable-line no-unused-expressions
// 						done();
// 					} catch (err) {
// 						done(err);
// 					}
// 				});
// 		});
// 	});

// 	describe('publish:', () => {
// 		it('should not wait for a response', (done) => {
// 			// Setup spies
// 			const spyResolveTopic = sandbox.spy(postmaster, 'resolveTopic');
// 			const spyPublish = sandbox.spy(postmaster.rabbit, 'publish');

// 			// Publish the message
// 			postmaster.publish('role:create', { max: 100, min: 25 })
// 				.then(() => {
// 					try {
// 						expect(spyResolveTopic.calledOnce).to.be.ok; // eslint-disable-line no-unused-expressions
// 						expect(spyPublish.called).to.be.ok; // eslint-disable-line no-unused-expressions
// 						done();
// 					} catch (err) {
// 						done(err);
// 					}
// 				})
// 				.catch((err) => {
// 					done(err);
// 				});
// 		});
// 	});
// });

// describe('full stack tests:', () => {
// 	let postmaster;
// 	let sandbox;

// 	beforeEach(() => {
// 		postmaster = new PostmasterGeneral(uuidv4());
// 		sandbox = sinon.sandbox.create();
// 	});

// 	afterEach(() => {
// 		sandbox.restore();
// 		return postmaster.stop();
// 	});

// 	it('should allow removing listeners', () => {
// 		return postmaster.addListener('action:get_greeting:*', (message, cb) => {
// 			return cb(null, { greeting: 'Hello, ' + message.name });
// 		})
// 			.then(() => {
// 				Object.keys(postmaster.listeners).length.should.equal(1);
// 				return postmaster.start();
// 			})
// 			.then(() => postmaster.removeListener('action:get_greeting:*'))
// 			.then(() => {
// 				Object.keys(postmaster.listeners).length.should.equal(0);
// 			});
// 	});

// 	it('should handle rpc', () => {
// 		return postmaster.addListener('action:get_greeting', (message, cb) => {
// 			return cb(null, {
// 				greeting: 'Hello, ' + message.name
// 			});
// 		})
// 			.then(() => postmaster.start())
// 			.then(() => postmaster.request('action:get_greeting', { name: 'Steve' }))
// 			.then((res) => {
// 				expect(res).to.exist();
// 				expect(res.greeting).to.exist();
// 				res.greeting.should.equal('Hello, Steve');
// 			});
// 	});

// 	it('should handle failed healtcheck', () => {
// 		return postmaster.addListener('action:get_greeting', (message, cb) => {
// 			return cb(null, {
// 				greeting: 'Hello, ' + message.name
// 			});
// 		})
// 			.then(() => postmaster.start())
// 			.then(() => {
// 				postmaster.settings.queues[0].name = 'bad queue';
// 			})
// 			.then(() => postmaster.healthcheck())
// 			.then(() => {
// 				throw new Error('Failed to miss invalid healthcheck!');
// 			})
// 			.catch(() => {});
// 	});

// 	it('should handle fire and forget', () => {
// 		return postmaster.addListener('action:get_greeting', (message, cb) => {
// 			return cb(null, {
// 				greeting: 'Hello, ' + message.name
// 			});
// 		})
// 			.then(() => postmaster.start())
// 			.then(() => postmaster.publish('action:get_greeting', { name: 'Steve' }))
// 			.then((res) => {
// 				expect(res).to.not.exist();
// 			});
// 	});

// 	it('should pass $requestId and $trace', () => {
// 		return postmaster.addListener('action:get_greeting', (message, cb) => {
// 			// Check the listener side.
// 			expect(message.$requestId).to.exist();
// 			message.$requestId.should.equal('testId');
// 			expect(message.$trace).to.exist();
// 			message.$trace.should.equal(true);

// 			return cb(null, {
// 				greeting: 'Hello, ' + message.name
// 			});
// 		})
// 			.then(() => postmaster.start())
// 			.then(() => postmaster.request('action:get_greeting', { name: 'Steve' }, { requestId: 'testId', trace: true }))
// 			.then((res) => {
// 				expect(res).to.exist();
// 				expect(res.greeting).to.exist();
// 				res.greeting.should.equal('Hello, Steve');

// 				// Check the callback side
// 				expect(res.$requestId).to.exist();
// 				res.$requestId.should.equal('testId');
// 				expect(res.$trace).to.exist();
// 				res.$trace.should.equal(true);
// 			});
// 	});

// 	it('should allow * matches in listener routes', function () {
// 		// Default timeout is 60 seconds, wait for it.
// 		postmaster.timeout(125 * 1000);

// 		return postmaster.addListener('log:*', (message, cb) => {
// 			return cb(null, {
// 				greeting: 'Hello, ' + message.name
// 			});
// 		})
// 			.then(() => postmaster.start())
// 			// Test match
// 			.then(() => postmaster.request('log:mytest', { name: 'Steve' }))
// 			.then((res) => {
// 				expect(res).to.exist();
// 				expect(res.greeting).to.exist();
// 				res.greeting.should.equal('Hello, Steve');
// 			})
// 			// Test mismatch
// 			.then(() => {
// 				return postmaster.request('log:mytest.another:test', { name: 'Steve' })
// 					.then(() => {
// 						return Promise.reject(new Error('Invalid match!'));
// 					})
// 					.catch(() => {});
// 			});
// 	});

// 	it('should allow # matches in listener routes', () => {
// 		return postmaster.addListener('log:#', (message, cb) => {
// 			return cb(null, {
// 				greeting: 'Hello, ' + message.name
// 			});
// 		})
// 			.then(() => postmaster.start())
// 			.then(() => postmaster.request('log:mytest.anotherthing:test', { name: 'Steve' }))
// 			.then((res) => {
// 				expect(res).to.exist();
// 				expect(res.greeting).to.exist();
// 				res.greeting.should.equal('Hello, Steve');
// 			});
// 	});
// });
