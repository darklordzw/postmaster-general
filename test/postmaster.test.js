/* eslint import/no-unassigned-import: 'off' */
/* eslint max-nested-callbacks: 'off' */
'use strict';

const chai = require('chai');
const dirtyChai = require('dirty-chai');
const sinon = require('sinon');
require('sinon-bluebird');
const sinonChai = require('sinon-chai');
const postmasterGeneral = require('../postmaster-general');

/* This sets up the Chai assertion library. "should" and "expect"
initialize their respective assertion properties. The "use()" functions
load plugins into Chai. "dirtyChai" just allows assertion properties to
use function call syntax ("calledOnce()" vs "calledOnce"). It makes them more
acceptable to the linter. */
const expect = chai.expect;
const assert = chai.assert;
chai.should();
chai.use(dirtyChai);
chai.use(sinonChai);


describe('utility functions', () => {
	let postmaster;

	before(() => {
		postmaster = new postmasterGeneral.Postmaster();
	});

	describe('resolveCallbackQueue()', () => {
		it('should use default prefix and separator if no options are provided', () => {
			let queue = postmaster.resolveCallbackQueue();

			// queue name should contain default prefix 'postmaster' and separator '.'
			queue.should.contain('postmaster.');
		});

		it('should use custom prefix', () => {
			let options = {
				prefix: 'myprefix'
			};

			let queue = postmaster.resolveCallbackQueue(options);
			queue.should.contain('myprefix.');
		});

		it('should use custom separator', () => {
			let options = {
				separator: '|'
			};

			let queue = postmaster.resolveCallbackQueue(options);
			queue.should.contain('postmaster|');
		});

		it('should use custom prefix and separator', () => {
			let options = {
				prefix: 'myprefix',
				separator: '|'
			};

			let queue = postmaster.resolveCallbackQueue(options);
			queue.should.contain('myprefix|');
		});
	});

	/**
	 * Function: resolveTopic()
	 */
	describe('resolveTopic()', () => {
		it('should use a topic name starting with the action prefix', () => {
			let topic = postmaster.resolveTopic('role:create');
			topic.should.contain('role.');
		});
	});
});

describe('publisher functions', () => {
	let postmaster;
	let sandbox;

	before(() => {
		postmaster = new postmasterGeneral.Postmaster();
		return postmaster.start();
	});

	beforeEach(function () {
		sandbox = sinon.sandbox.create();
	});

	after(() => {
		postmaster.stop();
	});

	afterEach(function () {
		sandbox.restore();
	});

	describe('publish()', () => {
		it('should timeout if not response is sent and replyRequired is true', function (done) {
			// Default timeout is 10 seconds, wait for it.
			this.timeout(15 * 1000);

			// Setup spies
			let spyResolveTopic = sandbox.spy(postmaster, 'resolveTopic');
			let spyPublish = sandbox.spy(postmaster.publisherConn.channel, 'publish');

			// publish the message
			postmaster.publish('role:create', { max: 100, min: 25 }, true)
				.then(() => {
					done('Should have timed out while waiting on a reply!');
				})
				.catch((err) => {
					try {
						spyResolveTopic.should.have.been.calledOnce();
						spyPublish.should.have.been.called();
						expect(err).to.be.an.instanceof(postmasterGeneral.RPCTimeoutError);
						done();
					} catch (err) {
						done(err);
					}
				});
		});

		it('should not wait for a response if replyRequired is not true', function (done) {
			// Setup spies
			let spyResolveTopic = sandbox.spy(postmaster, 'resolveTopic');
			let spyPublish = sandbox.spy(postmaster.publisherConn.channel, 'publish');

			// publish the message
			postmaster.publish('role:create', { max: 100, min: 25 })
				.then(() => {
					try {
						spyResolveTopic.should.have.been.calledOnce();
						spyPublish.should.have.been.called();
						done();
					} catch (err) {
						done(err);
					}
				})
				.catch((err) => {
					done(err);
				});
		});
	});

	//describe('awaitReply()', () => {
	//	it('should await for reply messages from the channel', sinon.test(() => {
	//		// stubs
	//		var spyConsume = sandbox.stub(client.transport.channel, 'consume', function (queue, cb) {
	//			cb(message);
	//		});

	//		var stubHandleResponse = sandbox.stub(client.utils, 'handle_response', () => { });

	//		// wait for reply messages
	//		client.awaitReply();

	//		/*
	//		 * assertions
	//		 */
	//		spyConsume.should.have.been.calledOnce();
	//		stubHandleResponse.should.have.been.calledOnce();
	//	}));
	//});

	//describe('consumeReply()', () => {
	//	it('should ignore messages if correlationId does not match', sinon.test(() => {
	//		// Spies
	//		var spyParseJSON = sandbox.spy(client.utils, 'parseJSON');
	//		var stubHandleResponse = sandbox.stub(client.utils, 'handle_response', Function.prototype);

	//		// Consume the response message
	//		client.consumeReply()({
	//			properties: {
	//				correlationId: 'foo_d8a42bc0-9022-4db5-ab57-121cd21ac295'
	//			}
	//		});

	//		spyParseJSON.should.not.have.been.called();
	//		stubHandleResponse.should.not.have.been.called();
	//	}));

	//	it('should consume reply messages from the channel', sinon.test(() => {
	//		// spies
	//		var spyParseJSON = sandbox.spy(client.utils, 'parseJSON');

	//		var stubHandleResponse = sandbox.stub(client.utils, 'handle_response', () => { });

	//		// consume the response message
	//		client.consumeReply()(message);

	//		/*
	//		 * assertions
	//		 */
	//		spyParseJSON.should.have.been.calledOnce();
	//		stubHandleResponse.should.have.been.calledOnce();
	//	}));

	//	it('should handle reply messages with no content', sinon.test(() => {
	//		// spies
	//		var spyParseJSON = sandbox.spy(client.utils, 'parseJSON');

	//		var stubHandleResponse = sandbox.stub(client.utils, 'handle_response', () => { });

	//		// consume the response message
	//		client.consumeReply()(message);

	//		/*
	//		 * assertions
	//		 */
	//		spyParseJSON.should.have.been.calledOnce();
	//		stubHandleResponse.should.have.been.calledOnce();
	//	}));
	//});

	//describe('callback()', () => {
	//	it('should forward channel messages to consumeReply()', sinon.test(() => {
	//		// stubs
	//		var spyConsume = sandbox.stub(client.transport.channel, 'consume').resolves(message);
	//		var stubSendDone = sandbox.stub();

	//		// consume the response message
	//		client.callback()(null, null, stubSendDone);

	//		/*
	//		 * assertions
	//		 */
	//		spyConsume.should.have.been.calledOnce();
	//	}));
	//});
});

describe('listener tests', function () {
	let postmaster;
	let sandbox;

	before(() => {
		postmaster = new postmasterGeneral.Postmaster();
		return postmaster.start();
	});

	beforeEach(function () {
		sandbox = sinon.sandbox.create();
	});

	after(() => {
		postmaster.stop();
	});

	afterEach(function () {
		sandbox.restore();
	});

	//describe('handleMessage()', function () {
	//	it('should not handle empty messages', Sinon.test(function () {
	//		// stubs
	//		var stubHandleRequest = this.stub(listener.utils, 'handle_request', function (seneca, data, options, cb) {
	//			return cb();
	//		});

	//		// spies
	//		var spyStringifyJSON = this.spy(listener.utils, 'stringifyJSON');
	//		var spySendToQueue = this.spy(transport.channel, 'sendToQueue');
	//		var spyAck = this.spy(transport.channel, 'ack');

	//		// handle the message
	//		listener.handleMessage(message, data);

	//		/*
	//		 * assertions
	//		 */
	//		stubHandleRequest.should.have.been.calledOnce();
	//		spyStringifyJSON.should.have.not.been.called();
	//		spySendToQueue.should.have.not.been.called();
	//		spyAck.should.have.not.been.called();
	//	}));

	//	it('should push messages to reply queue and acknowledge them', Sinon.test(function () {
	//		// stubs
	//		var stubHandleRequest = this.stub(listener.utils, 'handle_request', function (seneca, data, options, cb) {
	//			return cb(data);
	//		});

	//		// spies
	//		var spyStringifyJSON = this.spy(listener.utils, 'stringifyJSON');
	//		var spySendToQueue = this.spy(transport.channel, 'sendToQueue');
	//		var spyAck = this.spy(transport.channel, 'ack');

	//		// handle the message
	//		listener.handleMessage(message, data);

	//		/*
	//		 * assertions
	//		 */
	//		stubHandleRequest.should.have.been.calledOnce();
	//		spyStringifyJSON.should.have.been.calledOnce();
	//		spyStringifyJSON.should.have.been.calledWithExactly(seneca, 'listen-amqp', data);
	//		spySendToQueue.should.have.been.calledOnce();
	//		spySendToQueue.should.have.been.calledWithExactly(message.properties.replyTo, new Buffer(JSON.stringify(data)), {
	//			correlationId: message.properties.correlationId
	//		});
	//		spyAck.should.have.been.calledOnce();
	//		spyAck.should.have.been.calledWithExactly(message);
	//	}));
	//});

	//describe('listen()', function () {
	//	it('should listen and consume messages from the channel', Sinon.test(function () {
	//		// stubs
	//		var spyConsume = this.stub(postmaster.listenerConn.channel, 'consume', function (queue, cb) {
	//			// return the message
	//			return cb(message);
	//		});

	//		listener.listenForMessages(postmaster.listenerConn, message);

	//		spyConsume.should.have.been.calledOnce();
	//	}));
	//});

	//describe('consume()', function () {
	//	it('should not acknowledge messages without content', Sinon.test(function () {
	//		var msg = {
	//			properties: {
	//				replyTo: 'seneca.res.r1FYNSEN'
	//			}
	//		};

	//		var spyNack = this.spy(transport.channel, 'nack');
	//		var spyHandleMessage = this.spy(listener, 'handleMessage');

	//		// consume the message
	//		listener.consume()(msg);

	//		spyNack.should.have.been.calledOnce();
	//		spyHandleMessage.should.not.have.been.called();
	//	}));

	//	it('should not acknowledge messages without a replyTo property', Sinon.test(function () {
	//		var msg = {
	//			properties: {}
	//		};

	//		var spyNack = this.spy(transport.channel, 'nack');
	//		var spyHandleMessage = this.spy(listener, 'handleMessage');

	//		// consume the message
	//		listener.consume()(msg);

	//		spyNack.should.have.been.calledOnce();
	//		spyHandleMessage.should.not.have.been.called();
	//	}));

	//	it('should handle a valid message', Sinon.test(function () {
	//		var spyNack = this.spy(transport.channel, 'nack');
	//		var spyHandleMessage = this.spy(listener, 'handleMessage');
	//		var spyParseJSON = this.spy(listener.utils, 'parseJSON');

	//		// consume the message
	//		listener.consume()(message);

	//		spyNack.should.not.have.been.called();
	//		spyParseJSON.should.have.been.calledOnce();
	//		spyHandleMessage.should.have.been.calledOnce();
	//	}));
	//});
});

//describe('Unit tests for dead-letter module', function () {
//	let channel = {
//		assertQueue: (queue) => Promise.resolve({
//			queue
//		}),
//		assertExchange: (exchange) => Promise.resolve({
//			exchange
//		}),
//		bindQueue: () => Promise.resolve()
//	};

//	before(function () {
//		// Create spies for channel methods
//		Sinon.stub(channel, 'assertQueue', channel.assertQueue);
//		Sinon.stub(channel, 'assertExchange', channel.assertExchange);
//		Sinon.stub(channel, 'bindQueue', channel.bindQueue);
//	});

//	afterEach(function () {
//		// Reset the state of the stub functions
//		channel.assertQueue.reset();
//		channel.assertExchange.reset();
//		channel.bindQueue.reset();
//	});

//	describe('declareDeadLetter()', function () {
//		it('should return a Promise', function () {
//			DeadLetter.declareDeadLetter().should.be.instanceof(Promise);
//		});

//		it('should avoid any declaration if `options.queue` is not present', function (done) {
//			var options = {
//				exchange: {}
//			};
//			DeadLetter.declareDeadLetter(options, channel)
//				.then(() => {
//					Sinon.assert.notCalled(channel.assertQueue);
//					Sinon.assert.notCalled(channel.assertExchange);
//				})
//				.asCallback(done);
//		});

//		it('should avoid any declaration if `options.exchange` is not present', function (done) {
//			var options = {
//				queue: {}
//			};
//			DeadLetter.declareDeadLetter(options, channel)
//				.then(() => {
//					Sinon.assert.notCalled(channel.assertQueue);
//					Sinon.assert.notCalled(channel.assertExchange);
//				})
//				.asCallback(done);
//		});

//		it('should avoid any declaration if `channel` is null', function (done) {
//			DeadLetter.declareDeadLetter(DEFAULT_OPTIONS, null)
//				.then(() => {
//					Sinon.assert.notCalled(channel.assertQueue);
//					Sinon.assert.notCalled(channel.assertExchange);
//				})
//				.asCallback(done);
//		});

//		it('should declare a dead letter exchange', function (done) {
//			DeadLetter.declareDeadLetter(DEFAULT_OPTIONS, channel)
//				.then(() => {
//					var opt = DEFAULT_OPTIONS.exchange;
//					Sinon.assert.calledOnce(channel.assertExchange);
//					Sinon.assert.calledWithExactly(channel.assertExchange, opt.name, opt.type, opt.options);
//				})
//				.asCallback(done);
//		});

//		it('should declare a dead letter queue', function (done) {
//			DeadLetter.declareDeadLetter(DEFAULT_OPTIONS, channel)
//				.then(() => {
//					var opt = DEFAULT_OPTIONS.queue;
//					Sinon.assert.calledOnce(channel.assertQueue);
//					Sinon.assert.calledWithExactly(channel.assertQueue, opt.name, opt.options);
//				})
//				.asCallback(done);
//		});

//		it('should bind dead letter queue and exchange with "#" as routing key', function (done) {
//			DeadLetter.declareDeadLetter(DEFAULT_OPTIONS, channel)
//				.then(() => {
//					Sinon.assert.calledOnce(channel.bindQueue);
//					Sinon.assert.calledWithExactly(channel.bindQueue, DEFAULT_OPTIONS.queue.name, DEFAULT_OPTIONS.exchange.name, '#');
//				})
//				.asCallback(done);
//		});

//		it('should resolve to an object containing `dlq`, `dlx` and `rk` props', function (done) {
//			DeadLetter.declareDeadLetter(DEFAULT_OPTIONS, channel)
//				.then((dl) => {
//					// Match `dlq` property to `options.queue.name`
//					dl.should.have.property('dlq')
//						.and.equal(DEFAULT_OPTIONS.queue.name);
//					// Match `dlx` property to `options.exchange.name`
//					dl.should.have.property('dlx')
//						.and.equal(DEFAULT_OPTIONS.exchange.name);
//					// Match 'rk' property to '#'
//					dl.should.have.property('rk').and.equal('#');
//				})
//				.asCallback(done);
//		});
//	});
//});