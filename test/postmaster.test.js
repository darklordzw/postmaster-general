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
