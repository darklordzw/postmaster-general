/* eslint import/no-unassigned-import: 'off' */
/* eslint max-nested-callbacks: 'off' */
'use strict';

const chai = require('chai');
const dirtyChai = require('dirty-chai');
const sinon = require('sinon');
require('sinon-bluebird');
const sinonChai = require('sinon-chai');
const uuid = require('uuid');
const PostmasterGeneral = require('../postmaster-general');

/* This sets up the Chai assertion library. "should" and "expect"
initialize their respective assertion properties. The "use()" functions
load plugins into Chai. "dirtyChai" just allows assertion properties to
use function call syntax ("calledOnce()" vs "calledOnce"). It makes them more
acceptable to the linter. */
const expect = chai.expect;
chai.should();
chai.use(dirtyChai);
chai.use(sinonChai);

describe('utility functions:', function () {
	let postmaster;

	before(function () {
		postmaster = new PostmasterGeneral(uuid.v4());
	});

	/**
	 * Function: resolveTopic()
	 */
	describe('resolveTopic:', function () {
		it('should use a topic name starting with the action prefix', function () {
			let topic = postmaster.resolveTopic('role:create');
			topic.should.contain('role.');
		});
	});
});

describe('publisher functions:', function () {
	let postmaster;
	let sandbox;

	before(function () {
		postmaster = new PostmasterGeneral(uuid.v4());
		return postmaster.start();
	});

	beforeEach(function () {
		sandbox = sinon.sandbox.create();
	});

	after(function () {
		return postmaster.stop();
	});

	afterEach(function () {
		sandbox.restore();
	});

	describe('request:', function () {
		it('should timeout if not response is sent is true', function (done) {
			// Default timeout is 60 seconds, wait for it.
			this.timeout(65 * 1000);

			// Setup spies
			let spyResolveTopic = sandbox.spy(postmaster, 'resolveTopic');
			let spyPublish = sandbox.spy(postmaster.rabbit, 'request');

			// publish the message
			postmaster.request('role:create', {max: 100, min: 25})
				.then(() => {
					done('Should have timed out while waiting on a reply!');
				})
				.catch(() => {
					try {
						spyResolveTopic.should.have.been.calledOnce();
						spyPublish.should.have.been.called();
						done();
					} catch (err) {
						done(err);
					}
				});
		});
	});

	describe('publish:', function () {
		it('should not wait for a response', function (done) {
			// Setup spies
			let spyResolveTopic = sandbox.spy(postmaster, 'resolveTopic');
			let spyPublish = sandbox.spy(postmaster.rabbit, 'publish');

			// publish the message
			postmaster.publish('role:create', {max: 100, min: 25})
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
});

describe('full stack tests:', function () {
	let postmaster;
	let sandbox;

	beforeEach(function () {
		postmaster = new PostmasterGeneral(uuid.v4());
		sandbox = sinon.sandbox.create();
	});

	afterEach(function () {
		sandbox.restore();
		return postmaster.stop();
	});

	it('should allow removing listeners', function () {
		return postmaster.addListener('action:get_greeting:*', function (message, cb) {
			return cb(null, {greeting: 'Hello, ' + message.name});
		})
			.then(() => {
				Object.keys(postmaster.listeners).length.should.equal(1);
				return postmaster.start();
			})
			.then(() => postmaster.removeListener('action:get_greeting:*'))
			.then(() => {
				Object.keys(postmaster.listeners).length.should.equal(0);
			});
	});

	it('should handle rpc', function () {
		return postmaster.addListener('action:get_greeting', function (message, cb) {
			return cb(null, {
				greeting: 'Hello, ' + message.name
			});
		})
			.then(() => postmaster.start())
			.then(() => postmaster.request('action:get_greeting', {name: 'Steve'}))
			.then((res) => {
				expect(res).to.exist();
				expect(res.greeting).to.exist();
				res.greeting.should.equal('Hello, Steve');
			});
	});

	it('should handle clean healtcheck', function () {
		return postmaster.addListener('action:get_greeting', function (message, cb) {
			return cb(null, {
				greeting: 'Hello, ' + message.name
			});
		})
			.then(() => postmaster.start())
			.then(() => postmaster.healthcheck())
			.then((res) => {
				expect(res).to.exist();
				expect(res).to.be.true();
			});
	});

	it('should handle failed healtcheck', function () {
		return postmaster.addListener('action:get_greeting', function (message, cb) {
			return cb(null, {
				greeting: 'Hello, ' + message.name
			});
		})
			.then(() => postmaster.start())
			.then(() => {
				postmaster.settings.queues[0].name = 'bad queue';
			})
			.then(() => postmaster.healthcheck())
			.then(() => {
				throw new Error('Failed to miss invalid healthcheck!');
			})
			.catch(() => {});
	});

	it('should handle fire and forget', function () {
		return postmaster.addListener('action:get_greeting', function (message, cb) {
			return cb(null, {
				greeting: 'Hello, ' + message.name
			});
		})
			.then(() => postmaster.start())
			.then(() => postmaster.publish('action:get_greeting', {name: 'Steve'}))
			.then((res) => {
				expect(res).to.not.exist();
			});
	});

	it('should pass $requestId and $trace', function () {
		return postmaster.addListener('action:get_greeting', function (message, cb) {
			// Check the listener side.
			expect(message.$requestId).to.exist();
			message.$requestId.should.equal('testId');
			expect(message.$trace).to.exist();
			message.$trace.should.equal(true);

			return cb(null, {
				greeting: 'Hello, ' + message.name
			});
		})
			.then(() => postmaster.start())
			.then(() => postmaster.request('action:get_greeting', {name: 'Steve'}, {requestId: 'testId', trace: true}))
			.then((res) => {
				expect(res).to.exist();
				expect(res.greeting).to.exist();
				res.greeting.should.equal('Hello, Steve');

				// Check the callback side
				expect(res.$requestId).to.exist();
				res.$requestId.should.equal('testId');
				expect(res.$trace).to.exist();
				res.$trace.should.equal(true);
			});
	});

	it('should allow * matches in listener routes', function () {
		// Default timeout is 60 seconds, wait for it.
		this.timeout(125 * 1000);

		return postmaster.addListener('log:*', function (message, cb) {
			return cb(null, {
				greeting: 'Hello, ' + message.name
			});
		})
			.then(() => postmaster.start())
			// Test match
			.then(() => postmaster.request('log:mytest', {name: 'Steve'}))
			.then((res) => {
				expect(res).to.exist();
				expect(res.greeting).to.exist();
				res.greeting.should.equal('Hello, Steve');
			})
			// Test mismatch
			.then(() => {
				return postmaster.request('log:mytest.another:test', {name: 'Steve'})
					.then(() => {
						return Promise.reject('Invalid match!');
					})
					.catch(() => {});
			});
	});

	it('should allow # matches in listener routes', function () {
		return postmaster.addListener('log:#', function (message, cb) {
			return cb(null, {
				greeting: 'Hello, ' + message.name
			});
		})
			.then(() => postmaster.start())
			.then(() => postmaster.request('log:mytest.anotherthing:test', {name: 'Steve'}))
			.then((res) => {
				expect(res).to.exist();
				expect(res.greeting).to.exist();
				res.greeting.should.equal('Hello, Steve');
			});
	});
});
