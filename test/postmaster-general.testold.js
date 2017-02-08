'use strict';

const Chai = require('chai');
const DirtyChai = require('dirty-chai');
const Sinon = require('sinon');
const SinonChai = require('sinon-chai');
require('sinon-bluebird'); /* eslint import/no-unassigned-import: 'off' */

Chai.should();
Chai.use(SinonChai);
Chai.use(DirtyChai);

const Defaults = require('../defaults');
const PostmasterGeneral = require('../postmaster-general');

// use the default options
const options = Defaults.amqp;
options.clientPins = 'cmd:test,val:recipent';
options.listenerPins = 'cmd:test,val:recipent';
options.queue = 'postmaster.test.queue';

var postmaster = null;

describe('Unit tests for postmaster module', function () {
	before(function (done) {
		postmaster = new PostmasterGeneral(options);
		postmaster.init(() => {
			done();
		});
	});

	before(function () {
		postmaster.close();
	});

	describe('constructor()', function () {
		it('should throw if missing "queue" option', Sinon.test(function () {
			Chai.expect(function () {
				PostmasterGeneral();
			}).to.throw();
		}));

		it('should default to localhost if no url is passed', Sinon.test(function () {
			var localPostmaster = new PostmasterGeneral({queue: 'test'});
			localPostmaster.amqpUrl.should.equal('amqp://localhost');
		}));
	});

	describe('addRecipient()', function () {
		it('should add the pin to the listener', Sinon.test(function () {
			var spyAdd = this.spy(postmaster.seneca, 'add');
			postmaster.addRecipient('cmd:test,val:recipent', function () {
				return true;
			});

			spyAdd.should.have.been.calledOnce();
		}));
	});

	describe('send()', function () {
		it('should error if no pins were added', Sinon.test(function () {
			postmaster.clientPins = [];
			Chai.expect(postmaster.send).to.throw();
		}));

		it('should call act on the publisher', Sinon.test(function () {
			var spyAct = this.stub(postmaster.publisher, 'act', function () {});
			postmaster.send('cmd:test,val:recipent', {test: 'test'});
			spyAct.should.have.been.calledOnce();
		}));

		it('should call with a callback if passed', Sinon.test(function () {
			var spyAct = this.stub(postmaster.publisher, 'act', function (pin, data, callback) {
				var x = pin;
				x = data;
				x = callback;
				return x;
			});

			var passedCallback = function () {
				return true;
			};

			postmaster.send('cmd:test,val:recipent', {test: 'test'}, passedCallback);
			spyAct.should.have.been.calledOnce();
		}));

		it('should call without a callback if none passed', Sinon.test(function () {
			var spyAct = this.stub(postmaster.publisher, 'act', function (pin, data, callback) {
				var x = pin;
				x = data;
				x = callback;
				return x;
			});

			var data = {
				default$: {},
				skipReply$: true,
				test: 'test'
			};

			postmaster.send('cmd:test,val:recipent', data);
			spyAct.should.have.been.calledOnce();
			spyAct.should.have.been.calledWithExactly('cmd:test,val:recipent', data);
		}));
	});

	describe('listen()', function () {
		it('should error if no responders were added', Sinon.test(function () {
			Chai.expect(postmaster.listen).to.throw();
		}));

		it('should call listen on the listener', Sinon.test(function () {
			postmaster.addRecipient('cmd:test,val:recipent', function () {
				return true;
			});
			var spyListen = this.spy(postmaster.seneca, 'listen');
			postmaster.listen();
			spyListen.should.have.been.calledOnce();
		}));
	});

	describe('close()', function () {
		it('should close the listener', Sinon.test(function () {
			postmaster.addRecipient('cmd:test,val:recipent', function () {
				return true;
			});

			var spyClose = this.spy(postmaster.seneca, 'close');
			postmaster.close();
			spyClose.should.have.been.calledOnce();
		}));

		it('should close the producer', Sinon.test(function () {
			postmaster.send('cmd:test,val:recipent', function () {
				return true;
			});

			var spyClose = this.spy(postmaster.publisher, 'close');
			postmaster.close();
			spyClose.should.have.been.calledOnce();
		}));
	});
});