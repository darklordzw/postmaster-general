'use strict';

const Chai = require('chai');
const DirtyChai = require('dirty-chai');
const Sinon = require('sinon');
const SinonChai = require('sinon-chai');
require('sinon-bluebird');
Chai.should();
Chai.use(SinonChai);
Chai.use(DirtyChai);

const Defaults = require('../defaults');
const PostmasterGeneral = require('../postmaster-general');

// use the default options
const options = Defaults.amqp;
options.pins = 'cmd:test,val:recipent';
options.queue = 'postmaster.test.queue';

var postmaster = null;

describe('Unit tests for postmaster module', function() {
  before(function(done) {
    postmaster = new PostmasterGeneral(options);
    done();
  });

  before(function() {
    postmaster.close();
  });

  describe('constructor()', function() {
    it('should throw if missing "pins" option', Sinon.test(function() {
      Chai.expect(function() {
        PostmasterGeneral({ queue: 'test' });
      }).to.throw;
    }));

    it('should throw if missing "queue" option', Sinon.test(function() {
      Chai.expect(function() {
        PostmasterGeneral({ pins: ['test'] });
      }).to.throw;
    }));

    it('should default to localhost if no url is passed', Sinon.test(function() {
      var localPostmaster = new PostmasterGeneral({ pins: ['test'], queue: 'test' });
      localPostmaster.amqpUrl.should.equal('amqp://localhost');
    }));
  });

  describe('addRecipient()', function() {
    it('should initialize the listener', Sinon.test(function() {
      Chai.should().not.exist(postmaster.listener);
      postmaster.addRecipient('cmd:test,val:recipent', function() {
        return true;
      });
      Chai.should().exist(postmaster.listener);
    }));

    it('should add the pin to the listener', Sinon.test(function() {
      var spyAdd = this.spy(postmaster.listener, 'add');
      postmaster.addRecipient('cmd:test,val:recipent', function() {
        return true;
      });

      spyAdd.should.have.been.calledOnce();
    }));
  });

  describe('send()', function() {
    it('should initialize the publisher', Sinon.test(function() {
      Chai.should().not.exist(postmaster.publisher);
      postmaster.send('cmd:test,val:recipent', { test: 'test' });
      Chai.should().exist(postmaster.publisher);
    }));

    it('should error if no pins were added', Sinon.test(function() {
      postmaster.pins = [];
      Chai.expect(postmaster.send).to.throw();
    }));

    it('should call act on the publisher', Sinon.test(function() {
      var spyAct = this.stub(postmaster.publisher, 'act', function() {});
      postmaster.send('cmd:test,val:recipent', { test: 'test' });
      spyAct.should.have.been.calledOnce();
    }));

    it('should call with a callback if passed', Sinon.test(function() {
      var spyAct = this.stub(postmaster.publisher, 'act', function(pin, data, callback) {
        var x = pin;
        x = data;
        x = callback;
        return x;
      });

      var passedCallback = function() {
        return true;
      };

      postmaster.send('cmd:test,val:recipent', { test: 'test' }, passedCallback);
      spyAct.should.have.been.calledOnce();
      spyAct.should.have.been.calledWithExactly('cmd:test,val:recipent', { test: 'test' }, passedCallback);
    }));

    it('should call without a callback if none passed', Sinon.test(function() {
      var spyAct = this.stub(postmaster.publisher, 'act', function(pin, data, callback) {
        var x = pin;
        x = data;
        x = callback;
        return x;
      });

      var data = {
        fatal$: false,
        skipReply$: true,
        test: 'test'
      };

      postmaster.send('cmd:test,val:recipent', data);
      spyAct.should.have.been.calledOnce();
      spyAct.should.have.been.calledWithExactly('cmd:test,val:recipent', data);
    }));
  });

  describe('listen()', function() {
    it('should error if no responders were added', Sinon.test(function() {
      Chai.expect(postmaster.listen).to.throw();
    }));

    it('should call listen on the listener', Sinon.test(function() {
      postmaster.addRecipient('cmd:test,val:recipent', function() {
        return true;
      });
      var spyListen = this.spy(postmaster.listener, 'listen');
      postmaster.listen();
      spyListen.should.have.been.calledOnce();
    }));
  });

  describe('close()', function() {
    it('should close the listener', Sinon.test(function() {
      postmaster.addRecipient('cmd:test,val:recipent', function() {
        return true;
      });

      var spyClose = this.spy(postmaster.listener, 'close');
      postmaster.close();
      spyClose.should.have.been.calledOnce();
    }));

    it('should close the producer', Sinon.test(function() {
      postmaster.send('cmd:test,val:recipent', function() {
        return true;
      });

      var spyClose = this.spy(postmaster.publisher, 'close');
      postmaster.close();
      spyClose.should.have.been.calledOnce();
    }));
  });
});
