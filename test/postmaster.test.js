/* eslint import/no-unassigned-import: 'off' */
/* eslint max-nested-callbacks: 'off' */

const chai = require("chai");
const dirtyChai = require("dirty-chai");
const Promise = require("bluebird");
const sinon = require("sinon");
const { Transport } = require("postmaster-general-core");
const { PostmasterGeneral } = require("..");

/* This sets up the Chai assertion library. "should" and "expect"
initialize their respective assertion properties. The "use()" functions
load plugins into Chai. "dirtyChai" just allows assertion properties to
use function call syntax ("calledOnce()" vs "calledOnce"). It makes them more
acceptable to the linter. */
const { expect } = chai;
chai.should();
chai.use(dirtyChai);

describe("postmaster-general", () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
  });

  afterEach(() => {
    sandbox.restore();
  });

  describe("constructor:", () => {
    it("should properly initialize settings when passed nothing", () => {
      const postmaster = new PostmasterGeneral();
      expect(postmaster.transports).to.exist();
      postmaster.transports.should.be.empty();
    });
    it("should properly initialize settings when passed transports", () => {
      const postmaster = new PostmasterGeneral({
        requestTransport: new Transport(),
        publishTransport: new Transport(),
      });
      expect(postmaster.transports).to.exist();
      Object.keys(postmaster.transports).length.should.equal(2);
    });
    it("should error on invalid input", () => {
      try {
        const postmaster = new PostmasterGeneral({ requestTransport: "invalid" }); // eslint-disable-line no-unused-vars
      } catch (err) {
        return;
      }
      throw new Error("Failed to catch invalid input.");
    });
    it("should properly handle transport disconnect events", (done) => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      postmaster.on("disconnected", () => done());
      postmaster.transports.request.emit("disconnected");
    });
    it("should properly handle transport reconnect events", (done) => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      postmaster.on("reconnected", () => done());
      postmaster.transports.request.emit("reconnected");
    });
    it("should properly handle transport error events", (done) => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      postmaster.on("error", () => done());
      postmaster.transports.request.emit("error");
    });
  });

  describe("handlerTimings:", () => {
    let postmaster;

    beforeEach(() => {
      postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
    });

    it("should return an empty object if there are no timings", () => {
      postmaster.handlerTimings.should.be.empty();
    });
    it("should return proper timings if set", () => {
      postmaster.transports.request.timings.test = 100;
      Object.keys(postmaster.handlerTimings).length.should.equal(1);
      postmaster.handlerTimings.test.should.equal(100);
    });
  });

  describe("connect:", () => {
    it("should reject if no transports were configured", () => {
      const postmaster = new PostmasterGeneral();
      return postmaster
        .connect()
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== "No Transports were configured.") {
            throw err;
          }
        });
    });
    it("should connect all transports", () => {
      const transport1 = new Transport();
      const transport2 = new Transport();
      const spy1 = sandbox.stub(transport1, "connect");
      const spy2 = sandbox.stub(transport2, "connect");

      const postmaster = new PostmasterGeneral({
        publishTransport: transport1,
        requestTransport: transport2,
      });
      return postmaster.connect().then(() => {
        spy1.calledOnce.should.be.true();
        spy2.calledOnce.should.be.true();
      });
    });
  });

  describe("disconnect:", () => {
    it("should not reject if no transports were configured", () => {
      const postmaster = new PostmasterGeneral();
      return postmaster.disconnect();
    });
    it("should disconnect all transports", () => {
      const transport1 = new Transport();
      const transport2 = new Transport();
      const spy1 = sandbox.stub(transport1, "disconnect");
      const spy2 = sandbox.stub(transport2, "disconnect");

      const postmaster = new PostmasterGeneral({
        publishTransport: transport1,
        requestTransport: transport2,
      });
      return postmaster.disconnect().then(() => {
        spy1.calledOnce.should.be.true();
        spy2.calledOnce.should.be.true();
      });
    });
  });

  describe("addRequestListener:", () => {
    it("should reject if routing key is invalid", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .addRequestListener(4444, () => {
          return Promise.resolve();
        })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"routingKey" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if callback is invalid", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .addRequestListener("test")
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"callback" should be a function that returns a Promise.') {
            throw err;
          }
        });
    });
    it("should reject if no request transport is configured", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .addRequestListener("test", () => {
          return Promise.resolve();
        })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (
            err.message !== "Cannot add a new RPC listener, no RPC Transport has been configured."
          ) {
            throw err;
          }
        });
    });
    it("should hook up the listener on the correct transport", () => {
      const transport1 = new Transport();
      const spy1 = sandbox.stub(transport1, "addMessageListener");

      const postmaster = new PostmasterGeneral({ requestTransport: transport1 });
      return postmaster
        .addRequestListener("test", () => {
          return Promise.resolve();
        })
        .then(() => {
          spy1.calledOnce.should.be.true();
        });
    });
  });

  describe("addPublishListener:", () => {
    it("should reject if routing key is invalid", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .addPublishListener(4444, () => {
          return Promise.resolve();
        })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"routingKey" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if callback is invalid", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .addPublishListener("test")
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"callback" should be a function that returns a Promise.') {
            throw err;
          }
        });
    });
    it("should reject if no request transport is configured", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .addPublishListener("test", () => {
          return Promise.resolve();
        })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (
            err.message !==
            "Cannot add a new fire-and-forget listener, no fire-and-forget Transport has been configured."
          ) {
            throw err;
          }
        });
    });
    it("should hook up the listener on the correct transport", () => {
      const transport1 = new Transport();
      const spy1 = sandbox.stub(transport1, "addMessageListener");

      const postmaster = new PostmasterGeneral({ publishTransport: transport1 });
      return postmaster
        .addPublishListener("test", () => {
          return Promise.resolve();
        })
        .then(() => {
          spy1.calledOnce.should.be.true();
        });
    });
  });

  describe("removeRequestListener:", () => {
    it("should reject if routing key is invalid", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .removeRequestListener(4444, () => {
          return Promise.resolve();
        })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"routingKey" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if no request transport is configured", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .removeRequestListener("test", () => {
          return Promise.resolve();
        })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (
            err.message !== "Cannot remove an RPC listener, no RPC Transport has been configured."
          ) {
            throw err;
          }
        });
    });
    it("should remove the listener from the correct transport", () => {
      const transport1 = new Transport();
      const spy1 = sandbox.stub(transport1, "removeMessageListener");

      const postmaster = new PostmasterGeneral({ requestTransport: transport1 });
      return postmaster
        .removeRequestListener("test", () => {
          return Promise.resolve();
        })
        .then(() => {
          spy1.calledOnce.should.be.true();
        });
    });
  });

  describe("removePublishListener:", () => {
    it("should reject if routing key is invalid", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .removePublishListener(4444, () => {
          return Promise.resolve();
        })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"routingKey" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if no request transport is configured", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .removePublishListener("test", () => {
          return Promise.resolve();
        })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (
            err.message !==
            "Cannot remove a fire-and-forget listener, no fire-and-forget Transport has been configured."
          ) {
            throw err;
          }
        });
    });
    it("should remove the listener from the correct transport", () => {
      const transport1 = new Transport();
      const spy1 = sandbox.stub(transport1, "removeMessageListener");

      const postmaster = new PostmasterGeneral({ publishTransport: transport1 });
      return postmaster
        .removePublishListener("test", () => {
          return Promise.resolve();
        })
        .then(() => {
          spy1.calledOnce.should.be.true();
        });
    });
  });

  describe("listen:", () => {
    it("should reject if no transports were configured", () => {
      const postmaster = new PostmasterGeneral();
      return postmaster
        .listen()
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== "No Transports were configured.") {
            throw err;
          }
        });
    });
    it("should listen all listener transports", () => {
      const transport1 = new Transport();
      const transport2 = new Transport();
      const spy1 = sandbox.stub(transport1, "listen");
      const spy2 = sandbox.stub(transport2, "listen");

      const postmaster = new PostmasterGeneral({
        publishTransport: transport1,
        requestTransport: transport2,
      });
      return postmaster.listen().then(() => {
        spy1.calledOnce.should.be.true();
        spy2.calledOnce.should.be.true();
      });
    });
  });

  describe("publish:", () => {
    it("should reject if routing key is invalid", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .publish(4444, { testKey: "testVal" })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"routingKey" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if correlationId is invalid", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .publish("test", { testKey: "testVal" }, { correlationId: 55555 })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"options.correlationId" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if initiator is invalid", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .publish("test", { testKey: "testVal" }, { initiator: 55555 })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"options.initiator" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if no publish transport is configured", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .publish("test", { testKey: "testVal" })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== "Cannot publish message, no publish Transport has been configured.") {
            throw err;
          }
        });
    });
    it("should call publish on the correct transport", () => {
      const transport1 = new Transport();
      const spy1 = sandbox.stub(transport1, "publish");

      const postmaster = new PostmasterGeneral({ publishTransport: transport1 });
      return postmaster.publish("test", { testKey: "testVal" }).then(() => {
        spy1.calledOnce.should.be.true();
      });
    });
  });

  describe("request:", () => {
    it("should reject if routing key is invalid", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .request(4444, { testKey: "testVal" })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"routingKey" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if correlationId is invalid", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .request("test", { testKey: "testVal" }, { correlationId: 55555 })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"options.correlationId" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if initiator is invalid", () => {
      const postmaster = new PostmasterGeneral({ requestTransport: new Transport() });
      return postmaster
        .request("test", { testKey: "testVal" }, { initiator: 55555 })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== '"options.initiator" should be a string.') {
            throw err;
          }
        });
    });
    it("should reject if no request transport is configured", () => {
      const postmaster = new PostmasterGeneral({ publishTransport: new Transport() });
      return postmaster
        .request("test", { testKey: "testVal" })
        .then(() => {
          throw new Error("Failed to catch invalid input.");
        })
        .catch((err) => {
          if (err.message !== "Cannot send request, no request Transport has been configured.") {
            throw err;
          }
        });
    });
    it("should call request on the correct transport", () => {
      const transport1 = new Transport();
      const spy1 = sandbox.stub(transport1, "request");

      const postmaster = new PostmasterGeneral({ requestTransport: transport1 });
      return postmaster.request("test", { testKey: "testVal" }).then(() => {
        spy1.calledOnce.should.be.true();
      });
    });
  });
});
