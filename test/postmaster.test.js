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
