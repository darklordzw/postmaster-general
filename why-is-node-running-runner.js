'use strict';

// 1. Create the file: {projectRoot}/test.js
// 2. Install dependencies: npm i glob why-is-node-running
// 3. Run the tests: node --expose-internals test.js

const whyIsNodeRunning = require('why-is-node-running');
const glob = require('glob');
const Mocha = require('mocha');

const mocha = new Mocha();
const testFiles = glob.sync('./test/*test.js');

console.log('testing files: ', testFiles);

testFiles.forEach((file) => mocha.addFile(file));

mocha.run(() => {
	whyIsNodeRunning();
});
