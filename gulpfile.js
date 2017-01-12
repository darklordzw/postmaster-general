'use strict';

const gulp = require('gulp');

/* The "gulp-test-rigging" module injects a set of useful gulp tasks
 for running Mocha tests and XO code linting. It requires a gulp
 instance as a parameter, and optionally accepts a second paramter to
 set options. See https://github.com/darklordzw/gulp-test-rigging#readme */
require('gulp-test-rigging')(gulp, {
	paths: {
		src: ['postmaster-general.js', 'lib/**/*.js', 'examples/**/*.js']
	}
});

/* This defines a "default" task to run if the gulp command is run without
 parameters. In this case, the "validate" task from "gulp-test-rigging". */
gulp.task('default', ['validate']);
