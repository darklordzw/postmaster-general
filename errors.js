'use strict';

/**
 * A collection of errors
 * @module errors
 */

module.exports = {
	/**
	 * Class of error indicating an error in acknowledging the message.
	 */
	MessageACKError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	},

	/**
	 * Class of error indicating an error in rejecting the message.
	 */
	MessageNACKError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	},

	/**
	 * Class of error indicating that the message currently being processed should be retried.
	 */
	RetryableMessageHandlerError: class extends Error {
		constructor(message) {
			super(message);
			Error.captureStackTrace(this, this.constructor);
			this.name = this.constructor.name;
		}
	}
};
