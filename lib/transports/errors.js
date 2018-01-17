'use strict';

/**
 * Module representing a set of transport errors.
 * @module lib/transports/errors
 */

/**
 * Generated when a function is called that isn't implemented.
 */
class NotImplementedError extends Error {
	constructor(message) {
		super(message);
		this.name = 'NotImplementedError';
		Error.captureStackTrace(this, this.constructor);
	}
}

/**
 * Generated when the transport is disconnected and cannot reconnect.
 */
class TransportDisconnectedError extends Error {
	constructor(message) {
		super(message);
		this.name = 'TransportDisconnectedError';
		Error.captureStackTrace(this, this.constructor);
	}
}

/**
 * Generated when there is an error sending a message.
 */
class RequestError extends Error {
	constructor(message) {
		super(message);
		this.name = 'RequestError';
		Error.captureStackTrace(this, this.constructor);
	}
}

/**
 * Generated when there is an error with the response.
 * Base class for more specific response errors.
 */
class ResponseError extends Error {
	constructor(message, response) {
		super(message);
		this.name = 'ResponseError';
		Error.captureStackTrace(this, this.constructor);
		this.response = response;
	}
}

/**
 * Generated when the sent message was invalid.
 * @see {@link https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400|HTTP 400}
 */
class InvalidMessageError extends ResponseError {
	constructor(message, response) {
		super(message, response);
		this.name = 'InvalidMessageError';
		Error.captureStackTrace(this, this.constructor);
	}
}

/**
 * Generated when the caller is not authenticated to make the request.
 * @see {@link https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/401|HTTP 401}
 */
class UnauthorizedError extends ResponseError {
	constructor(message, response) {
		super(message, response);
		this.name = 'UnauthorizedError';
		Error.captureStackTrace(this, this.constructor);
	}
}

/**
 * Generated when the caller is authenticated but forbidden to make the request.
 * @see {@link https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403|HTTP 403}
 */
class ForbiddenError extends ResponseError {
	constructor(message, response) {
		super(message, response);
		this.name = 'ForbiddenError';
		Error.captureStackTrace(this, this.constructor);
	}
}

/**
 * Generated when the requested resource could not be found.
 * @see {@link https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404|HTTP 404}
 */
class NotFoundError extends ResponseError {
	constructor(message, response) {
		super(message, response);
		this.name = 'NotFoundError';
		Error.captureStackTrace(this, this.constructor);
	}
}

/**
 * Generated when there is a general error while processing the request.
 * @see {@link https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/500|HTTP_500}
 */
class ResponseProcessingError extends ResponseError {
	constructor(message, response) {
		super(message, response);
		this.name = 'ResponseProcessingError';
		Error.captureStackTrace(this, this.constructor);
	}
}

module.exports = {
	NotImplementedError,
	TransportDisconnectedError,
	RequestError,
	ResponseError,
	InvalidMessageError,
	UnauthorizedError,
	ForbiddenError,
	NotFoundError,
	ResponseProcessingError
};
