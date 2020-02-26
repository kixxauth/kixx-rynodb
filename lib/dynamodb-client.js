'use strict';

const EventEmitter = require('events');
const http = require('http');
const https = require('https');
const crypto = require('crypto');
const { assert, helpers } = require('kixx-assert');
const { delayPromise } = require('./utils');

const { isNonEmptyString, isFunction } = helpers;

// Used for internal string interpolation.
const DYNAMODB_API_VERSION = 'DynamoDB_20120810';

// Used for internal comparison.
const ECONNREFUSED = 'ECONNREFUSED';

const DEFAULT_BACKOFF_MULTIPLIER = 100;
const DEFAULT_OPERATION_TIMEOUT = 0;

// Error codes for external detection.
const THROUGHPUT_EXCEEDED = 'THROUGHPUT_EXCEEDED';
const OPERATION_TIMEOUT = 'OPERATION_TIMEOUT';
// AWS Error code for external detection.
const NOT_FOUND = 'ResourceNotFoundException';

// AWS Error code for internal comparison.
const ProvisionedThroughputExceededException = 'ProvisionedThroughputExceededException';

exports.ErrorCodes = Object.freeze({
	THROUGHPUT_EXCEEDED,
	OPERATION_TIMEOUT,
	NOT_FOUND,
});


class AwsApiError extends Error {
	constructor(name, message) {
		super(message);

		Object.defineProperties(this, {
			name: {
				enumerable: true,
				value: 'AwsApiError',
			},
			message: {
				enumerable: true,
				value: message,
			},
			code: {
				enumerable: true,
				value: name,
			},
		});

		Error.captureStackTrace(this, this.constructor);
	}
}
exports.AwsApiError = AwsApiError;


exports.create = (thisOptions, thisHttpOptions) => {
	const emitter = (thisOptions.emitter && isFunction(thisOptions.emitter.emit))
		? thisOptions.emitter
		: new EventEmitter();

	assert.isNonEmptyString(thisOptions.awsRegion, 'DynamoDbClient options.awsRegion');
	assert.isNonEmptyString(thisOptions.awsAccessKey, 'DynamoDbClient options.awsAccessKey');
	assert.isNonEmptyString(thisOptions.awsSecretKey, 'DynamoDbClient options.awsSecretKey');

	const { awsRegion, awsAccessKey, awsSecretKey } = thisOptions;

	const endpointUrl = isNonEmptyString(thisOptions.dynamodbEndpoint)
		? thisOptions.dynamodbEndpoint
		: `https://dynamodb.${awsRegion}.amazonaws.com`;

	let dynamodbEndpoint;
	try {
		dynamodbEndpoint = new URL(endpointUrl);
	}
	catch (err) {
		throw new Error(
			`The DynamoDB endpoint URL ${endpointUrl} is invalid: ${err.message}`
		);
	}

	const thisOperationTimeoutMs = Number.isInteger(thisOptions.operationTimeoutMs)
		? thisOptions.operationTimeoutMs
		: DEFAULT_OPERATION_TIMEOUT;

	const thisBackoffMultiplier = Number.isInteger(thisOptions.backoffMultiplier)
		? thisOptions.backoffMultiplier
		: DEFAULT_BACKOFF_MULTIPLIER;

	const thisHttpHeaders = thisHttpOptions ? (thisHttpOptions.headers || {}) : {};

	function mergeOptions(opts) {
		opts = opts || {};
		return Object.freeze({
			operationTimeoutMs: Number.isInteger(opts.operationTimeoutMs) ? opts.operationTimeoutMs : thisOperationTimeoutMs,
			backoffMultiplier: Number.isInteger(opts.backoffMultiplier) ? opts.backoffMultiplier : thisBackoffMultiplier,
		});
	}

	function mergeHttpOptions(opts) {
		opts = Object.assign({}, thisHttpOptions, opts);
		opts.headers = Object.freeze(Object.assign({}, thisHttpHeaders, opts.headers));
		return Object.freeze(opts);
	}

	function computeBackoffMilliseconds(backoffMultiplier) {
		return function (times) {
			return Math.pow(2, times) * backoffMultiplier;
		};
	}

	function getAwsErrorName(err) {
		// eslint-disable-next-line no-underscore-dangle
		return (err || {}).__type || '#UnrecognizedClientException';
	}

	function getAwsErrorMessage(err) {
		return (err || {}).message;
	}

	function hmac(key, data) {
		const buff = crypto.createHmac('sha256', key);
		buff.update(data);
		return buff.digest('hex');
	}

	function hash(data) {
		const buff = crypto.createHash('sha256');
		buff.update(data);
		return buff.digest('hex');
	}

	function amzSignatureKey(key, datestamp, region, service) {
		const kDate = sign('AWS4' + key, datestamp);
		const kRegion = sign(kDate, region);
		const kService = sign(kRegion, service);
		const kSigning = sign(kService, 'aws4_request');
		return kSigning;
	}

	function sign(key, data) {
		const buff = crypto.createHmac('sha256', key);
		buff.update(data);
		return buff.digest();
	}

	function createAmzRequestHeaders(target, payload) {
		const t = new Date();
		const parts = t.toISOString().split('.');
		const amzdate = parts[0].replace(/-|:/g, '') + 'Z'; // '20170630T060649Z'
		const datestamp = parts[0].split('T')[0].replace(/-/g, '');

		const payloadHash = hash(payload);
		const signedHeaders = 'host;x-amz-content-sha256;x-amz-date;x-amz-target';
		const scope = `${datestamp}/${awsRegion}/dynamodb/aws4_request`;

		const headers = [
			`host:${dynamodbEndpoint.host}`,
			`x-amz-content-sha256:${payloadHash}`,
			`x-amz-date:${amzdate}`,
			`x-amz-target:${target}`,
		].join('\n');

		const canonicalString = [
			'POST',
			`${dynamodbEndpoint.pathname}\n`,
			`${headers}\n`,
			signedHeaders,
			payloadHash,
		].join('\n');

		const stringToSign = [
			'AWS4-HMAC-SHA256',
			amzdate,
			scope,
			hash(canonicalString),
		].join('\n');

		const key = amzSignatureKey(awsSecretKey, datestamp, awsRegion, 'dynamodb');
		const signature = hmac(key, stringToSign);

		const authorization = [
			`AWS4-HMAC-SHA256 Credential=${awsAccessKey}/${scope}`,
			`SignedHeaders=${signedHeaders}`,
			`Signature=${signature}`,
		].join(',');

		return Object.freeze({
			'x-amz-date': amzdate,
			'Content-Type': 'application/x-amz-json-1.0',
			'Content-Length': Buffer.byteLength(payload),
			'Authorization': authorization,
			'x-amz-target': target,
			'x-amz-content-sha256': payloadHash,
		});
	}

	// - httpOptions Options Object passed into Node.js http.request(). Will be ovewritten with AWS
	//               request headers and the endpoint URL object.
	// - target      DynamoDB target (method) String.
	// - params      Parameters object to pass as the request body. Will be JSON stringified.
	function request(httpOptions, target, params) {
		return new Promise((resolve, reject) => {
			target = `${DYNAMODB_API_VERSION}.${target}`;
			const data = JSON.stringify(params);

			const amzHeaders = createAmzRequestHeaders(target, data);

			const requestOptions = Object.assign({}, httpOptions, dynamodbEndpoint, {
				method: 'POST',
				headers: Object.assign({}, httpOptions.headers, amzHeaders),
			});

			const NS = dynamodbEndpoint.protocol === 'https:' ? https : http;

			const req = NS.request(requestOptions, function bufferHttpServerResponse(res) {
				res.once('error', (err) => {
					reject(new Error(
						`Error event in DynamoDbClient response: ${err.message}`
					));
				});

				const chunks = [];
				res.on('data', (chunk) => {
					chunks.push(chunk);
				});

				res.on('end', () => {
					let body;
					try {
						body = JSON.parse(Buffer.concat(chunks).toString());
					}
					catch (err) {
						return reject(new Error(
							`JSON parsing error during DynamoDbClient response parsing: ${err.message}`
						));
					}

					if (res.statusCode === 200) {
						resolve(body);
					}
					else {
						const errName = getAwsErrorName(body).split('#').pop();
						const errMessage = getAwsErrorMessage(body) || errName;

						reject(new AwsApiError(errName, errMessage));
					}
				});
			});

			req.once('error', (err) => {
				if (err.code === ECONNREFUSED) {
					reject(new Error(
						`DynamoDbClient connection refused to ${this.thisDynamoDbEndpoint.origin}`
					));
				}
				else {
					reject(new Error(
						`Error event in DynamoDbClient request: ${err.message}`
					));
				}
			});

			req.write(data);
			req.end();
		});
	}

	function requestWithBackoff(options, httpOptions, target, params) {
		const { operationTimeoutMs, backoffMultiplier } = options;
		const backoff = computeBackoffMilliseconds(backoffMultiplier);
		const start = Date.now();
		let emittedThroughputError = false;

		function tryOperation(retryCount) {
			return request(httpOptions, target, params).catch((err) => {
				retryCount = retryCount + 1;

				if (err.code === ProvisionedThroughputExceededException) {
					const tableMessage = params.IndexName ? `index ${params.IndexName}` : `table ${params.TableName}`;

					// We only want to emit the throughput error on the event emitter once, otherwise
					// it will get spammy.
					if (!emittedThroughputError) {
						emittedThroughputError = true;
						const error = new Error(
							`DynamoDB ProvisionedThroughputExceededException during ${target} on ${tableMessage}`
						);
						error.code = THROUGHPUT_EXCEEDED;
						emitter.emit('warning', error);
					}

					const delay = backoff(retryCount);

					if (operationTimeoutMs && (Date.now() + delay - start) >= operationTimeoutMs) {
						const error = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on ${tableMessage}`
						);
						error.code = OPERATION_TIMEOUT;
						return Promise.reject(error);
					}

					return delayPromise(delay).then(() => {
						return tryOperation(retryCount);
					});
				}

				return Promise.reject(err);
			});
		}

		return tryOperation(0);
	}

	function batchGetWithBackoff(options, httpOptions, target, params) {
		const { operationTimeoutMs, backoffMultiplier } = options;
		const backoff = computeBackoffMilliseconds(backoffMultiplier);
		const start = Date.now();
		let emittedUnprocessedKeysError = false;
		let emittedThroughputError = false;

		// We can safely assume that we are only writing to a single table.
		const [ TableName ] = Object.keys(params.RequestItems);

		function tryOperation(thisParams, retryCount) {
			return request(httpOptions, target, thisParams).then((res) => {
				retryCount = retryCount + 1;

				const { UnprocessedKeys } = res;

				if (UnprocessedKeys[TableName]) {
					if (!emittedUnprocessedKeysError) {
						emittedUnprocessedKeysError = true;
						const error = new Error(
							`DynamoDB UnprocessedKeys during ${target} on table ${TableName}`
						);
						error.code = THROUGHPUT_EXCEEDED;
						emitter.emit('warning', error);
					}

					const delay = backoff(retryCount);

					if (operationTimeoutMs && (Date.now() + delay - start) >= operationTimeoutMs) {
						const error = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on table ${TableName}`
						);
						error.code = OPERATION_TIMEOUT;
						return Promise.reject(error);
					}

					return Promise.delay(delay).then(() => {
						const newParams = Object.assign({}, thisParams, { UnprocessedKeys });
						return tryOperation(newParams, retryCount);
					});
				}

				return res;
			}).catch((err) => {
				retryCount = retryCount + 1;

				if (err.code === ProvisionedThroughputExceededException) {
					// We only want to emit the throughput error on the event emitter once, otherwise
					// it will get spammy.
					if (!emittedThroughputError) {
						emittedThroughputError = true;
						const error = new Error(
							`DynamoDB ProvisionedThroughputExceededException during ${target} on ${TableName}`
						);
						error.code = THROUGHPUT_EXCEEDED;
						emitter.emit('warning', error);
					}

					const delay = backoff(retryCount);

					if (operationTimeoutMs && (Date.now() + delay - start) >= operationTimeoutMs) {
						const error = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on ${TableName}`
						);
						error.code = OPERATION_TIMEOUT;
						return Promise.reject(error);
					}

					return delayPromise(delay).then(() => {
						return tryOperation(thisParams, retryCount);
					});
				}

				return Promise.reject(err);
			});
		}

		return tryOperation(params, 0);
	}

	function batchWriteWithBackoff(options, httpOptions, target, params) {
		const { operationTimeoutMs, backoffMultiplier } = options;
		const backoff = computeBackoffMilliseconds(backoffMultiplier);
		const start = Date.now();
		let emittedUnprocessedItemsError = false;
		let emittedThroughputError = false;

		// We can safely assume that we are only writing to a single table.
		const [ TableName ] = Object.keys(params.RequestItems);

		function tryOperation(thisParams, retryCount) {
			return request(httpOptions, target, thisParams).then((res) => {
				retryCount = retryCount + 1;

				const { UnprocessedItems } = res;

				if (UnprocessedItems[TableName]) {
					if (!emittedUnprocessedItemsError) {
						emittedUnprocessedItemsError = true;
						const err = new Error(
							`DynamoDB UnprocessedItems during ${target} on table ${TableName}`
						);
						err.code = THROUGHPUT_EXCEEDED;
						emitter.emit('warning', err);
					}

					const delay = backoff(retryCount);

					if (operationTimeoutMs && (Date.now() + delay - start) >= operationTimeoutMs) {
						const err = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on table ${TableName}`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}

					return Promise.delay(delay).then(() => {
						const newParams = Object.assign({}, thisParams, { UnprocessedItems });
						return tryOperation(newParams, retryCount);
					});
				}

				return res;
			}).catch((err) => {
				retryCount = retryCount + 1;

				if (err.code === ProvisionedThroughputExceededException) {
					// We only want to emit the throughput error on the event emitter once, otherwise
					// it will get spammy.
					if (!emittedThroughputError) {
						emittedThroughputError = true;
						const error = new Error(
							`DynamoDB ProvisionedThroughputExceededException during ${target} on ${TableName}`
						);
						error.code = THROUGHPUT_EXCEEDED;
						emitter.emit('warning', error);
					}

					const delay = backoff(retryCount);

					if (operationTimeoutMs && (Date.now() + delay - start) >= operationTimeoutMs) {
						const error = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on ${TableName}`
						);
						error.code = OPERATION_TIMEOUT;
						return Promise.reject(error);
					}

					return delayPromise(delay).then(() => {
						return tryOperation(thisParams, retryCount);
					});
				}

				return Promise.reject(err);
			});
		}

		return tryOperation(params, 0);
	}

	return {
		emitter,

		requestWithBackoff(target, params, options, httpOptions) {
			options = mergeOptions(options);
			httpOptions = mergeHttpOptions(httpOptions);
			return requestWithBackoff(options, httpOptions, target, params);
		},

		batchGetWithBackoff(target, params, options, httpOptions) {
			options = mergeOptions(options);
			httpOptions = mergeHttpOptions(httpOptions);
			return batchGetWithBackoff(options, httpOptions, target, params);
		},

		batchWriteWithBackoff(target, params, options, httpOptions) {
			options = mergeOptions(options);
			httpOptions = mergeHttpOptions(httpOptions);
			return batchWriteWithBackoff(options, httpOptions, target, params);
		},
	};
};
