'use strict';

const Promise = require('bluebird');
const {assoc, isNonEmptyString} = require('kixx/library');
const http = require('http');
const https = require('https');
const url = require('url');
const crypto = require('crypto');

const DEFAULT_REQUEST_TIMEOUT = 700;
const DEFAULT_BACKOFF_MULTIPLIER = 100;
const DEFAULT_OPERATION_TIMEOUT = 0;
const DEFAULT_AWS_REGION = 'DEFAULT_AWS_REGION';

const DYNAMODB_API_VERSION = 'DynamoDB_20120810';
const ECONNREFUSED = 'ECONNREFUSED';
const THROUGHPUT_EXCEEDED = 'THROUGHPUT_EXCEEDED';
const OPERATION_TIMEOUT = 'OPERATION_TIMEOUT';
const ProvisionedThroughputExceededException = 'ProvisionedThroughputExceededException';
const ResourceNotFoundException = 'ResourceNotFoundException';


const assignRequestItems = assoc('RequestItems');

function getRequestTimeoutOption(options, def) {
	return Number.isInteger(options.requestTimeout) ? options.requestTimeout : def;
}

function getOperationTimeoutOption(options, def) {
	return Number.isInteger(options.operationTimeout) ? options.operationTimeout : def;
}

function getBackoffMultiplierOption(options, def) {
	return Number.isInteger(options.backoffMultiplier) ? options.backoffMultiplier : def;
}

// options.emitter
// options.awsRegion
// options.awsAccessKey
// options.awsSecretKey
// options.dynamodbEndpoint
// options.operationTimeout
// options.backoffMultiplier
// options.requestTimeout
class DynamoDbClient {
	static get THROUGHPUT_EXCEEDED() {
		return THROUGHPUT_EXCEEDED;
	}

	static get OPERATION_TIMEOUT() {
		return OPERATION_TIMEOUT;
	}

	static get ResourceNotFoundException() {
		return ResourceNotFoundException;
	}

	constructor(options) {
		const {emitter} = options;

		const requestTimeout = getRequestTimeoutOption(options, DEFAULT_REQUEST_TIMEOUT);
		const operationTimeout = getOperationTimeoutOption(options, DEFAULT_OPERATION_TIMEOUT);
		const backoffMultiplier = getBackoffMultiplierOption(options, DEFAULT_BACKOFF_MULTIPLIER);

		const awsRegion = isNonEmptyString(options.awsRegion) ? options.awsRegion : DEFAULT_AWS_REGION;
		const dynamodbEndpoint = isNonEmptyString(options.dynamodbEndpoint) ? options.dynamodbEndpoint : `https://dynamodb.${awsRegion}.amazonaws.com`;

		Object.defineProperties(this, {
			emitter: {
				value: emitter
			},
			requestTimeout: {
				enumerable: true,
				value: requestTimeout
			},
			backoffMultiplier: {
				enumerable: true,
				value: backoffMultiplier
			},
			operationTimeout: {
				enumerable: true,
				value: operationTimeout
			},
			awsRegion: {
				enumerable: true,
				value: awsRegion
			},
			awsAccessKey: {
				value: options.awsAccessKey
			},
			awsSecretKey: {
				value: options.awsSecretKey
			},
			dynamodbEndpoint: {
				enumerable: true,
				value: dynamodbEndpoint
			}
		});
	}

	_mergeOptions(options) {
		const emitter = options.emitter || this.emitter;

		if (!emitter || typeof emitter.emit !== 'function') {
			throw new Error(`expects options.emitter to be an EventEmitter`);
		}

		return Object.freeze({
			emitter,
			requestTimeout: getRequestTimeoutOption(options, this.requestTimeout),
			operationTimeout: getOperationTimeoutOption(options, this.operationTimeout),
			backoffMultiplier: getBackoffMultiplierOption(options, this.backoffMultiplier)
		});
	}

	requestWithBackoff(target, params, options = {}) {
		options = this._mergeOptions(options);
		return this._requestWithBackoff(target, options, params);
	}

	_requestWithBackoff(target, options, params) {
		const {emitter, operationTimeout, backoffMultiplier} = options;
		const backoff = computeBackoffMilliseconds(backoffMultiplier);
		const request = this._request.bind(this, target, options);
		const start = Date.now();
		let emittedThroughputError = false;

		function tryOperation(retryCount) {
			return request(params).catch((err) => {
				if (err.code === ProvisionedThroughputExceededException) {
					const tableMessage = params.IndexName ? `index ${params.IndexName}` : `table ${params.TableName}`;

					if (!emittedThroughputError) {
						emittedThroughputError = true;
						const err = new Error(
							`DynamoDB ProvisionedThroughputExceededException during ${target} on ${tableMessage}`
						);
						err.code = THROUGHPUT_EXCEEDED;
						emitter.emit('warning', err);
					}

					const delay = backoff(retryCount);

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on ${tableMessage}`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}

					return Promise.delay(delay).then(() => tryOperation(retryCount + 1));
				}

				return Promise.reject(err);
			});
		}

		return tryOperation(0);
	}

	batchWriteWithBackoff(target, params, options = {}) {
		options = this._mergeOptions(options);
		return this._batchWriteWithBackoff(target, options, params);
	}

	_batchWriteWithBackoff(target, options, params) {
		const {emitter, operationTimeout, backoffMultiplier} = options;
		const backoff = computeBackoffMilliseconds(backoffMultiplier);
		const request = this._request.bind(this, target, options);
		const start = Date.now();
		let emittedUnprocessedItemsError = false;
		let emittedThroughputError = false;

		const TableName = Object.keys(params.RequestItems)[0];

		function tryOperation(params, retryCount) {
			return request(params).then((res) => {
				const {UnprocessedItems} = res;

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

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on table ${TableName}`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}

					return Promise.delay(delay).then(() => {
						const newParams = assignRequestItems(UnprocessedItems, params);
						return tryOperation(newParams, retryCount += 1);
					});
				}

				return res;
			}).catch((err) => {
				if (err.code === ProvisionedThroughputExceededException) {
					if (!emittedThroughputError) {
						emittedThroughputError = true;
						const err = new Error(
							`DynamoDB ProvisionedThroughputExceededException during ${target} on table ${TableName}`
						);
						err.code = THROUGHPUT_EXCEEDED;
						emitter.emit('warning', err);
					}

					const delay = backoff(retryCount);

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on table ${TableName}`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}

					return Promise.delay(delay).then(() => tryOperation(params, retryCount + 1));
				}

				return Promise.reject(err);
			});
		}

		return tryOperation(params, 0);
	}

	batchGetWithBackoff(target, params, options = {}) {
		options = this._mergeOptions(options);
		return this._batchGetWithBackoff(target, options, params);
	}

	_batchGetWithBackoff(target, options, params) {
		const {emitter, operationTimeout, backoffMultiplier} = options;
		const backoff = computeBackoffMilliseconds(backoffMultiplier);
		const request = this._request.bind(this, target, options);
		const start = Date.now();

		const TableName = Object.keys(params.RequestItems)[0];

		function tryOperation(params, retryCount) {
			return request(params).then((res) => {
				const {UnprocessedKeys} = res;

				if (UnprocessedKeys[TableName]) {
					const err = new Error(
						`DynamoDB UnprocessedKeys during ${target} on table ${TableName}`
					);
					err.code = THROUGHPUT_EXCEEDED;
					emitter.emit('warning', err);

					const delay = backoff(retryCount);

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on table ${TableName}`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}

					return Promise.delay(delay).then(() => {
						const newParams = assignRequestItems(UnprocessedKeys, params);
						return tryOperation(newParams, retryCount += 1);
					});
				}

				return res;
			}).catch((err) => {
				if (err.code === ProvisionedThroughputExceededException) {
					const err = new Error(
						`DynamoDB ProvisionedThroughputExceededException during ${target} on table ${TableName}`
					);
					err.code = THROUGHPUT_EXCEEDED;
					emitter.emit('warning', err);

					const delay = backoff(retryCount);

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on table ${TableName}`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}

					return Promise.delay(delay).then(() => tryOperation(params, retryCount + 1));
				}

				return Promise.reject(err);
			});
		}

		return tryOperation(params, 0);
	}

	request(target, params, options = {}) {
		options = this._mergeOptions(options);
		params = params || {};
		return this._request(target, options, params);
	}

	_request(target, options, params) {
		const timeout = options.requestTimeout;
		const region = this.awsRegion;
		const accessKey = this.awsAccessKey;
		const secretKey = this.awsSecretKey;
		const endpoint = this.dynamodbEndpoint;

		target = `${DYNAMODB_API_VERSION}.${target}`;
		const data = JSON.stringify(params);

		return new Promise((resolve, reject) => {
			const {protocol, hostname, port, method, headers, path} = amzRequestOptions({
				region,
				accessKey,
				secretKey,
				endpoint,
				target
			}, data);

			const params = {
				protocol,
				hostname,
				port,
				method: method || 'POST',
				path,
				headers,
				timeout
			};

			const NS = protocol === 'https:' ? https : http;

			const req = NS.request(params, function bufferHttpServerResponse(res) {
				res.once('error', (err) => {
					return reject(new Error(
						`Error event in DynamoDbClient response: ${err.message}`
					));
				});

				const chunks = [];
				res.on('data', (chunk) => chunks.push(chunk));

				res.on('end', () => {
					const body = JSON.parse(Buffer.concat(chunks).toString());

					if (res.statusCode === 200) return resolve(body);

					const errName = getAwsErrorName(body).split('#').pop();
					const errMessage = getAwsErrorMessage(body) || errName;

					reject(new AwsApiError(errName, errMessage));
				});
			});

			req.once('error', (err) => {
				if (err.code === ECONNREFUSED) {
					return reject(new Error(
						`DynamoDbClient connection refused to ${protocol}//${hostname}:${port}`
					));
				}
				return reject(new Error(
					`Error event in DynamoDbClient request: ${err.message}`
				));
			});

			req.write(data);
			req.end();
		});
	}

	static create(options = {}) {
		if (!options.emitter || typeof options.emitter.emit !== 'function') {
			throw new Error(`expects options.emitter to be an EventEmitter`);
		}

		return new DynamoDbClient(options);
	}
}

module.exports = DynamoDbClient;


class AwsApiError extends Error {
	constructor(name, message) {
		super(message);

		Object.defineProperties(this, {
			name: {
				enumerable: true,
				value: name
			},
			message: {
				enumerable: true,
				value: message
			},
			code: {
				enumerable: true,
				value: name
			}
		});

		if (Error.captureStackTrace) {
			Error.captureStackTrace(this, this.constructor);
		}
	}
}

function computeBackoffMilliseconds(backoffMultiplier) {
	return function (times) {
		return Math.pow(2, times) * backoffMultiplier;
	};
}

function getAwsErrorName(err) {
	return (err || {}).__type || '#UnrecognizedClientException';
}

function getAwsErrorMessage(err) {
	return (err || {}).message;
}

function hmac(key, data) {
	const hmac = crypto.createHmac('sha256', key);
	hmac.update(data);
	return hmac.digest('hex');
}

function sign(key, data) {
	const hmac = crypto.createHmac('sha256', key);
	hmac.update(data);
	return hmac.digest();
}

function hash(data) {
	const hash = crypto.createHash('sha256');
	hash.update(data);
	return hash.digest('hex');
}

function amzSignatureKey(key, datestamp, region, service) {
	const kDate = sign('AWS4' + key, datestamp);
	const kRegion = sign(kDate, region);
	const kService = sign(kRegion, service);
	const kSigning = sign(kService, 'aws4_request');
	return kSigning;
}

function amzRequestOptions(options, payload) {
	const {region, accessKey, secretKey, target} = options;
	const endpoint = url.parse(options.endpoint);

	const t = new Date();
	const parts = t.toISOString().split('.');
	const amzdate = parts[0].replace(/-|:/g, '') + 'Z'; // '20170630T060649Z'
	const datestamp = parts[0].split('T')[0].replace(/-/g, '');

	const payloadHash = hash(payload);
	const signedHeaders = 'host;x-amz-content-sha256;x-amz-date;x-amz-target';
	const scope = `${datestamp}/${region}/dynamodb/aws4_request`;

	const headers = [
		`host:${endpoint.hostname}`,
		`x-amz-content-sha256:${payloadHash}`,
		`x-amz-date:${amzdate}`,
		`x-amz-target:${target}`
	].join('\n');

	const CanonicalString = [
		'POST',
		`${endpoint.path}\n`,
		`${headers}\n`,
		signedHeaders,
		payloadHash
	].join('\n');

	const StringToSign = [
		'AWS4-HMAC-SHA256',
		amzdate,
		scope,
		hash(CanonicalString)
	].join('\n');

	const key = amzSignatureKey(secretKey, datestamp, region, 'dynamodb');
	const signature = hmac(key, StringToSign);

	const Authorization = [
		`AWS4-HMAC-SHA256 Credential=${accessKey}/${scope}`,
		`SignedHeaders=${signedHeaders}`,
		`Signature=${signature}`
	].join(`,`);

	return Object.freeze({
		protocol: endpoint.protocol,
		hostname: endpoint.hostname,
		port: endpoint.port,
		method: 'POST',
		path: endpoint.path,
		headers: Object.freeze({
			'x-amz-date': amzdate,
			'Content-Type': 'application/x-amz-json-1.0',
			'Content-Length': Buffer.byteLength(payload),
			'Authorization': Authorization,
			'x-amz-target': target,
			'x-amz-content-sha256': payloadHash
		})
	});
}
