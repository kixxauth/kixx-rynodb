
function getRequestTimeoutOption(options, def) {
	return Number.isInteger(options.requestTimeout) ? options.requestTimeout : def;
}

function getOperationTimeoutOption(options, def) {
	return Number.isInteger(options.operationTimeout) ? options.operationTimeout : def;
}

function getBackoffMultiplierOption(options, def) {
	return Number.isInteger(options.backoffMultiplier) ? options.backoffMultiplier : def;
}

function getEmitterOption(options, def) {
	return options.emitter ? options.emitter : def;
}

exports.THROUGHPUT_EXCEEDED = THROUGHPUT_EXCEEDED;
exports.OPERATION_TIMEOUT = OPERATION_TIMEOUT;
exports.ResourceNotFoundException = ResourceNotFoundException;

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


exports.create = (options) => {
	const thisEmitter = options.emitter;
	const thisRequestTimeout = getRequestTimeoutOption(options, DEFAULT_REQUEST_TIMEOUT);
	const thisOperationTimeout = getOperationTimeoutOption(options, DEFAULT_OPERATION_TIMEOUT);
	const thisBackoffMultiplier = getBackoffMultiplierOption(options, DEFAULT_BACKOFF_MULTIPLIER);

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

	function amzRequestOptions(target, payload) {
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

		const canonicalString = [
			'POST',
			`${endpoint.path}\n`,
			`${headers}\n`,
			signedHeaders,
			payloadHash
		].join('\n');

		const stringToSign = [
			'AWS4-HMAC-SHA256',
			amzdate,
			scope,
			hash(canonicalString)
		].join('\n');

		const key = amzSignatureKey(secretKey, datestamp, region, 'dynamodb');
		const signature = hmac(key, stringToSign);

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
				'x-amz-content-sha256': payloadHash,
			}),
		});
	}

	function mergeOptions(options) {
		return Object.freeze({
			emitter: getEmitterOption(options, thisEmitter),
			requestTimeout: getRequestTimeoutOption(options, thisRequestTimeout),
			operationTimeout: getOperationTimeoutOption(options, thisOperationTimeout),
			backoffMultiplier: getBackoffMultiplierOption(options, thisBackoffMultiplier)
		});
	}

	function request(options, target, params) {
		return new Promise((resolve, reject) => {
			const fullTarget = `${DYNAMODB_API_VERSION}.${target}`;
			const data = JSON.stringify(params);

			const {
				protocol,
				hostname,
				port,
				method,
				headers,
				path,
			} = amzRequestOptions(fullTarget, data);

			const params = {
				protocol,
				hostname,
				port,
				method,
				path,
				headers,
				timeout: requestTimeout,
			};

			const NS = protocol === 'https:' ? https : http;

			const req = NS.request(params, function bufferHttpServerResponse(res) {
				res.once('error', (err) => {
					reject(new Error(
						`Error event in DynamoDbClient response: ${err.message}`
					));
				});

				const chunks = [];
				res.on('data', (chunk) => chunks.push(chunk));

				res.on('end', () => {
					let body;
					try {
						body = JSON.parse(Buffer.concat(chunks).toString());
					} catch (err) {
						return reject(new Error(
							`JSON parsing error during DynamoDbClient response parsing: ${err.message}`
						));
					}

					if (res.statusCode === 200) {
						resolve(body);
					} else {
						const errName = getAwsErrorName(body).split('#').pop();
						const errMessage = getAwsErrorMessage(body) || errName;

						reject(new AwsApiError(errName, errMessage));
					}
				});
			});

			req.once('error', (err) => {
				if (err.code === ECONNREFUSED) {
					reject(new Error(
						`DynamoDbClient connection refused to ${protocol}//${hostname}:${port}`
					));
				} else {
					reject(new Error(
						`Error event in DynamoDbClient request: ${err.message}`
					));
				}
			});

			req.write(data);
			req.end();
		});
	}

	function requestWithBackoff(options, target, params) {
		const { emitter, operationTimeout, backoffMultiplier } = options;
		const backoff = computeBackoffMilliseconds(backoffMultiplier);
		const start = Date.now();
		let emittedThroughputError = false;

		function tryOperation(retryCount) {
			return request(options, target, params).catch((err) => {
				if (err.code === ProvisionedThroughputExceededException) {
					const tableMessage = params.IndexName ? `index ${params.IndexName}` : `table ${params.TableName}`;

					// We only want to emit the throughput error on the event emitter once, otherwise it will get spammy.
					if (!emittedThroughputError) {
						emittedThroughputError = true;
						const err = new Error(
							`DynamoDB ProvisionedThroughputExceededException during ${target} on ${tableMessage}`
						);
						err.code = THROUGHPUT_EXCEEDED;
						emitter.emit('warning', err);
					}

					const delay = backoff(retryCount);

					if (operationTimeout && (Date.now() + delay - start) >= operationTimeout) {
						const err = new Error(
							`DynamoDB client operation timeout error during ${target} due to throttling on ${tableMessage}`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}

					return delayPromise(delay).then(() => {
						return tryOperation(retryCount + 1);
					});
				}

				return Promise.reject(err);
			});
		}

		return tryOperation(0);
	}

	return {
		requestWithBackoff(target, params, opts) {
			const options = mergeOptions(opts || {});
			return requestWithBackoff(options, target, params);
		},

		batchWriteWithBackoff(target, params, opts) {
			const options = mergeOptions(opts || {});
			return batchWriteWithBackoff(options, target, params);
		},
	};
};
