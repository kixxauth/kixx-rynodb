'use strict';

const Promise = require(`bluebird`);
const {StackedError} = require(`kixx`);
const {assert, compact, isNonEmptyString, path, pathOr} = require(`kixx/library`);
const http = require(`http`);
const https = require(`https`);
const url = require(`url`);
const crypto = require(`crypto`);

const DEFAULT_REQUEST_TIMEOUT = 10000;
const DEFAULT_AWS_REGION = `default-region`;
const DYNAMODB_API_VERSION = `DynamoDB_20120810`;

const hasOwn = Object.prototype.hasOwnProperty;
const getAwsErrorName = pathOr(`#UnrecognizedClientException`, [`__type`]);
const getAwsErrorMessage = path([`message`]);

class DynamodDB {
	// options.tablePrefix
	// options.requestTimeout
	// options.awsRegion
	// options.awsAccessKey
	// options.awsSecretKey
	// options.awsEndpoint
	constructor(options) {
		const {tablePrefix} = options;

		Object.defineProperties(this, {
			tablePrefix: {
				enumerable: true,
				value: tablePrefix
			},
			rootEntityTable: {
				enumerable: true,
				value: `${tablePrefix}_root_entities`
			},
			entitiesByTypeIndex: {
				enumerable: true,
				value: `${tablePrefix}_entities_by_type`
			},
			relationshipEntriesTable: {
				enumerable: true,
				value: `${tablePrefix}_relationship_entries`
			},
			reverseRelationshipsIndex: {
				enumerable: true,
				value: `${tablePrefix}_reverse_relationships`
			},
			indexEntriesTable: {
				enumerable: true,
				value: `${tablePrefix}_index_entries`
			},
			indexLookupIndex: {
				enumerable: true,
				value: `${tablePrefix}_index_lookup`
			},
			requestTimeout: {
				enumerable: true,
				value: options.requestTimeout
			},
			awsRegion: {
				enumerable: true,
				value: options.awsRegion
			},
			awsAccessKey: {
				value: options.awsAccessKey
			},
			awsSecretKey: {
				value: options.awsSecretKey
			},
			awsEndpoint: {
				value: options.awsEndpoint
			}
		});
	}

	createTable(params) {
	}

	describeTable(params) {
		const {TableName} = params;
		assert.isNonEmptyString(TableName, `TableName must be a String`);

		return this.request(`DescribeTable`, {TableName});
	}

	request(target, params) {
		const timeout = this.requestTimeout;
		const region = this.awsRegion;
		const accessKey = this.awsAccessKey;
		const secretKey = this.awsSecretKey;
		const endpoint = this.awsEndpoint;

		target = `${DYNAMODB_API_VERSION}.${target}`;
		const data = JSON.stringify(params);

		return new Promise((resolve, reject) => {
			const {protocol, hostname, port, method, headers, path} = DynamodDB.amzRequestOptions({
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
				method: method || `POST`,
				path,
				headers,
				timeout
			};

			const NS = protocol === `https:` ? https : http;

			const req = NS.request(params, function bufferHttpServerResponse(res) {
				res.once(`error`, (err) => {
					return reject(new StackedError(
						`Error event in RynoDB AWS HTTP client response: ${err.code} ${err.message}`,
						err
					));
				});

				const chunks = [];
				res.on(`data`, (chunk) => chunks.push(chunk));

				res.on(`end`, () => {
					const body = JSON.parse(Buffer.concat(chunks).toString());

					if (res.statusCode === 200) return resolve(body);

					const errName = getAwsErrorName(body).split(`#`).pop();
					const errMessage = getAwsErrorMessage(body) || errName;

					reject(new AwsApiError(errName, errMessage));
				});
			});

			req.once(`error`, (err) => {
				return reject(new StackedError(
					`Error event in RynoDB AWS HTTP client request: ${err.code} ${err.message}`,
					err
				));
			});

			req.write(data);
			req.end();
		});
	}

	static serializeObject(obj) {
		return Object.keys(obj).reduce((item, key) => {
			const val = serializeObject(obj[key]);
			if (val) item[key] = val;
			return item;
		}, {});
	}

	static deserializeObject(obj) {
		return Object.keys(obj).reduce(function (rv, key) {
			rv[key] = deserializeObject(obj[key]);
			return rv;
		}, Object.create(null));
	}

	// config.region
	// config.accessKey
	// config.secretKey
	// config.endpoint
	// options.target
	static amzRequestOptions(options, payload) {
		const {region, accessKey, secretKey, target} = options;
		const endpoint = url.parse(options.endpoint);

		const t = new Date();
		const parts = t.toISOString().split(`.`);
		const amzdate = parts[0].replace(/-|:/g, ``) + `Z`; // '20170630T060649Z'
		const datestamp = parts[0].split(`T`)[0].replace(/-/g, ``);

		const payloadHash = hash(payload);
		const signedHeaders = `host;x-amz-content-sha256;x-amz-date;x-amz-target`;
		const scope = `${datestamp}/${region}/dynamodb/aws4_request`;

		const headers = [
			`host:${endpoint.hostname}`,
			`x-amz-content-sha256:${payloadHash}`,
			`x-amz-date:${amzdate}`,
			`x-amz-target:${target}`
		].join(`\n`);

		const CanonicalString = [
			`POST`,
			`${endpoint.path}\n`,
			`${headers}\n`,
			signedHeaders,
			payloadHash
		].join(`\n`);

		const StringToSign = [
			`AWS4-HMAC-SHA256`,
			amzdate,
			scope,
			hash(CanonicalString)
		].join(`\n`);

		const key = amzSignatureKey(secretKey, datestamp, region, `dynamodb`);
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
			method: `POST`,
			path: endpoint.path,
			headers: Object.freeze({
				'x-amz-date': amzdate,
				'Content-Type': `application/x-amz-json-1.0`,
				'Content-Length': Buffer.byteLength(payload),
				'Authorization': Authorization,
				'x-amz-target': target,
				'x-amz-content-sha256': payloadHash
			})
		});
	}

	// options.tablePrefix
	// options.requestTimeout
	// options.awsRegion
	// options.awsAccessKey
	// options.awsSecretKey
	// options.awsEndpoint
	static create(options) {
		assert.isOk(
			/^[a-zA-Z_]+$/.test(options.tablePrefix),
			`invalid table prefix String`
		);

		assert.isOk(
			isNonEmptyString(options.awsRegion) || isNonEmptyString(options.awsEndpoint),
			`awsRegion or awsEndpoint Strings must be present`
		);

		const requestTimeout = DEFAULT_REQUEST_TIMEOUT;
		const awsRegion = isNonEmptyString(options.awsRegion) ? options.awsRegion : DEFAULT_AWS_REGION;
		const awsEndpoint = isNonEmptyString(options.awsEndpoint) ? options.awsEndpoint : `https://dynamodb.${awsRegion}.amazonaws.com`;

		return new DynamodDB(Object.assign({}, {
			requestTimeout,
			awsRegion,
			awsEndpoint
		}, options));
	}
}

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

function hmac(key, data) {
	const hmac = crypto.createHmac(`sha256`, key);
	hmac.update(data);
	return hmac.digest(`hex`);
}

function sign(key, data) {
	const hmac = crypto.createHmac(`sha256`, key);
	hmac.update(data);
	return hmac.digest();
}

function hash(data) {
	const hash = crypto.createHash(`sha256`);
	hash.update(data);
	return hash.digest(`hex`);
}

function amzSignatureKey(key, datestamp, region, service) {
	const kDate = sign(`AWS4` + key, datestamp);
	const kRegion = sign(kDate, region);
	const kService = sign(kRegion, service);
	const kSigning = sign(kService, `aws4_request`);
	return kSigning;
}

function serializeObject(obj) {
	switch (typeof obj) {
		case `string`:
			if (obj.length === 0) return {NULL: true};
			return {S: obj};
		case `number`:
			if (isNaN(obj)) return {NULL: true};
			return {N: obj.toString()};
		case `boolean`:
			return {BOOL: obj};
		case `function`:
		case `undefined`:
			return null;
		case `object`:
			if (!obj) return {NULL: true};
			return Array.isArray(obj) ? serializeArray(obj) : serializeMap(obj);
		default:
			throw new Error(`Unsupported JavaScript type '${typeof obj}' for DynamodDB serialization`);
	}
}

function serializeArray(obj) {
	return {L: compact(obj.map(serializeObject))};
}

function serializeMap(obj) {
	const keys = Object.keys(obj);
	const rv = {M: {}};

	if (keys.length === 0) {
		return rv;
	}

	rv.M = keys.reduce(function (M, key) {
		const val = serializeObject(obj[key]);
		if (val) {
			M[key] = val;
		}
		return M;
	}, rv.M);

	return rv;
}

function deserializeObject(val) {
	if (hasOwn.call(val, `S`)) {
		return val.S.toString();
	} else if (hasOwn.call(val, `N`)) {
		return parseFloat(val.N);
	} else if (val.SS || val.NS) {
		return val.SS || val.NS;
	} else if (hasOwn.call(val, `BOOL`)) {
		return Boolean(val.BOOL);
	} else if (hasOwn.call(val, `M`)) {
		return DynamodDB.deserializeObject(val.M);
	} else if (hasOwn.call(val, `L`)) {
		return val.L.map(deserializeObject);
	} else if (hasOwn.call(val, `NULL`)) {
		return null;
	}
}

module.exports = DynamodDB;

// {
//   "statusCode": 400,
//   "headers": {
//     "server": "Server",
//     "date": "Fri, 09 Mar 2018 23:06:03 GMT",
//     "content-type": "application/x-amz-json-1.0",
//     "content-length": "63",
//     "connection": "close",
//     "x-amzn-requestid": "7MPNM852AKL1132B4P64M34TGBVV4KQNSO5AEMVJF66Q9ASUAAJG",
//     "x-amz-crc32": "1368724161"
//   },
//   "body": {
//     "__type": "com.amazon.coral.service#UnknownOperationException"
//   }
// }

// {
//   "statusCode": 400,
//   "headers": {
//     "server": "Server",
//     "date": "Fri, 09 Mar 2018 23:09:20 GMT",
//     "content-type": "application/x-amz-json-1.0",
//     "content-length": "132",
//     "connection": "close",
//     "x-amzn-requestid": "2C31J7MAIR5PUVEDUEG0I87A0RVV4KQNSO5AEMVJF66Q9ASUAAJG",
//     "x-amz-crc32": "3880715766"
//   },
//   "body": {
//     "__type": "com.amazon.coral.service#UnrecognizedClientException",
//     "message": "The security token included in the request is invalid."
//   }
// }

// {
//   "statusCode": 400,
//   "headers": {
//     "server": "Server",
//     "date": "Fri, 09 Mar 2018 23:46:44 GMT",
//     "content-type": "application/x-amz-json-1.0",
//     "content-length": "143",
//     "connection": "close",
//     "x-amzn-requestid": "R2BKVRQDROCLJRL30DTN9SGBCBVV4KQNSO5AEMVJF66Q9ASUAAJG",
//     "x-amz-crc32": "3733221906"
//   },
//   "body": {
//     "__type": "com.amazon.coral.validate#ValidationException",
//     "message": "The parameter 'TableName' is required but was not present in the request"
//   }
// }

// {
//   "statusCode": 400,
//   "headers": {
//     "server": "Server",
//     "date": "Fri, 09 Mar 2018 23:48:58 GMT",
//     "content-type": "application/x-amz-json-1.0",
//     "content-length": "134",
//     "connection": "close",
//     "x-amzn-requestid": "TRH0JTK82DRRSDNTT9MQ5USN6JVV4KQNSO5AEMVJF66Q9ASUAAJG",
//     "x-amz-crc32": "2101048194"
//   },
//   "body": {
//     "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
//     "message": "Requested resource not found: Table: foo not found"
//   }
// }
