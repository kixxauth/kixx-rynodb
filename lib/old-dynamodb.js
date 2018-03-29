'use strict';

const Promise = require('bluebird');
const {clone, compact, partition} = require('./');
const KixxAssert = require('kixx-assert');
const http = require('http');
const https = require('https');
const url = require('url');
const crypto = require('crypto');

const hasOwn = Object.prototype.hasOwnProperty;
const {isNonEmptyString} = KixxAssert.helpers;
const batch = partition(25);

const DYNAMODB_API_VERSION = 'DynamoDB_20120810';
const DEFAULT_REQUEST_TIMEOUT = 700;
const DEFAULT_BACKOFF_MULTIPLIER = 100;
const DEFAULT_AWS_REGION = 'DEFAULT_AWS_REGION';

const DEFAULT_THROUGHPUT = Object.freeze({
	ReadCapacityUnits: 3,
	WriteCapacityUnits: 3
});

const MISSING_TABLE = 'MISSING_TABLE';
const THROUGHPUT_EXCEEDED = 'THROUGHPUT_EXCEEDED';
const OPERATION_TIMEOUT = 'OPERATION_TIMEOUT';

class DynamoDB {
	// options.emitter
	// options.tablePrefix
	// options.requestTimeout
	// options.backoffMultiplier
	// options.operationTimeout
	// options.awsRegion
	// options.awsAccessKey
	// options.awsSecretKey
	// options.dynamodbEndpoint
	constructor(options) {
		const {tablePrefix} = options;

		Object.defineProperties(this, {
			emitter: {
				value: options.emitter
			},
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
			backoffMultiplier: {
				enumerable: true,
				value: options.backoffMultiplier
			},
			operationTimeout: {
				enumerable: true,
				value: options.operationTimeout
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
			dynamodbEndpoint: {
				value: options.dynamodbEndpoint
			}
		});
	}

	getOperationTimeout(options) {
		options = options || {};
		if (Number.isInteger(options.operationTimeout)) {
			return options.operationTimeout;
		}
		return this.operationTimeout;
	}

	getBackoffMultiplier(options) {
		options = options || {};
		if (Number.isInteger(options.backoffMultiplier)) {
			return options.backoffMultiplier;
		}
		return this.backoffMultiplier;
	}

	mergeOptions(options) {
		return Object.assign({}, {
			emitter: this.emitter,
			requestTimeout: this.requestTimeout,
			backoffMultiplier: this.backoffMultiplier,
			operationTimeout: this.operationTimeout
		}, options);
	}

	createRootEntityTableSchema() {
		return {
			TableName: this.rootEntityTable,
			AttributeDefinitions: [
				{AttributeName: '_id', AttributeType: 'S'},
				{AttributeName: '_scope_type_key', AttributeType: 'S'},
				{AttributeName: '_updated', AttributeType: 'S'}
			],
			KeySchema: [
				{AttributeName: '_id', KeyType: 'HASH'},
				{AttributeName: '_scope_type_key', KeyType: 'RANGE'}
			],
			ProvisionedThroughput: DEFAULT_THROUGHPUT,
			GlobalSecondaryIndexes: [{
				IndexName: this.entitiesByTypeIndex,
				KeySchema: [
					{AttributeName: '_scope_type_key', KeyType: 'HASH'},
					{AttributeName: '_updated', KeyType: 'RANGE'}
				],
				Projection: {ProjectionType: 'ALL'},
				ProvisionedThroughput: DEFAULT_THROUGHPUT
			}]
		};
	}

	createRelationshipsTableSchema() {
		return {
			TableName: this.relationshipEntriesTable,
			AttributeDefinitions: [
				{AttributeName: '_subject_key', AttributeType: 'S'},
				{AttributeName: '_predicate_key', AttributeType: 'S'},
				{AttributeName: '_object_key', AttributeType: 'S'},
				{AttributeName: '_index', AttributeType: 'N'}
			],
			KeySchema: [
				{AttributeName: '_subject_key', KeyType: 'HASH'},
				{AttributeName: '_predicate_key', KeyType: 'RANGE'}
			],
			ProvisionedThroughput: DEFAULT_THROUGHPUT,
			GlobalSecondaryIndexes: [{
				IndexName: this.reverseRelationshipsIndex,
				KeySchema: [
					{AttributeName: '_object_key', KeyType: 'HASH'},
					{AttributeName: '_index', KeyType: 'RANGE'}
				],
				Projection: {ProjectionType: 'ALL'},
				ProvisionedThroughput: DEFAULT_THROUGHPUT
			}]
		};
	}

	createIndexEntriesSchema() {
		return {
			TableName: this.indexEntriesTable,
			AttributeDefinitions: [
				{AttributeName: '_index_key', AttributeType: 'S'},
				{AttributeName: '_scope_index_name', AttributeType: 'S'},
				{AttributeName: '_subject_key', AttributeType: 'S'},
				{AttributeName: '_unique_key', AttributeType: 'S'}
			],
			KeySchema: [
				{AttributeName: '_subject_key', KeyType: 'HASH'},
				{AttributeName: '_unique_key', KeyType: 'RANGE'}
			],
			ProvisionedThroughput: DEFAULT_THROUGHPUT,
			GlobalSecondaryIndexes: [{
				IndexName: this.indexLookupIndex,
				KeySchema: [
					{AttributeName: '_scope_index_name', KeyType: 'HASH'},
					{AttributeName: '_index_key', KeyType: 'RANGE'}
				],
				Projection: {ProjectionType: 'ALL'},
				ProvisionedThroughput: DEFAULT_THROUGHPUT
			}]
		};
	}

	setupSchema(options) {
		options = this.mergeOptions(options);
		const {emitter} = options;

		const describeRootEntityTable = () => {
			return this.request('DescribeTable', {TableName: this.rootEntityTable}, options);
		};

		const describeRelationshipEntriesTable = () => {
			return this.request('DescribeTable', {TableName: this.relationshipEntriesTable}, options);
		};

		const describeIndexEntriesTable = () => {
			return this.request('DescribeTable', {TableName: this.indexEntriesTable}, options);
		};

		const pollTableStatus = (TableName) => {
			return this.request('DescribeTable', {TableName}, options).then(({Table}) => {
				const {TableStatus} = Table;
				const {IndexStatus} = (Table.GlobalSecondaryIndexes || [])[0] || {};
				if (TableStatus !== 'ACTIVE' || IndexStatus !== 'ACTIVE') {
					return pollTableStatus(TableName);
				}
				return true;
			});
		};

		const createRootEntityTable = () => {
			const schema = this.createRootEntityTableSchema();
			return this.request('CreateTable', schema, options).then(() => {
				return pollTableStatus(schema.TableName);
			});
		};

		const createRelationshipEntriesTable = () => {
			const schema = this.createRelationshipsTableSchema();
			return this.request('CreateTable', schema, options).then(() => {
				return pollTableStatus(schema.TableName);
			});
		};

		const createIndexEntriesTable = () => {
			const schema = this.createIndexEntriesSchema();
			return this.request('CreateTable', schema, options).then(() => {
				return pollTableStatus(schema.TableName);
			});
		};

		const checkRootEntityTable = (description) => {
			return description;
		};

		const checkRelationshipEntriesTable = (description) => {
			return description;
		};

		const checkIndexEntriesTable = (description) => {
			return description;
		};

		const catchError = (err) => {
			if (err.code === 'ResourceNotFoundException') {
				return null;
			}
			return Promise.reject(err);
		};

		const descriptions = [
			describeRootEntityTable().catch(catchError),
			describeRelationshipEntriesTable().catch(catchError),
			describeIndexEntriesTable().catch(catchError)
		];

		return Promise.all(descriptions).then((res) => {
			const [rootEntityTable, relationshipEntriesTable, indexEntriesTable] = res;
			const promises = [];

			if (!rootEntityTable) {
				promises.push(createRootEntityTable());
				emitter.emit('info', {
					code: 'CREATING_TABLE',
					message: `Table ${this.rootEntityTable} does not exist. Creating now.`
				});
			} else {
				promises.push(checkRootEntityTable(rootEntityTable));
				emitter.emit('info', {
					code: 'VALIDATING_TABLE',
					message: `Table ${this.rootEntityTable} already exists. Doing integrity check.`
				});
			}

			if (!relationshipEntriesTable) {
				promises.push(createRelationshipEntriesTable());
				emitter.emit('info', {
					code: 'CREATING_TABLE',
					message: `Table ${this.relationshipEntriesTable} does not exist. Creating now.`
				});
			} else {
				promises.push(checkRelationshipEntriesTable(relationshipEntriesTable));
				emitter.emit('info', {
					code: 'VALIDATING_TABLE',
					message: `Table ${this.rootEntityTable} already exists. Doing integrity check.`
				});
			}

			if (!indexEntriesTable) {
				promises.push(createIndexEntriesTable());
				emitter.emit('info', {
					code: 'CREATING_TABLE',
					message: `Table ${this.indexEntriesTable} does not exist. Creating now.`
				});
			} else {
				promises.push(checkIndexEntriesTable(indexEntriesTable));
				emitter.emit('info', {
					code: 'VALIDATING_TABLE',
					message: `Table ${this.rootEntityTable} already exists. Doing integrity check.`
				});
			}

			return Promise.all(promises);
		});
	}

	setEntity(record, options) {
		options = this.mergeOptions(options);
		const request = this.requestWithBackoff.bind(this, 'PutItem', options);

		const TableName = this.rootEntityTable;
		const Item = DynamoDB.serializeObject(record);

		const params = {
			TableName,
			Item
		};

		return request(params).then(() => {
			return {item: clone(record)};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(err);
		});
	}

	getEntity(key, options) {
		options = this.mergeOptions(options);
		const request = this.requestWithBackoff.bind(this, 'GetItem', options);

		const TableName = this.rootEntityTable;

		const Key = {
			_id: {S: key._id},
			_scope_type_key: {S: key._scope_type_key}
		};

		const params = {TableName, Key};

		return request(params).then((res) => {
			if (res.Item) return {item: DynamoDB.deserializeObject(res.Item)};
			return {item: null};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(err);
		});
	}

	getIndexEntriesBySubject(subjectKey, options) {
		options = this.mergeOptions(options);
		const request = this.requestWithBackoff.bind(this, 'Query', options);

		const TableName = this.indexEntriesTable;

		const params = Object.freeze({
			TableName,
			ExpressionAttributeNames: {
				'#PartitionKey': '_subject_key'
			},
			ExpressionAttributeValues: {
				':pk': {S: subjectKey}
			},
			KeyConditionExpression: '#PartitionKey = :pk'
		});

		const getPage = (items, ExclusiveStartKey) => {
			let pageParams = params;
			if (ExclusiveStartKey) {
				pageParams = Object.assign({}, pageParams, {ExclusiveStartKey});
			}

			return request(pageParams).then((res) => {
				if (res.Items) items = items.concat(res.Items);
				if (res.LastEvaluatedKey) return getPage(items, res.LastEvaluatedKey);
				return items;
			});
		};

		return getPage([], null).then((items) => {
			return {items: items.map(DynamoDB.deserializeObject)};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(err);
		});
	}

	batchSetIndexEntries(entries, options) {
		options = this.mergeOptions(options);
		const batches = batch(entries);
		const TableName = this.indexEntriesTable;
		const request = this.batchWrite.bind(this, TableName, options);

		const createRequest = (entry) => {
			const Item = DynamoDB.serializeObject(entry);
			return {PutRequest: {Item}};
		};

		const promise = batches.reduce((promise, batch) => {
			return promise.then(() => request(batch.map(createRequest)));
		}, Promise.resolve(null));

		return promise.then(() => true).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(err);
		});
	}

	batchRemoveIndexEntries(keys, options) {
		options = this.mergeOptions(options);
		const batches = batch(keys);
		const TableName = this.indexEntriesTable;
		const request = this.batchWrite.bind(this, TableName, options);

		const createRequest = (key) => {
			const Key = DynamoDB.serializeObject(key);
			return {PutRequest: {Key}};
		};

		const promise = batches.reduce((promise, batch) => {
			return promise.then(() => request(batch.map(createRequest)));
		}, Promise.resolve(null));

		return promise.then(() => true).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(err);
		});
	}

	batchWrite(TableName, options, requests) {
		options = this.mergeOptions(options);
		const {emitter} = options;
		const operationTimeout = this.getOperationTimeout(options);
		const backoff = computeBackoffMs(this.getBackoffMultiplier(options));
		const request = this.requestWithBackoff.bind(this, 'BatchWriteItem', options);
		const start = Date.now();

		function composeParams(requests) {
			const RequestItems = {};
			RequestItems[TableName] = requests;
			return {RequestItems};
		}

		function tryOperation(params, retryCount) {
			return request(params).then((res) => {
				const {UnprocessedItems} = res;
				if (UnprocessedItems && UnprocessedItems[TableName]) {
					const err = new Error(
						`DynamoDB#batchSet() encountered UnprocessedItems running BatchWriteItem on table ${TableName}`
					);
					err.code = THROUGHPUT_EXCEEDED;
					emitter.emit('warning', err);

					const delay = backoff(retryCount);

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB operation timeout error in call to DynamoDB#batchSet() on table ${TableName} due to throttling`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}
					return Promise.delay(delay).then(() => {
						return tryOperation(composeParams(UnprocessedItems[TableName]), retryCount + 1);
					});
				}

				return true;
			});
		}

		const params = composeParams(requests);

		return tryOperation(params, 0);
	}

	requestWithBackoff(target, options, params) {
		const {emitter} = options;
		const operationTimeout = this.getOperationTimeout(options);
		const backoff = computeBackoffMs(this.getBackoffMultiplier(options));
		const request = this.request.bind(this, target, options);
		const start = Date.now();

		function tryOperation(retryCount) {
			return request(params).catch((err) => {
				if (err.code === 'ProvisionedThroughputExceededException') {
					const tableMessage = params.IndexName ? `index ${params.IndexName}` : `table ${params.TableName}`;
					const err = new Error(
						`DynamoDB#requestWithBackoff() encountered ProvisionedThroughputExceededException on ${tableMessage}`
					);
					err.code = THROUGHPUT_EXCEEDED;
					emitter.emit('warning', err);

					const delay = backoff(retryCount);

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB operation timeout error in call to ${tableMessage} due to throttling`
						);
						err.code = OPERATION_TIMEOUT;
						return Promise.reject(err);
					}
					return Promise.delay(delay).then(() => {
						return tryOperation(retryCount + 1);
					});
				}
				return Promise.reject(err);
			});
		}

		return tryOperation(0);
	}

	request(target, options, params) {
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
						`Error event in RynoDB AWS HTTP client response: ${err.message}`
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
				if (err.code === 'ECONNREFUSED') {
					return reject(new Error(
						`DynamoDB connection refused to ${protocol}//${hostname}:${port}`
					));
				}
				return reject(new Error(
					`Error event in RynoDB AWS HTTP client request: ${err.message}`
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
		return Object.keys(obj).reduce((rv, key) => {
			rv[key] = deserializeObject(obj[key]);
			return rv;
		}, Object.create(null));
	}

	// options.emitter
	// options.tablePrefix
	// options.requestTimeout
	// options.backoffMultiplier
	// options.operationTimeout
	// options.awsRegion
	// options.awsAccessKey
	// options.awsSecretKey
	// options.dynamodbEndpoint
	static create(options) {
		if (!options.emitter || typeof options.emitter.emit !== 'function') {
			throw new Error(`expects options.emitter EventEmitter`);
		}

		if (!/^[a-zA-Z_]+$/.test(options.tablePrefix)) {
			throw new Error(`invalid table prefix String`);
		}

		if (!isNonEmptyString(options.awsRegion) && !isNonEmptyString(options.dynamodbEndpoint)) {
			throw new Error(`awsRegion or dynamodbEndpoint Strings must be present`);
		}

		const requestTimeout = DEFAULT_REQUEST_TIMEOUT;
		const backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
		const operationTimeout = parseInt(options.operationTimeout, 10) || 0;
		const awsRegion = typeof options.awsRegion === 'string' ? options.awsRegion : DEFAULT_AWS_REGION;
		const dynamodbEndpoint = isNonEmptyString(options.dynamodbEndpoint) ? options.dynamodbEndpoint : `https://dynamodb.${awsRegion}.amazonaws.com`;

		return new DynamoDB(Object.assign({}, options, {
			requestTimeout,
			backoffMultiplier,
			operationTimeout,
			awsRegion,
			dynamodbEndpoint
		}));
	}
}

module.exports = DynamoDB;

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

function getAwsErrorName(err) {
	return (err || {}).__type || '#UnrecognizedClientException';
}

function getAwsErrorMessage(err) {
	return (err || {}).message;
}

function computeBackoffMs(backoffMultiplier) {
	return function computeBackoffMs(times) {
		return Math.pow(2, times) * backoffMultiplier;
	};
}

function serializeObject(obj) {
	switch (typeof obj) {
	case 'string':
		if (obj.length === 0) return {NULL: true};
		return {S: obj};
	case 'number':
		if (isNaN(obj)) return {NULL: true};
		return {N: obj.toString()};
	case 'boolean':
		return {BOOL: obj};
	case 'function':
	case 'undefined':
		return null;
	case 'object':
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
	if (hasOwn.call(val, 'S')) {
		return val.S.toString();
	} else if (hasOwn.call(val, 'N')) {
		return parseFloat(val.N);
	} else if (val.SS || val.NS) {
		return val.SS || val.NS;
	} else if (hasOwn.call(val, 'BOOL')) {
		return Boolean(val.BOOL);
	} else if (hasOwn.call(val, 'M')) {
		return DynamoDB.deserializeObject(val.M);
	} else if (hasOwn.call(val, 'L')) {
		return val.L.map(deserializeObject);
	} else if (hasOwn.call(val, 'NULL')) {
		return null;
	}
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
