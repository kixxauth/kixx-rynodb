'use strict';

const Promise = require('bluebird');
const {StackedError} = require('kixx');
const {clone, compact, isNonEmptyString} = require('kixx/library');
const http = require('http');
const https = require('https');
const url = require('url');
const crypto = require('crypto');

const hasOwn = Object.prototype.hasOwnProperty;

const DEFAULT_REQUEST_TIMEOUT = 700;
const DEFAULT_BACKOFF_MULTIPLIER = 100;
const DEFAULT_OPERATION_TIMEOUT = 0;
const DEFAULT_AWS_REGION = 'DEFAULT_AWS_REGION';

const DEFAULT_THROUGHPUT = Object.freeze({
	ReadCapacityUnits: 1,
	WriteCapacityUnits: 2
});

const DYNAMODB_API_VERSION = 'DynamoDB_20120810';
const ECONNREFUSED = 'ECONNREFUSED';
const MISSING_TABLE = 'MISSING_TABLE';
const THROUGHPUT_EXCEEDED = 'THROUGHPUT_EXCEEDED';
const OPERATION_TIMEOUT = 'OPERATION_TIMEOUT';
const ProvisionedThroughputExceededException = 'ProvisionedThroughputExceededException';
const ResourceNotFoundException = 'ResourceNotFoundException';
const UnrecognizedClientException = 'UnrecognizedClientException';

const PutItem = 'PutItem';
const GetItem = 'GetItem';
const Query = 'Query';
const DeleteItem = 'DeleteItem';

class DynamoDB {
	static get MISSING_TABLE() {
		return MISSING_TABLE;
	}

	static get THROUGHPUT_EXCEEDED() {
		return THROUGHPUT_EXCEEDED;
	}

	static get OPERATION_TIMEOUT() {
		return OPERATION_TIMEOUT;
	}

	static get ProvisionedThroughputExceededException() {
		return ProvisionedThroughputExceededException;
	}

	static get ResourceNotFoundException() {
		return ResourceNotFoundException;
	}

	static get UnrecognizedClientException() {
		return UnrecognizedClientException;
	}

	// options.emitter
	// options.tablePrefix
	// options.awsRegion
	// options.awsAccessKey
	// options.awsSecretKey
	// options.dynamodbEndpoint
	// options.operationTimeout
	// options.backoffMultiplier
	// options.requestTimeout
	constructor(options) {
		const {tablePrefix} = options;

		const requestTimeout = Number.isInteger(options.requestTimeout) ? options.requestTimeout : DEFAULT_REQUEST_TIMEOUT;
		const operationTimeout = Number.isInteger(options.operationTimeout) ? options.operationTimeout : DEFAULT_OPERATION_TIMEOUT;
		const backoffMultiplier = Number.isInteger(options.backoffMultiplier) ? options.backoffMultiplier : DEFAULT_BACKOFF_MULTIPLIER;

		const awsRegion = typeof options.awsRegion === 'string' ? options.awsRegion : DEFAULT_AWS_REGION;

		const dynamodbEndpoint = isNonEmptyString(options.dynamodbEndpoint) ? options.dynamodbEndpoint : `https://dynamodb.${awsRegion}.amazonaws.com`;

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
				value: dynamodbEndpoint
			}
		});
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

	setEntity(obj, options = {}) {
		options = this._mergeOptions(options);
		const request = this._requestWithBackoff.bind(this, PutItem, options);

		const TableName = this.rootEntityTable;
		const Item = DynamoDB.serializeObject(obj);

		const params = {
			TableName,
			Item
		};

		return request(params).then(() => {
			return {Item: clone(obj)};
		}).catch((err) => {
			if (err.code === DynamoDB.UnrecognizedClientException) {
				const returnError = new Error(`DynamoDB request error: ${err.message} during setEntity()`);
				returnError.code = err.code;
				return Promise.reject(returnError);
			}
			if (err.code === DynamoDB.ResourceNotFoundException) {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(new StackedError(
				`Unexpected DynamoDB exception in DynamoDB.setEntity()`,
				err
			));
		});
	}

	getEntity(key, options = {}) {
		options = this._mergeOptions(options);
		const {ProjectionExpression, ExpressionAttributeNames} = options;
		const request = this._requestWithBackoff.bind(this, GetItem, options);

		const TableName = this.rootEntityTable;
		const Key = {
			_id: {S: key._id},
			_scope_type_key: {S: key._scope_type_key}
		};

		const params = {
			TableName,
			Key
		};

		if (ProjectionExpression && ExpressionAttributeNames) {
			params.ExpressionAttributeNames = ExpressionAttributeNames;
			params.ProjectionExpression = ProjectionExpression;
		}

		return request(params).then((res) => {
			return {Item: DynamoDB.deserializeObject(res.Item)};
		}).catch((err) => {
			if (err.code === DynamoDB.UnrecognizedClientException) {
				const returnError = new Error(`DynamoDB request error: ${err.message} during getEntity()`);
				returnError.code = err.code;
				return Promise.reject(returnError);
			}
			if (err.code === DynamoDB.ResourceNotFoundException) {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(new StackedError(
				`Unexpected DynamoDB exception in DynamoDB.getEntity()`,
				err
			));
		});
	}

	removeEntity(key, options = {}) {
		options = this._mergeOptions(options);
		const request = this._requestWithBackoff.bind(this, DeleteItem, options);

		const TableName = this.rootEntityTable;
		const Key = {
			_id: {S: key._id},
			_scope_type_key: {S: key._scope_type_key}
		};

		const params = {
			TableName,
			Key
		};

		return request(params).then(() => {
			return true;
		}).catch((err) => {
			if (err.code === DynamoDB.UnrecognizedClientException) {
				const returnError = new Error(`DynamoDB request error: ${err.message} during removeEntity()`);
				returnError.code = err.code;
				return Promise.reject(returnError);
			}
			if (err.code === DynamoDB.ResourceNotFoundException) {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(new StackedError(
				`Unexpected DynamoDB exception in DynamoDB.removeEntity()`,
				err
			));
		});
	}

	scanByType(args, options = {}) {
		options = this._mergeOptions(options);
		const {key, Limit, ExclusiveStartKey} = args;
		const request = this._requestWithBackoff.bind(this, Query, options);

		const TableName = this.rootEntityTable;
		const IndexName = this.entitiesByTypeIndex;

		const params = {
			TableName,
			IndexName,
			ExpressionAttributeNames: {
				'#key': '_scope_type_key'
			},
			ExpressionAttributeValues: {
				':val': {S: key}
			},
			KeyConditionExpression: '#key = :val'
		};

		if (Limit) {
			params.Limit = Limit;
		}
		if (ExclusiveStartKey) {
			params.ExclusiveStartKey = ExclusiveStartKey;
		}

		return request(params).then((res) => {
			const rv = {
				Items: res.Items.map(DynamoDB.deserializeObject.bind(DynamoDB)),
				Count: res.Count
			};

			if (res.LastEvaluatedKey) {
				rv.LastEvaluatedKey = res.LastEvaluatedKey;
			}

			return rv;
		}).catch((err) => {
			if (err.code === DynamoDB.UnrecognizedClientException) {
				const returnError = new Error(`DynamoDB request error: ${err.message} during scanByType()`);
				returnError.code = err.code;
				return Promise.reject(returnError);
			}
			if (err.code === DynamoDB.ResourceNotFoundException) {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(new StackedError(
				`Unexpected DynamoDB exception in DynamoDB.scanByType()`,
				err
			));
		});
	}

	getIndexKeys(subjectKey, options = {}) {
		options = this._mergeOptions(options);

		const TableName = this.indexEntriesTable;

		const constantParams = {
			TableName,
			ExpressionAttributeNames: {
				'#key': '_subject_key',
				'#uniquekey': '_unique_key'
			},
			ExpressionAttributeValues: {
				':val': {S: subjectKey}
			},
			KeyConditionExpression: '#key = :val',
			ProjectionExpression: '#key, #uniquekey'
		};

		return this._getAllKeys(constantParams, options).then((items) => {
			return items.map(DynamoDB.deserializeObject.bind(DynamoDB));
		}).catch((err) => {
			if (err.code === DynamoDB.UnrecognizedClientException) {
				const returnError = new Error(`DynamoDB request error: ${err.message} during getIndexKeys()`);
				returnError.code = err.code;
				return Promise.reject(returnError);
			}
			if (err.code === DynamoDB.ResourceNotFoundException) {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(new StackedError(
				`Unexpected DynamoDB exception in DynamoDB.getIndexKeys()`,
				err
			));
		});
	}

	getRelationshipKeys(subjectKey, options = {}) {
		options = this._mergeOptions(options);

		const TableName = this.indexEntriesTable;

		const constantParams = {
			TableName,
			ExpressionAttributeNames: {
				'#key': '_subject_key',
				'#uniquekey': '_predicate_key'
			},
			ExpressionAttributeValues: {
				':val': {S: subjectKey}
			},
			KeyConditionExpression: '#key = :val',
			ProjectionExpression: '#key, #uniquekey'
		};

		return this._getAllKeys(constantParams, options).then((items) => {
			return items.map(DynamoDB.deserializeObject.bind(DynamoDB));
		}).catch((err) => {
			if (err.code === DynamoDB.UnrecognizedClientException) {
				const returnError = new Error(`DynamoDB request error: ${err.message} during getRelationshipKeys()`);
				returnError.code = err.code;
				return Promise.reject(returnError);
			}
			if (err.code === DynamoDB.ResourceNotFoundException) {
				const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
				returnError.code = MISSING_TABLE;
				return Promise.reject(returnError);
			}
			return Promise.reject(new StackedError(
				`Unexpected DynamoDB exception in DynamoDB.getRelationshipKeys()`,
				err
			));
		})
	}

	// getIndexEntriesBySubject(subjectKey, options) {
	// 	options = this.mergeOptions(options);
	// 	const request = this.requestWithBackoff.bind(this, 'Query', options);

	// 	const TableName = this.indexEntriesTable;

	// 	const params = Object.freeze({
	// 		TableName,
	// 		ExpressionAttributeNames: {
	// 			'#PartitionKey': '_subject_key'
	// 		},
	// 		ExpressionAttributeValues: {
	// 			':pk': {S: subjectKey}
	// 		},
	// 		KeyConditionExpression: '#PartitionKey = :pk'
	// 	});

	// 	const getPage = (items, ExclusiveStartKey) => {
	// 		let pageParams = params;
	// 		if (ExclusiveStartKey) {
	// 			pageParams = Object.assign({}, pageParams, {ExclusiveStartKey});
	// 		}

	// 		return request(pageParams).then((res) => {
	// 			if (res.Items) items = items.concat(res.Items);
	// 			if (res.LastEvaluatedKey) return getPage(items, res.LastEvaluatedKey);
	// 			return items;
	// 		});
	// 	};

	// 	return getPage([], null).then((items) => {
	// 		return {items: items.map(DynamoDB.deserializeObject)};
	// 	}).catch((err) => {
	// 		if (err.code === DynamoDB.ResourceNotFoundException) {
	// 			const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
	// 			returnError.code = MISSING_TABLE;
	// 			return Promise.reject(returnError);
	// 		}
	// 		return Promise.reject(err);
	// 	});
	// }

	// batchSetIndexEntries(entries, options) {
	// 	options = this.mergeOptions(options);
	// 	const batches = batch(entries);
	// 	const TableName = this.indexEntriesTable;
	// 	const request = this.batchWrite.bind(this, TableName, options);

	// 	const createRequest = (entry) => {
	// 		const Item = DynamoDB.serializeObject(entry);
	// 		return {PutRequest: {Item}};
	// 	};

	// 	const promise = batches.reduce((promise, batch) => {
	// 		return promise.then(() => request(batch.map(createRequest)));
	// 	}, Promise.resolve(null));

	// 	return promise.then(() => true).catch((err) => {
	// 		if (err.code === DynamoDB.ResourceNotFoundException) {
	// 			const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
	// 			returnError.code = MISSING_TABLE;
	// 			return Promise.reject(returnError);
	// 		}
	// 		return Promise.reject(err);
	// 	});
	// }

	// batchRemoveIndexEntries(keys, options) {
	// 	options = this.mergeOptions(options);
	// 	const batches = batch(keys);
	// 	const TableName = this.indexEntriesTable;
	// 	const request = this.batchWrite.bind(this, TableName, options);

	// 	const createRequest = (key) => {
	// 		const Key = DynamoDB.serializeObject(key);
	// 		return {PutRequest: {Key}};
	// 	};

	// 	const promise = batches.reduce((promise, batch) => {
	// 		return promise.then(() => request(batch.map(createRequest)));
	// 	}, Promise.resolve(null));

	// 	return promise.then(() => true).catch((err) => {
	// 		if (err.code === DynamoDB.ResourceNotFoundException) {
	// 			const returnError = new Error(`The DynamoDB table '${TableName}' does not exist.`);
	// 			returnError.code = MISSING_TABLE;
	// 			return Promise.reject(returnError);
	// 		}
	// 		return Promise.reject(err);
	// 	});
	// }

	listTables(options = {}) {
		options = this._mergeOptions(options);
		return this._request('ListTables', options, {}).then((res) => {
			return {tables: res.TableNames};
		}).catch((err) => {
			return Promise.reject(new StackedError(
				`Unexpected DynamoDB exception in DynamoDB.listTables()`,
				err
			));
		});
	}

	setupSchema(options = {}) {
		options = this._mergeOptions(options);
		const {emitter} = options;

		const describeRootEntityTable = () => {
			return this._request('DescribeTable', options, {TableName: this.rootEntityTable});
		};

		const describeRelationshipEntriesTable = () => {
			return this._request('DescribeTable', options, {TableName: this.relationshipEntriesTable});
		};

		const describeIndexEntriesTable = () => {
			return this._request('DescribeTable', options, {TableName: this.indexEntriesTable});
		};

		const pollTableStatus = (TableName) => {
			return this._request('DescribeTable', options, {TableName}).then(({Table}) => {
				const {TableStatus} = Table;
				const {IndexStatus} = (Table.GlobalSecondaryIndexes || [])[0] || {};
				if (TableStatus !== 'ACTIVE' || IndexStatus !== 'ACTIVE') {
					return pollTableStatus(TableName);
				}
				return Table;
			});
		};

		const createRootEntityTable = () => {
			const schema = this.createRootEntityTableSchema();
			return this._request('CreateTable', options, schema).then(() => {
				return pollTableStatus(schema.TableName);
			});
		};

		const createRelationshipEntriesTable = () => {
			const schema = this.createRelationshipsTableSchema();
			return this._request('CreateTable', options, schema).then(() => {
				return pollTableStatus(schema.TableName);
			});
		};

		const createIndexEntriesTable = () => {
			const schema = this.createIndexEntriesSchema();
			return this._request('CreateTable', options, schema).then(() => {
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
			if (err.code === DynamoDB.ResourceNotFoundException) {
				return null;
			}
			return Promise.reject(err);
		};

		const descriptions = [
			describeRootEntityTable().catch(catchError),
			describeRelationshipEntriesTable().catch(catchError),
			describeIndexEntriesTable().catch(catchError)
		];

		return Promise.all(descriptions).then((tables) => {
			let [rootEntityTable, relationshipEntriesTable, indexEntriesTable] = tables;

			return Promise.resolve(null)
				.then(() => {
					if (!rootEntityTable) {
						emitter.emit('info', {
							code: 'CREATING_TABLE',
							message: `Table ${this.rootEntityTable} does not exist. Creating now.`
						});
						return createRootEntityTable();
					}

					emitter.emit('info', {
						code: 'VALIDATING_TABLE',
						message: `Table ${this.rootEntityTable} already exists. Doing integrity check.`
					});
					return checkRootEntityTable(rootEntityTable);
				})
				.then((table) => rootEntityTable = table)
				.then(() => {
					if (!relationshipEntriesTable) {
						emitter.emit('info', {
							code: 'CREATING_TABLE',
							message: `Table ${this.relationshipEntriesTable} does not exist. Creating now.`
						});
						return createRelationshipEntriesTable();
					}

					emitter.emit('info', {
						code: 'VALIDATING_TABLE',
						message: `Table ${this.rootEntityTable} already exists. Doing integrity check.`
					});
					return checkRelationshipEntriesTable(relationshipEntriesTable);
				})
				.then((table) => relationshipEntriesTable = table)
				.then(() => {
					if (!indexEntriesTable) {
						emitter.emit('info', {
							code: 'CREATING_TABLE',
							message: `Table ${this.indexEntriesTable} does not exist. Creating now.`
						});
						return createIndexEntriesTable();
					}

					emitter.emit('info', {
						code: 'VALIDATING_TABLE',
						message: `Table ${this.rootEntityTable} already exists. Doing integrity check.`
					});
					return checkIndexEntriesTable(indexEntriesTable);
				})
				.then((table) => indexEntriesTable = table)
				.then(() => [rootEntityTable, relationshipEntriesTable, indexEntriesTable])
				.catch((err) => {
					return Promise.reject(new StackedError(
						`Unexpected DynamoDB exception in DynamoDB.setupSchema()`,
						err
					));
				});
		});
	}

	_getAllKeys(constantParams, options) {
		const request = this._requestWithBackoff.bind(this, Query, options);

		function getPage(items, ExclusiveStartKey) {
			const params = Object.assign({}, constantParams, {
				ExclusiveStartKey
			});
			return request(params).then((res) => {
				items = items.concat(res.Items);

				if (res.LastEvaluatedKey) {
					return getPage(items, res.LastEvaluatedKey);
				}

				return items;
			});
		}

		return getPage([], null);
	}

	_batchWrite(TableName, requests, options) {
		const {emitter, operationTimeout, backoffMultiplier} = options;
		const backoff = DynamoDB.computeBackoffMilliseconds(backoffMultiplier);
		const request = this._requestWithBackoff.bind(this, Query, options);
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
						`DynamoDB encountered UnprocessedItems while running BatchWriteItem on table ${TableName}`
					);
					err.code = THROUGHPUT_EXCEEDED;
					emitter.emit('warning', err);

					const delay = backoff(retryCount);

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB operation timeout error during BatchWriteItem due to throttling on table ${TableName}`
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

	_mergeOptions(options) {
		const requestTimeout = Number.isInteger(options.requestTimeout) ? options.requestTimeout : DEFAULT_REQUEST_TIMEOUT;
		const operationTimeout = Number.isInteger(options.operationTimeout) ? options.operationTimeout : DEFAULT_OPERATION_TIMEOUT;
		const backoffMultiplier = Number.isInteger(options.backoffMultiplier) ? options.backoffMultiplier : DEFAULT_BACKOFF_MULTIPLIER;
		const emitter = options.emitter || this.emitter;

		if (!emitter || typeof emitter.emit !== 'function') {
			throw new Error(`expects options.emitter to be an EventEmitter`);
		}

		return Object.assign({}, options, {
			emitter,
			requestTimeout,
			backoffMultiplier,
			operationTimeout
		});
	}

	_requestWithBackoff(target, options, params) {
		const {emitter, operationTimeout, backoffMultiplier} = options;
		const backoff = DynamoDB.computeBackoffMilliseconds(backoffMultiplier);
		const request = this._request.bind(this, target, options);
		const start = Date.now();

		function tryOperation(retryCount) {
			return request(params).catch((err) => {
				if (err.code === ProvisionedThroughputExceededException) {
					const tableMessage = params.IndexName ? `index ${params.IndexName}` : `table ${params.TableName}`;
					const err = new Error(
						`DynamoDB encountered ProvisionedThroughputExceededException on ${tableMessage} during ${target}`
					);
					err.code = THROUGHPUT_EXCEEDED;
					emitter.emit('warning', err);

					const delay = backoff(retryCount);

					if (operationTimeout && Date.now() + delay - start >= operationTimeout) {
						const err = new Error(
							`DynamoDB operation timeout error during ${target} due to throttling on ${tableMessage} `
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
						`Error event in Kixx RynoDB AWS HTTP client response: ${err.message}`
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
		return Object.keys(obj || {}).reduce((item, key) => {
			const val = serializeObject(obj[key]);
			if (val) item[key] = val;
			return item;
		}, {});
	}

	static deserializeObject(obj) {
		return Object.keys(obj || {}).reduce((rv, key) => {
			rv[key] = deserializeObject(obj[key]);
			return rv;
		}, Object.create(null));
	}

	static computeBackoffMilliseconds(backoffMultiplier) {
		return function computeBackoffMilliseconds(times) {
			return Math.pow(2, times) * backoffMultiplier;
		};
	}

	// options.emitter
	// options.tablePrefix
	// options.awsRegion
	// options.awsAccessKey
	// options.awsSecretKey
	// options.dynamodbEndpoint
	// options.operationTimeout
	// options.backoffMultiplier
	// options.requestTimeout
	static create(options = {}) {
		if (!options.emitter || typeof options.emitter.emit !== 'function') {
			throw new Error(`expects options.emitter to be an EventEmitter`);
		}

		if (!isNonEmptyString(options.tablePrefix) || !/^[a-zA-Z_]{2,10}$/.test(options.tablePrefix)) {
			throw new Error(`invalid table prefix String`);
		}

		if (!isNonEmptyString(options.awsRegion) && !isNonEmptyString(options.dynamodbEndpoint)) {
			throw new Error(`awsRegion or dynamodbEndpoint Strings must be present`);
		}

		return new DynamoDB(options);
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
