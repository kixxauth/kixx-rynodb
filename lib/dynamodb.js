'use strict';

const Promise = require('bluebird');
const {ConflictError, NotFoundError, StackedError} = require('kixx');
const DynamoDbClient = require('./dynamodb-client');
const {clone, compact, splitEvery} = require('kixx/library');

const hasOwn = Object.prototype.hasOwnProperty;

const DEFAULT_THROUGHPUT = Object.freeze({
	ReadCapacityUnits: 1,
	WriteCapacityUnits: 2
});


class DynamoDB {
	// options.emitter
	// options.client
	// options.tablePrefix
	constructor(options) {
		const {tablePrefix, emitter, client} = options;

		Object.defineProperties(this, {
			emitter: {
				value: emitter
			},
			client: {
				value: client
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

	createEntity(entity, options = {}) {
		const TableName = this.rootEntityTable;
		const Item = DynamoDB.serializeObject(entity);

		const params = {
			TableName,
			Item,
			ExpressionAttributeNames: {'#id': '_id'},
			ConditionExpression: 'attribute_not_exists(#id)'
		};

		return this.client.requestWithBackoff('PutItem', params, options).then(() => {
			return {entity: clone(entity)};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The '${TableName}' DynamoDB table does not exist.`
				));
			}
			if (err.code === 'ConditionalCheckFailedException') {
				return Promise.reject(new ConflictError(
					`Item '${entity._type}':'${entity._id}' already exists.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#createEntity()',
				err
			));
		});
	}

	updateEntity(entity, options = {}) {
		const TableName = this.rootEntityTable;
		const Item = DynamoDB.serializeObject(entity);

		const params = {
			TableName,
			Item,
			ExpressionAttributeNames: {'#id': '_id'},
			ExpressionAttributeValues: {':id': Item._id},
			ConditionExpression: '#id = :id'
		};

		return this.client.requestWithBackoff('PutItem', params, options).then(() => {
			return {entity: clone(entity)};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The '${TableName}' DynamoDB table does not exist.`
				));
			}
			if (err.code === 'ConditionalCheckFailedException') {
				return Promise.reject(new NotFoundError(
					`Item '${entity._type}':'${entity._id}' does not exist for update.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#updateEntity()',
				err
			));
		});
	}

	getEntity(key, options = {}) {
		const TableName = this.rootEntityTable;

		const Key = {
			_id: {S: key._id},
			_scope_type_key: {S: key._scope_type_key}
		};

		const params = {
			TableName,
			Key
		};

		return this.client.requestWithBackoff('GetItem', params, options).then((res) => {
			const entity = res.Item ? DynamoDB.deserializeObject(res.Item) : null;
			return {entity};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The '${TableName}' DynamoDB table does not exist.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#getEntity()',
				err
			));
		});
	}

	deleteEntity(key, options = {}) {
		const TableName = this.rootEntityTable;

		const Key = {
			_id: {S: key._id},
			_scope_type_key: {S: key._scope_type_key}
		};

		const params = {
			TableName,
			Key
		};

		return this.client.requestWithBackoff('DeleteItem', params, options).then(() => {
			return true;
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The '${TableName}' DynamoDB table does not exist.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#deleteEntity()',
				err
			));
		});
	}

	getIndexEntries(key, options = {}) {
		const TableName = this.indexEntriesTable;

		const ExpressionAttributeNames = {
			'#sk': '_subject_key'
		};

		const ExpressionAttributeValues = {
			':sk': {S: key._subject_key}
		};

		const KeyConditionExpression = '#sk = :sk';

		const getPage = (entries, ExclusiveStartKey) => {
			const params = {
				TableName,
				ExpressionAttributeNames,
				ExpressionAttributeValues,
				ExclusiveStartKey,
				KeyConditionExpression
			};

			return this.client.requestWithBackoff('Query', params, options).then((res) => {
				const thisEntries = res.Items.map((entry) => {
					return entry ? DynamoDB.deserializeObject(entry) : null;
				});

				if (res.LastEvaluatedKey) {
					return getPage(entries.concat(thisEntries), res.LastEvaluatedKey);
				}

				return {entries: entries.concat(thisEntries)};
			});
		};

		return getPage([], null).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The '${TableName}' DynamoDB table does not exist.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#getIndexEntries()',
				err
			));
		});
	}

	updateIndexEntries(toRemove, toCreate, options = {}) {
		const TableName = this.indexEntriesTable;

		const removeRequests = toRemove.map((entry) => {
			return {DeleteRequest: {
				Key: {
					_subject_key: {S: entry._subject_key},
					_unique_key: {S: entry._unique_key}
				}
			}};
		});

		const createRequests = toCreate.map((entry) => {
			return {PutRequest: {
				Item: DynamoDB.serializeObject(entry)
			}};
		});

		const requests = removeRequests.concat(createRequests);

		const setChunk = (requests) => {
			const RequestItems = {};
			RequestItems[TableName] = requests;

			const params = {RequestItems};

			return this.client.batchWriteWithBackoff('BatchWriteItem', params, options);
		};

		const chunks = splitEvery(25, requests);

		const promise = chunks.reduce((promise, chunk) => {
			return promise.then(() => {
				return setChunk(chunk);
			});
		}, Promise.resolve(null));

		return promise.then(() => {
			return true;
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The '${TableName}' DynamoDB table does not exist.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#updateIndexEntries()',
				err
			));
		});
	}

	batchSetEntities(entities, options = {}) {
		const TableName = this.rootEntityTable;

		const setChunk = (entities) => {
			const RequestItems = {};
			RequestItems[TableName] = entities.map((entity) => {
				return {PutRequest: {
					Item: DynamoDB.serializeObject(entity)
				}};
			});

			const params = {RequestItems};

			return this.client.batchWriteWithBackoff('BatchWriteItem', params, options);
		};

		const chunks = splitEvery(25, entities);

		const promise = chunks.reduce((promise, chunk) => {
			return promise.then(() => {
				return setChunk(chunk);
			});
		}, Promise.resolve(null));

		return promise.then(() => {
			return {entities: clone(entities)};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The '${TableName}' DynamoDB table does not exist.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#batchSetEntities()',
				err
			));
		});
	}

	batchGetEntities(keys, options = {}) {
		const TableName = this.rootEntityTable;
		const RequestItems = {};

		const keysHash = keys.reduce((hash, key) => {
			const {_scope_type_key, _id} = key;
			hash[`${_scope_type_key}:${_id}`] = key;
			return hash;
		}, {});

		const Keys = Object.keys(keysHash).map((k) => {
			const key = keysHash[k];
			return {
				_id: {S: key._id},
				_scope_type_key: {S: key._scope_type_key}
			};
		});

		RequestItems[TableName] = {Keys};

		const params = {RequestItems};

		return this.client.batchGetWithBackoff('BatchGetItem', params, options).then((res) => {
			const items = res.Responses[TableName];

			const hash = items.reduce((hash, item) => {
				const entity = item ? DynamoDB.deserializeObject(item) : null;
				const {_scope_type_key, _id} = entity;
				hash[`${_scope_type_key}:${_id}`] = entity;
				return hash;
			}, {});

			const entities = keys.map((key) => {
				const {_scope_type_key, _id} = key;
				return hash[`${_scope_type_key}:${_id}`];
			});

			return {entities};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The '${TableName}' DynamoDB table does not exist.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#batchGetEntities()',
				err
			));
		});
	}

	// args.key - scope_type_key String
	// args.cursor - Dynamodb LastEvaluatedKey Object
	// args.limit - Integer
	// args.backward - Boolean. Defaults to `false`.
	scanEntities(args, options = {}) {
		const {key, cursor, limit} = args;

		const TableName = this.rootEntityTable;
		const IndexName = this.entitiesByTypeIndex;
		const ExclusiveStartKey = cursor || null;
		const KeyConditionExpression = '#stk = :stk';

		const ExpressionAttributeNames = {
			'#stk': '_scope_type_key'
		};

		const ExpressionAttributeValues = {
			':stk': {S: key}
		};

		const params = {
			TableName,
			IndexName,
			ExpressionAttributeNames,
			ExpressionAttributeValues,
			ExclusiveStartKey,
			KeyConditionExpression,
			ScanIndexForward: !args.backward
		};

		if (Number.isInteger(limit)) {
			params.Limit = limit;
		}

		return this.client.requestWithBackoff('Query', params, options).then((res) => {
			const entities = res.Items.map((item) => {
				return item ? DynamoDB.deserializeObject(item) : null;
			});

			const cursor = res.LastEvaluatedKey || null;

			return {entities, cursor};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The DynamoDB '${TableName}' table or '${IndexName}' index does not exist.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#scanEntities()',
				err
			));
		});
	}

	queryIndex(args, options = {}) {
		const {key, operator, cursor, limit} = args;

		const TableName = this.indexEntriesTable;
		const IndexName = this.indexLookupIndex;

		const ExpressionAttributeNames = {
			'#index': '_scope_index_name',
			'#value': '_index_key'
		};

		const ExpressionAttributeValues = {
			':index': {S: key._scope_index_name},
			':value': typeof key._index_key === 'string' ? {S: key._index_key} : {N: key._index_key}
		};

		let condition;

		switch (operator) {
		case 'equals':
			condition = '#value = :value';
			break;
		case 'begins_with':
			condition = 'begins_with ( #value, :value )';
			break;
		default:
			throw new Error(`Invalid conditional operator '${operator}'`);
		}

		const KeyConditionExpression = `#index = :index AND ${condition}`;

		const ExclusiveStartKey = cursor || null;

		const params = {
			TableName,
			IndexName,
			ExpressionAttributeNames,
			ExpressionAttributeValues,
			ExclusiveStartKey,
			KeyConditionExpression
		};

		if (Number.isInteger(limit)) {
			params.Limit = limit;
		}

		return this.client.requestWithBackoff('Query', params, options).then((res) => {
			const entities = res.Items.map((item) => {
				return item ? DynamoDB.deserializeObject(item) : null;
			});

			const cursor = res.LastEvaluatedKey || null;

			return {entities, cursor};
		}).catch((err) => {
			if (err.code === 'ResourceNotFoundException') {
				return Promise.reject(new Error(
					`The DynamoDB '${TableName}' table or '${IndexName}' index does not exist.`
				));
			}
			return Promise.reject(new StackedError(
				'Error in DynamoDB#queryIndex()',
				err
			));
		});
	}

	setupSchema(options = {}) {
		const {client, emitter, rootEntityTable, indexEntriesTable} = this;

		const describeRootEntityTable = () => {
			return client.request('DescribeTable', {TableName: rootEntityTable}, options);
		};

		const describeIndexEntriesTable = () => {
			return client.request('DescribeTable', {TableName: indexEntriesTable}, options);
		};

		const pollTableStatus = (TableName) => {
			return client.request('DescribeTable', {TableName}, options).then(({Table}) => {
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
			return client.request('CreateTable', schema, options).then(() => {
				return pollTableStatus(schema.TableName);
			});
		};

		const createIndexEntriesTable = () => {
			const schema = this.createIndexEntriesSchema();
			return client.request('CreateTable', schema, options).then(() => {
				return pollTableStatus(schema.TableName);
			});
		};

		const checkRootEntityTable = (description) => {
			return description;
		};

		const checkIndexEntriesTable = (description) => {
			return description;
		};

		const catchError = (err) => {
			if (err.code === DynamoDbClient.ResourceNotFoundException) {
				return null;
			}
			return Promise.reject(err);
		};

		const descriptions = [
			describeRootEntityTable().catch(catchError),
			describeIndexEntriesTable().catch(catchError)
		];

		return Promise.all(descriptions).then((tables) => {
			let [rootEntityTable, indexEntriesTable] = tables;

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
				.then(() => [rootEntityTable, indexEntriesTable])
				.catch((err) => {
					return Promise.reject(new StackedError(
						`Unexpected DynamoDB exception in DynamoDB.setupSchema()`,
						err
					));
				});
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

	static create(options = {}) {
		const {
			emitter,
			awsRegion,
			awsAccessKey,
			awsSecretKey,
			dynamodbEndpoint,
			operationTimeout,
			backoffMultiplier,
			requestTimeout,
			tablePrefix
		} = options;

		const client = DynamoDbClient.create({
			emitter,
			awsRegion,
			awsAccessKey,
			awsSecretKey,
			dynamodbEndpoint,
			operationTimeout,
			backoffMultiplier,
			requestTimeout
		});

		return new DynamoDB({emitter, client, tablePrefix});
	}
}

module.exports = DynamoDB;

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

