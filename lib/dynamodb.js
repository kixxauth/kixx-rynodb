'use strict';

const Promise = require('bluebird');
const {StackedError} = require('kixx');
const {clone, compact, splitAt} = require('kixx/library');

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

	// TODO: Handle ResourceNotFoundException in public methods.

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
		});
	}

	setEntity(entity, options = {}) {
		const TableName = this.rootEntityTable;
		const Item = DynamoDB.serializeObject(entity);

		const params = {
			TableName,
			Item
		};

		return this.client.requestWithBackoff('PutItem', params, options).then(() => {
			return {entity: clone(entity)};
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

		const chunks = splitAt(25, entities).filter((chunk) => chunk.length > 0);

		const promise = chunks.reduce((promise, chunk) => {
			return promise.then(() => {
				return setChunk(chunk);
			});
		}, Promise.resolve(null));

		return promise.then(() => {
			return {entities: clone(entities)};
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
		});
	}

	// args.key - scope_type_key String
	// args.cursor - Dynamodb LastEvaluatedKey Object
	// args.limit - Integer
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
		});
	}

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

