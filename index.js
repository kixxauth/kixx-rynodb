'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const {StackedError} = require('kixx');
const Transaction = require('./lib/transaction');
const DynamoDbClient = require('./lib/dynamodb-client');
const DynamoDb = require('./lib/dynamodb');
const Entity = require('./lib/entity');
const IndexEntry = require('./lib/index-entry');
const {assert, isNonEmptyString, isNumber, isObject, isUndefined} = require('kixx/library');

exports.DynamoDbClient = DynamoDbClient;
exports.DynamoDb = DynamoDb;
exports.Transaction = Transaction;

// options.indexes
// options.tablePrefix
// options.awsRegion
// options.awsAccessKey
// options.awsSecretKey
// options.dynamodbEndpoint
// options.operationTimeout
// options.backoffMultiplier
// options.requestTimeout
exports.create = function create(options = {}) {
	const emitter = options.emitter || new EventEmitter();

	if (!isNonEmptyString(options.tablePrefix) || !/^[a-zA-Z_]{2,50}$/.test(options.tablePrefix)) {
		throw new Error(`invalid table prefix String`);
	}
	if (!isNonEmptyString(options.awsRegion)) {
		throw new Error(`invalid awsRegion String`);
	}
	if (!isNonEmptyString(options.awsAccessKey)) {
		throw new Error(`invalid awsAccessKey String`);
	}
	if (!isNonEmptyString(options.awsSecretKey)) {
		throw new Error(`invalid awsSecretKey String`);
	}

	const {
		indexes,
		tablePrefix,
		awsRegion,
		awsAccessKey,
		awsSecretKey,
		dynamodbEndpoint,
		operationTimeout,
		backoffMultiplier,
		requestTimeout
	} = options;

	Object.keys(indexes || {}).forEach((type) => {
		const indexNames = Object.keys(indexes[type]);
		for (let i = 0; i < indexNames.length; i++) {
			assert.isEqual('function', typeof indexes[type][indexNames[i]], 'index mapper is not a Function');
		}
	});

	const client = new DynamoDbClient({
		emitter,
		awsRegion,
		awsAccessKey,
		awsSecretKey,
		dynamodbEndpoint,
		operationTimeout,
		backoffMultiplier,
		requestTimeout
	});

	const dynamodb = new DynamoDb({
		emitter,
		tablePrefix,
		client
	});

	return Object.assign(emitter, {
		createTransaction() {
			return new Transaction({dynamodb, indexes});
		},

		setupSchema() {
			return dynamodb.setupSchema();
		},

		batchSetItems(objects, options = {}) {
			assert.isOk(Array.isArray(objects), 'batchSetItems() objects Array');

			const entities = objects.map((object) => {
				const {scope, type, id} = object;
				assert.isNonEmptyString(scope, 'batchSetItems() object.scope');
				assert.isNonEmptyString(type, 'batchSetItems() object.type');
				assert.isNonEmptyString(id, 'batchSetItems() object.id');
				return Entity.fromPublicObject(object);
			});

			return dynamodb.batchSetEntities(entities, options).then((res) => {
				return {
					items: res.entities.map((entity) => {
						return Entity.fromDatabaseRecord(entity).toPublicItem();
					})
				};
			});
		},

		batchGetItems(objects, options = {}) {
			assert.isOk(Array.isArray(objects), 'batchGetItems() objects Array');

			const keys = objects.map((object) => {
				const {scope, type, id} = object;
				assert.isNonEmptyString(scope, 'batchGetItems() object.scope');
				assert.isNonEmptyString(type, 'batchGetItems() object.type');
				assert.isNonEmptyString(id, 'batchGetItems() object.id');
				return Entity.createKey(scope, type, id);
			});

			return dynamodb.batchGetEntities(keys, options).then((res) => {
				return {
					items: res.entities.map((entity) => {
						if (!entity) return null;
						return Entity.fromDatabaseRecord(entity).toPublicItem();
					})
				};
			});
		},

		// args.scope - String
		// args.type - String
		// args.cursor - DynamoDB LastEvaluatedKey Object
		// args.limit - Integer
		// args.backward - Boolean. Defaults to `false`.
		itemsByType(args, options = {}) {
			const {scope, type, cursor, limit, backward} = args;
			assert.isNonEmptyString(scope, 'itemsByType() scope');
			assert.isNonEmptyString(type, 'itemsByType() type');

			const key = Entity.createKey(scope, type)._scope_type_key;

			const params = {key, cursor, limit, backward};

			return dynamodb.scanEntities(params, options)
				.then((res) => {
					return {
						items: res.entities.map((entity) => {
							if (!entity) return null;
							return Entity.fromDatabaseRecord(entity).toPublicItem();
						}),
						cursor: res.cursor
					};
				})
				.catch((err) => {
					return Promise.reject(new StackedError(
						`Error during itemsByType()`,
						err
					));
				});
		},

		// args.scope - String
		// args.index - String
		// args.operator - String "equals", "greater_than", "less_than" or "begins_with"
		// args.value - String or Number
		// args.cursor - DynamoDB LastEvaluatedKey Object
		// args.limit - Integer
		query(args, options = {}) {
			const {scope, index, value, operator, cursor, limit} = args;

			assert.isOk(
				isUndefined(cursor) || cursor === null || isObject(cursor),
				'query() cursor is undefined, null, or Object'
			);

			switch (operator) {
			case 'equals':
				assert.isOk(
					isNonEmptyString(value) || isNumber(value),
					'query() operator "equals" : value String or Number'
				);
				break;
			case 'greater_than':
				assert.isOk(
					isNonEmptyString(value) || isNumber(value),
					'query() operator "greater_than" : value String or Number'
				);
				break;
			case 'less_than':
				assert.isOk(
					isNonEmptyString(value) || isNumber(value),
					'query() operator "greater_than" : value String or Number'
				);
				break;
			case 'begins_with':
				assert.isNonEmptyString(value, 'query() operator "begins_with" : value String');
				break;
			default:
				assert.isOk(false, `query() operator "${operator}"`);
			}

			const key = IndexEntry.queryKey(scope, index, value);

			const params = {key, operator, cursor, limit};

			return dynamodb.queryIndex(params)
				.then((res) => {
					return {
						items: res.entities.map((entity) => {
							if (!entity) return null;
							return Entity.fromDatabaseRecord(entity).toPublicItem();
						}),
						cursor: res.cursor
					};
				})
				.catch((err) => {
					return Promise.reject(new StackedError(
						`Error during query()`,
						err
					));
				});
		},

		dynamodb
	});
};
