'use strict';

const EventEmitter = require('events');
const Transaction = require('./lib/transaction');
const DynamoDbClient = require('./lib/dynamodb-client');
const DynamoDb = require('./lib/dynamodb');
const Entity = require('./lib/entity');
const {assert, isNonEmptyString} = require('kixx/library');

exports.DynamoDbClient = DynamoDbClient;
exports.DynamoDb = DynamoDb;
exports.Transaction = Transaction;

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
		tablePrefix,
		awsRegion,
		awsAccessKey,
		awsSecretKey,
		dynamodbEndpoint,
		operationTimeout,
		backoffMultiplier,
		requestTimeout
	} = options;

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
			return new Transaction({dynamodb});
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
		itemsByType(args, options = {}) {
			const {scope, type, cursor, limit} = args;
			assert.isNonEmptyString(scope, 'itemsByType() scope');
			assert.isNonEmptyString(type, 'itemsByType() type');

			const key = Entity.createKey(scope, type)._scope_type_key;

			const params = {key, cursor, limit};

			return dynamodb.scanEntities(params, options).then((res) => {
				return {
					items: res.entities.map((entity) => {
						if (!entity) return null;
						return Entity.fromDatabaseRecord(entity).toPublicItem();
					}),
					cursor: res.cursor
				};
			});
		},

		dynamodb
	});
};
