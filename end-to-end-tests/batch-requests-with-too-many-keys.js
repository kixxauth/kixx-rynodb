/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDB = require('../lib/dynamodb');
const DynamoDbClient = require('../lib/dynamodb-client');
const {assert, range} = require('kixx/library');
const tools = require('./tools');

const debug = tools.debug('batch-requests-with-too-many-keys');
const {createTestRecord} = tools;

const SCOPE = 'foo-bar';
const TYPE = 'foobar';

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

const emitter = new EventEmitter();

const client = DynamoDbClient.create({
	emitter,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

const tests = [];

const keys = [];

tests.push(function waitForThroughput() {
	const seconds = 60;
	debug(`wating ${seconds} seconds for throughput to recover`);
	return Promise.delay(seconds * 1000);
});

tests.push(function testBatchWriteItem() {
	debug('test BatchWriteItem');

	const entities = range(0, 26).map(() => {
		return createTestRecord(SCOPE, TYPE);
	});

	const ttt_root_entities = entities.map((entity) => {
		const Item = DynamoDB.serializeObject(entity);
		return {PutRequest: {Item}};
	});

	const params = {
		RequestItems: {ttt_root_entities}
	};

	return client.request('BatchWriteItem', params).then(() => {
		assert.isOk(false, 'should not be called');
		return false;
	}, (err) => {
		debug('BatchWriteItem got error');
		assert.isEqual('ValidationException', err.name);
		assert.isEqual('ValidationException', err.code);
		assert.isMatch(/Member must have length less than or equal to 25/, err.message);
		return null;
	});
});

tests.push(function createItems() {
	debug('create items with BatchWriteItem');

	const entities = range(0, 25).map(() => {
		return createTestRecord(SCOPE, TYPE);
	});

	entities.forEach((entity) => {
		keys.push({_id: entity._id, _scope_type_key: entity._scope_type_key});
	});

	const ttt_root_entities = entities.map((entity) => {
		const Item = DynamoDB.serializeObject(entity);
		return {PutRequest: {Item}};
	});

	const params = {
		RequestItems: {ttt_root_entities}
	};

	return client.request('BatchWriteItem', params).catch((err) => {
		assert.isOk(false, 'should not be called');
		return null;
	});
});

tests.push(function testBatchGetItem() {
	debug('test BatchGetItem');

	range(0, 5).forEach(() => {
		const entity = createTestRecord(SCOPE, TYPE);
		keys.push({_id: entity._id, _scope_type_key: entity._scope_type_key});
	});

	const Keys = keys.map((key) => {
		return {
			_id: {S: key._id},
			_scope_type_key: {S: key._scope_type_key}
		};
	});

	const params = {
		RequestItems: {
			ttt_root_entities: {Keys}
		}
	};

	// Can request more than 25 items at a time.
	return client.request('BatchGetItem', params).then((res) => {
		debug('BatchGetItem got response');

		// We only get the items that were created.
		assert.isEqual(25, res.Responses.ttt_root_entities.length);
		return null;
	}, (err) => {
		assert.isOk(false, 'should not be called');
		return null;
	});
});

exports.main = function main() {
	return tests.reduce((promise, test) => {
		return promise.then(() => test());
	}, Promise.resolve(null));
};

if (require.main === module) {
	exports.main().then(() => {
		console.log('Done :-)');
		return null;
	}).catch((err) => {
		console.error('Runtime Error:');
		console.error(err.stack);
	});
}
