/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDB = require('../lib/dynamodb');
const DynamoDbClient = require('../lib/dynamodb-client');
const {assert, isObject, range} = require('kixx/library');
const tools = require('./tools');

const debug = tools.debug('attempt-throttled-batch-requests');
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

tests.push(function testBatchWriteItem() {
	debug('test BatchWriteItem');

	const makeRequest = () => {
		debug('BatchWriteItem makeRequest');

		const ttt_root_entities = range(0, 25).map(() => {
			const entity = createTestRecord(SCOPE, TYPE);
			keys.push({_id: entity._id, _scope_type_key: entity._scope_type_key});

			const Item = DynamoDB.serializeObject(entity);

			return {PutRequest: {Item}};
		});

		const params = {
			RequestItems: {ttt_root_entities}
		};

		return client.request('BatchWriteItem', params).then((res) => {
			const {UnprocessedItems} = res;

			// UnprocessedItems is *always* present, but may be empty.
			assert.isOk(isObject(UnprocessedItems));

			if (UnprocessedItems.ttt_root_entities) {
				debug('BatchWriteItem got UnprocessedItems');
				const {ttt_root_entities} = UnprocessedItems;

				assert.isOk(Array.isArray(ttt_root_entities));

				ttt_root_entities.forEach((req) => {
					const {PutRequest} = req;
					assert.isOk(isObject(PutRequest));
				});
			}

			return makeRequest();
		}, (err) => {
			// After enough tries, even with UprocessedItems, there will eventually
			// be a ProvisionedThroughputExceededException.
			debug('BatchWriteItem got ProvisionedThroughputExceededException');
			assert.isEqual('ProvisionedThroughputExceededException', err.name);
			assert.isEqual('ProvisionedThroughputExceededException', err.code);
			return null;
		});
	};

	return makeRequest();
});

tests.push(function testBatchGetItem() {
	debug('test BatchGetItem');

	const makeRequest = () => {
		debug('BatchGetItem makeRequest');

		const Keys = range(0, 25).map((i) => {
			const key = keys[i];
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

		return client.request('BatchGetItem', params).then((res) => {
			const {UnprocessedKeys} = res;

			// UnprocessedKeys is *always* present, but may be empty.
			assert.isOk(isObject(UnprocessedKeys));

			// UnprocessedKeys is rare. A ProvisionedThroughputExceededException is
			// much more likely.
			if (UnprocessedKeys.ttt_root_entities) {
				debug('BatchGetItem got UnprocessedKeys');
				const {ttt_root_entities} = UnprocessedKeys;

				assert.isOk(isObject(ttt_root_entities));
			}

			// Make another request, trying to get to ProvisionedThroughputExceededException.
			return makeRequest();
		}, (err) => {
			// After enough tries, even with UprocessedItems, there will eventually
			// be a ProvisionedThroughputExceededException.
			debug('BatchGetItem got ProvisionedThroughputExceededException');
			assert.isEqual('ProvisionedThroughputExceededException', err.name);
			assert.isEqual('ProvisionedThroughputExceededException', err.code);
			return null;
		});
	};

	return makeRequest();
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
