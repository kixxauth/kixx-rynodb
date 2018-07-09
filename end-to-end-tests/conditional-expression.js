'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDB = require('../lib/dynamodb');
const DynamoDbClient = require('../lib/dynamodb-client');
const {reportFullStackTrace} = require('kixx');
const {assert} = require('kixx/library');
const tools = require('./tools');

const debug = tools.debug('attempt-throttled-batch-requests');
const {TABLE_PREFIX, createTestRecord} = tools;

const SCOPE = 'foo-bar';
const TYPE = 'foobar';

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

const TableName = `${TABLE_PREFIX}_root_entities`;

const emitter = new EventEmitter();

const client = DynamoDbClient.create({
	emitter,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

const entity = createTestRecord(SCOPE, TYPE);

const tests = [];

tests.push(function testUpdateItem() {
	debug('test update item');

	const Item = DynamoDB.serializeObject(entity);

	const params = {
		TableName,
		Item,
		ExpressionAttributeNames: {'#id': '_id'},
		ExpressionAttributeValues: {':id': Item._id},
		ConditionExpression: '#id = :id',
		ReturnConsumedCapacity: 'INDEXES'
	};

	return client.request('PutItem', params).then((res) => {
		console.log('test update item response', res);
	}).catch((err) => {
		console.log('test update item error', {
			name: err.name,
			code: err.code,
			message: err.message
		});
	});
});

tests.push(function testCreateItem() {
	debug('test create item');

	const Item = DynamoDB.serializeObject(entity);

	const params = {
		TableName,
		Item
	};

	return client.request('PutItem', params).then((res) => {
		const params = {
			TableName,
			Item,
			ExpressionAttributeNames: {'#id': '_id'},
			ConditionExpression: 'attribute_not_exists(#id)',
			ReturnConsumedCapacity: 'INDEXES'
		};

		return client.request('PutItem', params).then((res) => {
			console.log('test create item response', res);
		}).catch((err) => {
			console.log('test create item error', {
				name: err.name,
				code: err.code,
				message: err.message
			});
		});
	});
});

exports.main = function main() {
	return tests.reduce((promise, test) => {
		return promise.then(() => test());
	}, Promise.resolve(null));
};

/* eslint-disable no-console */
if (require.main === module) {
	exports.main().then(() => {
		console.log('Done :-)');
		return null;
	}).catch((err) => {
		console.error('Runtime Error:');
		reportFullStackTrace(err);
	});
}
/* eslint-enable */
