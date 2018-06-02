'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDbClient = require('../lib/dynamodb-client');
const {assert} = require('kixx/library');
const tools = require('./tools');

const debug = tools.debug('missing-dynamodb-table');
const {tableTargets} = tools;

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

const emitter = new EventEmitter();

const client = DynamoDbClient.create({
	emitter,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

const tests = tableTargets
	.map(function (target) {
		switch (target) {
		case 'BatchGetItem':
			return {target, params: {
				RequestItems: {
					'table_does_not_exist': {Keys: [{}]}
				}
			}};
		case 'BatchWriteItem':
			return {target, params: {
				RequestItems: {
					'table_does_not_exist': [{
						DeleteRequest: {
							Key: {}
						}
					}]
				}
			}};
		case 'DeleteItem':
			return {target, params: {
				TableName: 'table_does_not_exist',
				Key: {}
			}};
		case 'GetItem':
			return {target, params: {
				TableName: 'table_does_not_exist',
				Key: {}
			}};
		case 'PutItem':
			return {target, params: {
				TableName: 'table_does_not_exist',
				Item: {}
			}};
		case 'Query':
			return {target, params: {
				TableName: 'table_does_not_exist',
				ExpressionAttributeValues: {':v': {S: 'bar'}},
				KeyConditionExpression: 'foo = :v'
			}};
		case 'Scan':
			return {target, params: {
				TableName: 'table_does_not_exist'
			}};
		default:
			throw new Error(`Unexpected DynamoDbClient target "${target}"`);
		}
	})
	.map(function ({target, params}) {
		return function () {
			debug(`target ${target}`);

			return client.request(target, params).catch(function (err) {
				assert.isEqual('ResourceNotFoundException', err.name);
				assert.isEqual('ResourceNotFoundException', err.code);
				assert.isEqual('Requested resource not found', err.message);
				return null;
			});
		};
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
		console.error(err.stack);
	});
}
/* eslint-enable */
