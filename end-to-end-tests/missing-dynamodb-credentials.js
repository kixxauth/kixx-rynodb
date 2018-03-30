/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDB = require('../lib/dynamodb');
const {assert} = require('kixx/library');
const tools = require('./tools');

const debug = tools.debug('missing-dynamodb-credentials');

const tests = [];

const emitter = new EventEmitter();

const dynamodb = DynamoDB.create({
	emitter,
	tablePrefix: tools.TABLE_PREFIX,
	awsRegion: 'us-east-1'
});

tests.push(function with_setEntity() {
	debug(`setEntity()`);
	return dynamodb.setEntity().catch((err) => {
		assert.isEqual('UnrecognizedClientException', err.code);
		assert.isMatch(/^DynamoDB request error/, err.message);
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
