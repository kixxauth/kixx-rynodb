/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDB = require('../lib/dynamodb');
const {assert} = require('kixx/library');
const tools = require('./tools');

const debug = tools.debug('missing-dynamodb-table');

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

const tests = [];

const emitter = new EventEmitter();

const dynamodb = DynamoDB.create({
	emitter,
	tablePrefix: tools.TABLE_PREFIX,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

tests.push(function with_setEntity() {
	debug(`setEntity()`);
	return dynamodb.setEntity().catch((err) => {
		assert.isEqual('MISSING_TABLE', err.code);
		assert.isEqual(`The DynamoDB table 'ttt_root_entities' does not exist.`, err.message);
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