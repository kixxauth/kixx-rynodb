/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDB = require('../lib/dynamodb');
const tools = require('./tools');

const debug = tools.debug('setup-schema');

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

debug(`AWS region: ${awsRegion}`);
debug(`AWS access key id: ${awsAccessKey}`);
debug(`AWS secret key: ${awsSecretKey}`);

const tests = [];

const emitter = new EventEmitter();

emitter.on('info', (ev) => {
	debug(ev.message);
});

const dynamodb = DynamoDB.create({
	emitter,
	tablePrefix: tools.TABLE_PREFIX,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

tests.push(function listTables() {
	debug(`setupSchema()`);
	return dynamodb.setupSchema();
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
