/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDB = require('../lib/dynamodb');
const tools = require('./tools');

const debug = tools.debug('check-tables');

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

debug(`AWS region: ${awsRegion}`);
debug(`AWS access key id: ${awsAccessKey}`);
debug(`AWS secret key: ${awsSecretKey}`);

const tests = [];

const emitter = new EventEmitter();

const dynamodb = DynamoDB.create({
	emitter,
	tablePrefix: tools.TABLE_PREFIX,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

tests.push(function listTables() {
	debug(`listTables()`);
	return dynamodb.listTables().then(({tables}) => {
		const testTables = tables.filter((table) => {
			return table.startsWith(tools.TABLE_PREFIX);
		});

		if (testTables.length > 0) {
			testTables.forEach((table) => {
				console.log(`The test table "${table}" is still present.`);
			});
			throw new Error(`Test tables must be deleted before continuing tests.`);
		}

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
