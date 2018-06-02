'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDbClient = require('../lib/dynamodb-client');
const tools = require('./tools');

const debug = tools.debug('check-tables');

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

const tests = [];

const emitter = new EventEmitter();

const client = DynamoDbClient.create({
	emitter,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

tests.push(function listTables() {
	debug(`ListTables`);
	return client.request('ListTables').then(({TableNames}) => {
		const tables = TableNames;
		const testTables = tables.filter((table) => {
			return table.startsWith(tools.TABLE_PREFIX);
		});

		if (testTables.length > 0) {
			testTables.forEach((table) => {
				debug(`The test table "${table}" is still present.`);
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
