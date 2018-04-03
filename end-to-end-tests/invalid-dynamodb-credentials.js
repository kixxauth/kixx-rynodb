/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const DynamoDbClient = require('../lib/dynamodb-client');
const {assert} = require('kixx/library');
const tools = require('./tools');

const debug = tools.debug('invalid-dynamodb-credentials');
const {targets} = tools;

const emitter = new EventEmitter();

const client = DynamoDbClient.create({
	emitter,
	awsRegion: 'us-east-1',
	awsAccessKey: 'foo',
	awsSecretKey: 'bar'
});

const tests = targets.map(function (target) {
	return function () {
		debug(`target ${target}`);

		return client.request(target, {}).catch(function (err) {
			assert.isEqual('UnrecognizedClientException', err.name);
			assert.isEqual('UnrecognizedClientException', err.code);
			assert.isEqual('The security token included in the request is invalid.', err.message);
			return null;
		});
	};
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
