/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const Chance = require('chance');
const DynamoDB = require('../lib/dynamodb');
const {range, splitEvery} = require('kixx/library');
const tools = require('./tools');

const debug = tools.debug('attempt-throttled-requests');

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

const chance = new Chance();

const tests = [];

const emitter = new EventEmitter();

const dynamodb = DynamoDB.create({
	emitter,
	tablePrefix: tools.TABLE_PREFIX,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

emitter.on('warning', (ev) => {
	debug(`warning event: ${ev.message}`);
});

const COUNT = 700;


const generateId = (function () {
	let i = 0;
	return function generateId() {
		i += 1;
		return `${chance.guid()}-${i}`;
	};
}());

const entities = range(0, COUNT).map((x, y, z, _) => {
	const scope = 'foo-bar';

	const type = chance.pickone([
		'fooType',
		'barType'
	]);

	return {
		_id: generateId(),
		_scope_type_key: `${scope}:${type}`,
		_updated: new Date().toISOString(),
		undefinedValue: _,
		nullValue: null,
		nanValue: NaN,
		zeroValue: 0,
		integerValue: 1,
		floatValue: 1.25,
		functionValue: function myFunction() {},
		emptyStringValue: '',
		stringValue: 'x',
		dateValue: new Date(),
		booleanTrueValue: true,
		booleanFalseValue: false,
		emptyList: [],
		listOfPrimitives: [_, null, NaN, 0, 1, 1.25, function myFunction() {}, '', 'x', new Date(), true, false, []],
		listOfLists: [
			[_, null, NaN, 0, 1, 1.25, function myFunction() {}, '', 'x', new Date(), true, false, []]
		],
		listOfHashes: [{
			undefinedValue: _,
			nullValue: null,
			nanValue: NaN,
			zeroValue: 0,
			integerValue: 1,
			floatValue: 1.25,
			functionValue: function myFunction() {},
			emptyStringValue: '',
			stringValue: 'x',
			dateValue: new Date(),
			booleanTrueValue: true,
			booleanFalseValue: false
		}],
		hash: {
			undefinedValue: _,
			nullValue: null,
			nanValue: NaN,
			zeroValue: 0,
			integerValue: 1,
			floatValue: 1.25,
			functionValue: function myFunction() {},
			emptyStringValue: '',
			stringValue: 'x',
			dateValue: new Date(),
			booleanTrueValue: true,
			booleanFalseValue: false
		},
		hashOfLists: {
			A: [_, null, NaN, 0, 1, 1.25, function myFunction() {}, '', 'x', new Date(), true, false, []]
		},
		hashOfHashes: {
			A: {
				undefinedValue: _,
				nullValue: null,
				nanValue: NaN,
				zeroValue: 0,
				integerValue: 1,
				floatValue: 1.25,
				functionValue: function myFunction() {},
				emptyStringValue: '',
				stringValue: 'x',
				dateValue: new Date(),
				booleanTrueValue: true,
				booleanFalseValue: false
			}
		}
	};
});

tests.push(function with_setAndGet() {
	debug(`set and get ${COUNT} entities`);

	const batches = splitEvery(50, entities);

	const put = batches.reduce((promise, batch, i) => {
		debug(`writing batch ${i + 1} of ${batches.length}`);
		return promise.then(() => {
			return Promise.all(batch.map((entity) => {
				return dynamodb.setEntity(entity);
			}));
		});
	}, Promise.resolve(null));

	return put.then(() => {

		// 1.5x number of batches to try to get throughput exceptions.
		const fetchBatches = batches.concat(batches.slice(0, Math.round(batches.length / 2)));

		return fetchBatches.reduce((promise, batch, i) => {
			debug(`fetching batch ${i + 1} of ${batches.length}`);

			return promise.then(() => {
				return Promise.all(batch.map((entity, n) => {
					const key = {_id: entity._id, _scope_type_key: entity._scope_type_key};

					if (n % 2 === 0) {
						return dynamodb.getEntity(key);
					}

					return dynamodb.getEntity(key, {
						ExpressionAttributeNames: {'#id': '_id', '#key': '_scope_type_key'},
						ProjectionExpression: '#id, #key'
					});
				}));
			});
		}, Promise.resolve(null));
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
