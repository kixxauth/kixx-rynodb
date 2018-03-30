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

let existingMessage = false;

emitter.on('warning', (ev) => {
	const {message} = ev;
	if (message === existingMessage) return;
	existingMessage = message;
	debug(`warning event (may be many more): ${message}`);
});

const COUNT = 1000;
const SCOPE = 'foo-bar';
const TYPE = 'foobar';


const generateId = (function () {
	let i = 0;
	return function generateId() {
		i += 1;
		return `${chance.guid()}-${i}`;
	};
}());

const entities = range(0, COUNT).map((x, y, z, _) => {

	return {
		_id: generateId(),
		_scope_type_key: `${SCOPE}:${TYPE}`,
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

const batches = splitEvery(50, entities);

tests.push(function with_setEntity() {
	debug(`set ${COUNT} entities`);

	return batches.reduce((promise, batch, i) => {
		return promise.then(() => {
			debug(`writing batch ${i + 1} of ${batches.length}`);
			return Promise.all(batch.map((entity) => {
				return dynamodb.setEntity(entity, {operationTimeout: 1500});
			}));
		});
	}, Promise.resolve(null));
});

tests.push(function with_getEntity() {
	debug(`get ${COUNT} entities`);

	// 1.3x number of batches to try to get throughput exceptions.
	const fetchBatches = batches.concat(batches.slice(0, Math.round(batches.length * 0.3)));

	return fetchBatches.reduce((promise, batch, i) => {
		return promise.then(() => {
			debug(`fetching batch ${i + 1} of ${fetchBatches.length}`);

			return Promise.all(batch.map((entity, n) => {
				const key = {_id: entity._id, _scope_type_key: entity._scope_type_key};

				if (n % 2 === 0) {
					return dynamodb.getEntity(key, {operationTimeout: 1500});
				}

				return dynamodb.getEntity(key, {
					ExpressionAttributeNames: {'#id': '_id', '#key': '_scope_type_key'},
					ProjectionExpression: '#id, #key',
					operationTimeout: 5000
				});
			}));
		});
	}, Promise.resolve(null));
});

tests.push(function with_scanByType() {
	debug(`scan entities by type`);

	const key = `${SCOPE}:${TYPE}`;

	const attempts = range(0, 10);

	function consumeAsManyAsPossible() {
		return dynamodb.scanByType({key}, {operationTimeout: 4000}).then((res) => {
			debug(`scanByType() got ${res.Items.length} results`);
		});
	}

	function consumeAll() {
		function getPage(ExclusiveStartKey, i) {
			const params = {
				key,
				Limit: 100,
				ExclusiveStartKey
			};

			return dynamodb.scanByType(params, {operationTimeout: 1000}).then((res) => {
				debug(`scanByType() got page ${i}`);
				if (res.LastEvaluatedKey) {
					return getPage(res.LastEvaluatedKey, i + 1);
				}
				return null;
			});
		}

		return getPage(null, 0);
	}

	return attempts.reduce((promise, i) => {
		return promise.then(() => {
			if (i % 2 === 0) return consumeAsManyAsPossible();
			return consumeAll;
		});
	}, Promise.resolve(null));
});

tests.push(function with_removeEntity() {
	debug(`get ${COUNT} entities`);

	return batches.reduce((promise, batch, i) => {
		return promise.then(() => {
			debug(`deleting batch ${i + 1} of ${batches.length}`);

			return Promise.all(batch.map((entity, n) => {
				const key = {_id: entity._id, _scope_type_key: entity._scope_type_key};
				return dynamodb.removeEntity(key, {operationTimeout: 1500});
			}));
		});
	}, Promise.resolve(null));
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
