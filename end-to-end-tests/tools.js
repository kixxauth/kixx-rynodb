/* eslint-disable no-process-env */
'use strict';

const {uuidV1} = require('kixx');
const {isNonEmptyString} = require('kixx/library');
const debug = require('debug');

exports.TABLE_PREFIX = 'ttt';

exports.targets = Object.freeze([
	'BatchGetItem',
	'BatchWriteItem',
	'CreateTable',
	'DeleteItem',
	'DescribeTable',
	'GetItem',
	'ListTables',
	'PutItem',
	'Query',
	'Scan'
]);

exports.tableTargets = Object.freeze([
	'BatchGetItem',
	'BatchWriteItem',
	'DeleteItem',
	'GetItem',
	'PutItem',
	'Query',
	'Scan'
]);

exports.debug = function (name) {
	return debug(`kixx-rynodb:end-to-end:${name}`);
};

exports.getAwsCredentials = () => {
	const awsAccessKey = process.env.AWS_ACCESS_KEY_ID;
	const awsSecretKey = process.env.AWS_SECRET_ACCESS_KEY;
	const awsRegion = process.env.AWS_REGION;

	if (!isNonEmptyString(awsAccessKey)) {
		throw new Error('process.env.AWS_ACCESS_KEY_ID is required');
	}
	if (!isNonEmptyString(awsSecretKey)) {
		throw new Error('process.env.AWS_SECRET_ACCESS_KEY is required');
	}
	if (!isNonEmptyString(awsRegion)) {
		throw new Error('process.env.AWS_REGION is required');
	}

	return {
		awsAccessKey,
		awsSecretKey,
		awsRegion
	};
};

exports.createTestRecord = function createTestRecord(scope, type, _) {
	return {
		_id: uuidV1(),
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
};
