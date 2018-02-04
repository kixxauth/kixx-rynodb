'use strict';

const ddb = require(`../../lib/dynamodb`);
const DynamoDB = require(`../test-support/mocks/dynamodb`);
const {assert} = require(`kixx/library`);
const sinon = require(`sinon`);
const ProvisionedThroughputExceededException = require(`../test-support/mocks/provisioned-throughput-exceeded-exception`);
const ResourceNotFoundException = require(`../test-support/mocks/resource-not-found-exception`);

module.exports = (t) => {
	const prefix = `test`;
	const SCOPE = `SCOPEX`;
	const TABLE_NAME = `${prefix}_entities_master`;
	const INDEX_NAME = `${prefix}_entities_by_type`;

	t.describe(`simple use case`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.spy(dynamodb, `query`);

		// Create our curried scanQuery function.
		const dynamodbScan = ddb.scanQuery(dynamodb, {prefix});

		// Input parameter.
		const type = `foo`;

		let RESULT = null;

		t.before((done) => {
			return dynamodbScan(null, SCOPE, type).then((res) => {
				RESULT = res;
				return done();
			}).catch(done);
		});

		t.it(`returns result objects as result data`, (t) => {
			assert.isEqual(10, RESULT.data.length, `data.length`);
			RESULT.data.forEach((obj) => {
				assert.isEqual(`foo`, obj.type, `type prop`);
				assert.isNonEmptyString(obj.id, `id prop`);
				assert.isNonEmptyString(obj.attributes.title, `title.attribute`);
			});
		});

		t.it(`passes through the native database cursor`, (t) => {
			const cursor = RESULT.cursor;
			assert.isEqual(`foo`, cursor.attr1.S, `cursor.attr1`);
			assert.isEqual(`bar`, cursor.attr2.S, `cursor.attr2`);
		});

		t.it(`passes through the native database meta`, (t) => {
			const meta = RESULT.meta;
			assert.isEqual(10, meta.Count, `meta.Count`);
			assert.isEqual(10, meta.ScannedCount, `meta.ScannedCount`);
		});

		t.it(`calls DynamoDB#query()`, () => {
			assert.isOk(dynamodb.query.calledOnce, `query() called once`);
		});

		t.it(`sends correct table and index name in params`, () => {
			const params = dynamodb.query.args[0][0];
			assert.isEqual(TABLE_NAME, params.TableName, `TableName`);
			assert.isEqual(INDEX_NAME, params.IndexName, `IndexName`);
		});

		t.it(`uses scope_type_key as primary index hash key`, () => {
			const params = dynamodb.query.args[0][0];
			assert.isEqual(`${SCOPE}:${type}`, params.ExpressionAttributeValues[`:key`].S, `ExpressionAttributeValues`);
			assert.isEqual(`scope_type_key = :key`, params.KeyConditionExpression, `ExpressionAttributeValues`);
		});

		t.it(`sends default Limit: 10`, () => {
			const params = dynamodb.query.args[0][0];
			assert.isEqual(10, params.Limit, `Limit`);
		});

		t.it(`sends default ExclusiveStartKey: null`, () => {
			const params = dynamodb.query.args[0][0];
			assert.isEqual(null, params.ExclusiveStartKey, `Limit`);
		});
	});

	t.describe(`always a throughput error`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `query`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried scanQuery function.
		const dynamodbScan = ddb.scanQuery(dynamodb, {
			prefix,
			backoffMultiplier: 10
		});

		// Input parameter.
		const type = `foo`;

		let ERROR = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			return dynamodbScan(null, SCOPE, type).then((res) => {
				return done(new Error(`should not resolve`));
			}).catch((err) => {
				ERROR = err;
				ELAPSED = Date.now() - START;
				return done();
			});
		});

		t.it(`rejects with a StackedError`, () => {
			assert.isEqual(`StackedError`, ERROR.name);
			assert.isEqual(`ProvisionedThroughputExceededException`, ERROR.errors[0].name);
		});

		t.it(`calls DynamoDB#query() 5 times`, () => {
			assert.isEqual(5, dynamodb.query.callCount, `query() calls`);

			dynamodb.query.args.forEach((args) => {
				const params = args[0];

				assert.isEqual(TABLE_NAME, params.TableName, `TableName`);
				assert.isEqual(INDEX_NAME, params.IndexName, `IndexName`);
				assert.isEqual(`${SCOPE}:${type}`, params.ExpressionAttributeValues[`:key`].S, `ExpressionAttributeValues`);
				assert.isEqual(`scope_type_key = :key`, params.KeyConditionExpression, `ExpressionAttributeValues`);
				assert.isEqual(10, params.Limit, `Limit`);
				assert.isEqual(null, params.ExclusiveStartKey, `Limit`);
			});
		});

		// The log backoff formula defined in dynamodb.js computeBackoffTime() that gives us:
		// `600 === [2,3,4,5].reduce((n, i) => n + Math.pow(2, i) * 10, 0)`
		t.it(`consumes more than 600ms for 5 retries`, () => {
			assert.isGreaterThan(600, ELAPSED, `elapsed time`);
		});
	});

	t.describe(`with throughput error in first call only`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		let count = 0;

		// Input parameter.
		const type = `foo`;

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `query`).callsFake((params, callback) => {
			// Increment the counter.
			count += 1;

			// Make the callback async.
			process.nextTick(() => {
				if (count < 2) {
					// Raise the throughput error if this is the first try.
					callback(new ProvisionedThroughputExceededException(`TEST`));
				}

				const Items = [{
					type: {S: type},
					id: {S: `foo-bar-baz`},
					attributes: {M: {
						title: {S: `Jaws`}
					}}
				}];

				callback(null, {
					Items,
					Count: Items.length,
					ScannedCount: Items.length,
					LastEvaluatedKey: {attr1: {S: `foo`}, attr2: {S: `bar`}}
				});
			});
		});

		// Create our curried scanQuery function.
		const dynamodbScan = ddb.scanQuery(dynamodb, {
			prefix
		});

		let RESULT = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			// Override the backoffMultiplier
			return dynamodbScan({backoffMultiplier: 100}, SCOPE, type).then((res) => {
				RESULT = res;
				ELAPSED = Date.now() - START;
				return done();
			}).catch(done);
		});

		t.it(`calls DynamoDB#query() correct number of times`, () => {
			assert.isOk(dynamodb.query.calledTwice, `query() called twice`);
		});

		t.it(`sends the correct parameters each time`, () => {
			dynamodb.query.args.forEach((args) => {
				const params = args[0];

				assert.isEqual(TABLE_NAME, params.TableName, `TableName`);
				assert.isEqual(INDEX_NAME, params.IndexName, `IndexName`);
				assert.isEqual(`${SCOPE}:${type}`, params.ExpressionAttributeValues[`:key`].S, `ExpressionAttributeValues`);
				assert.isEqual(`scope_type_key = :key`, params.KeyConditionExpression, `ExpressionAttributeValues`);
				assert.isEqual(10, params.Limit, `Limit`);
				assert.isEqual(null, params.ExclusiveStartKey, `Limit`);
			});
		});

		// The log backoff formula defined in dynamodb.js computeBackoffTime() that gives us:
		// `400 === Math.pow(2, 0 + 2) * 100`
		t.it(`consumes more than 400ms for 1 retry`, () => {
			assert.isGreaterThan(400, ELAPSED, `elapsed time`);
		});

		t.it(`has expected response`, () => {
			assert.isEqual(1, RESULT.data.length, `data.length`);

			RESULT.data.forEach((obj) => {
				assert.isEqual(`foo`, obj.type, `type prop`);
				assert.isNonEmptyString(obj.id, `id prop`);
				assert.isNonEmptyString(obj.attributes.title, `title.attribute`);
			});
		});
	});

	t.describe(`with retryLimit set in initial options`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `query`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried batchGet function.
		const dynamodbScan = ddb.scanQuery(dynamodb, {
			prefix,
			backoffMultiplier: 1,
			retryLimit: 2
		});

		// Input parameter.
		const type = `foo`;

		t.before((done) => {
			return dynamodbScan(null, SCOPE, type).then((res) => {
				return done(new Error(`should not resolve`));
			}).catch(() => {
				return done();
			});
		});

		t.it(`calls DynamoDB#query() 2 times`, () => {
			assert.isEqual(2, dynamodb.query.callCount, `query() calls`);
		});
	});

	t.describe(`with retryLimit set in call options`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `query`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried query function.
		const dynamodbScan = ddb.scanQuery(dynamodb, {
			prefix,
			backoffMultiplier: 10000,
			retryLimit: 10
		});

		// Input parameter.
		const type = `foo`;

		t.before((done) => {
			const opts = {
				backoffMultiplier: 1,
				retryLimit: 2
			};

			return dynamodbScan(opts, SCOPE, type).then((res) => {
				return done(new Error(`should not resolve`));
			}).catch(() => {
				return done();
			});
		});

		t.it(`calls DynamoDB#query() 2 times`, () => {
			assert.isEqual(2, dynamodb.query.callCount, `query() calls`);
		});
	});

	t.describe(`with ResourceNotFoundException`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Stub the DynamoDB method.
		sinon.stub(dynamodb, `query`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ResourceNotFoundException(`TEST`));
			});
		});

		// Create our curried query function.
		const dynamodbScan = ddb.scanQuery(dynamodb, {prefix});

		// Input parameter.
		const type = `foo`;

		let ERROR = null;

		t.before((done) => {
			return dynamodbScan(null, SCOPE, type).then((res) => {
				return done(new Error(`should not resolve`));
			}).catch((err) => {
				ERROR = err;
				return done();
			});
		});

		t.it(`rejects with a StackedError`, () => {
			assert.isEqual(`StackedError`, ERROR.name, `error name`);
			assert.isEqual(`ResourceNotFoundException`, ERROR.errors[0].name, `root error name`);
			assert.isEqual(`Missing DynamoDB table "${TABLE_NAME}" or index "${INDEX_NAME}": TEST`, ERROR.message, `error message`);
		});
	});
};
