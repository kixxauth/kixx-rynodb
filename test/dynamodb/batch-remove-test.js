'use strict';

const ddb = require(`../../lib/dynamodb`);
const DynamoDB = require(`../test-support/mocks/dynamodb`);
const {assert, clone, tail, range} = require(`kixx/library`);
const sinon = require(`sinon`);
const Chance = require(`chance`);
const ProvisionedThroughputExceededException = require(`../test-support/mocks/provisioned-throughput-exceeded-exception`);
const ResourceNotFoundException = require(`../test-support/mocks/resource-not-found-exception`);

const chance = new Chance();

module.exports = (t) => {
	const prefix = `test`;
	const SCOPE = `SCOPEX`;
	const TABLE_NAME = `${prefix}_entities_master`;

	t.describe(`simple use case`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.spy(dynamodb, `batchWriteItem`);

		// Create our curried batchRemove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {prefix});

		// Input parameter.
		const keys = [
			{
				type: `foo`,
				id: `bar`
			},
			{
				type: `foo`,
				id: `baz`
			}
		];

		let RESULT = null;

		t.before((done) => {
			return dynamodbBatchRemoveObjects(null, SCOPE, keys).then((res) => {
				RESULT = res;
				return done();
			}).catch(done);
		});

		t.it(`returns an array of true values`, () => {
			assert.isEqual(keys.length, RESULT.data.length, `same length`);

			RESULT.data.forEach((val) => {
				assert.isEqual(true, val, `value is true`);
			});
		});

		t.it(`calls DynamoDB#batchWriteItem()`, () => {
			assert.isOk(dynamodb.batchWriteItem.calledOnce, `batchWriteItem() called once`);
		});

		t.it(`sends correct table name in params`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			assert.isEqual(TABLE_NAME, Object.keys(params.RequestItems)[0], `TableName`);
			assert.isOk(Array.isArray(params.RequestItems[TABLE_NAME]), `RequestItems`);
		});

		t.it(`sends serialized keys in params`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(keys.length, requests.length, `keys.length === requests.length`);

			requests.forEach((req, i) => {
				req = req.DeleteRequest;
				const key = keys[i];
				assert.isEqual(key.type, req.Key.scope_type_key.S.split(`:`)[1], `key type`);
				assert.isEqual(key.id, req.Key.id.S, `key id`);
			});
		});

		t.it(`includes scope_type_key attribute`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(keys.length, requests.length, `keys.length === requests.length`);

			requests.forEach((req, i) => {
				req = req.DeleteRequest;
				assert.isEqual(`${SCOPE}:${keys[i].type}`, req.Key.scope_type_key.S, `scope_type_key`);
			});
		});
	});

	t.describe(`always a throughput error`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried batchRemove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {
			backoffMultiplier: 10,
			prefix
		});

		// Input parameter.
		const keys = [
			{
				type: `foo`,
				id: `bar`
			}
		];

		let ERROR = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			return dynamodbBatchRemoveObjects(null, SCOPE, keys).then((res) => {
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

		// The log backoff formula defined in dynamodb.js computeBackoffTime() that gives us:
		// `600 === [2,3,4,5].reduce((n, i) => n + Math.pow(2, i) * 10, 0)`
		t.it(`consumes more than 600ms for 5 retries`, () => {
			assert.isGreaterThan(600, ELAPSED, `elapsed time`);
		});

		t.it(`calls DynamoDB#batchWriteItem() 5 times`, () => {
			assert.isEqual(5, dynamodb.batchWriteItem.callCount, `batchWriteItem() calls`);

			// Use the sinon .args Array to access each method call.
			dynamodb.batchWriteItem.args.forEach((args) => {
				const requests = args[0].RequestItems[TABLE_NAME];

				assert.isEqual(keys.length, requests.length, `keys.length === requests.length`);

				requests.forEach((req, i) => {
					req = req.DeleteRequest;
					const key = keys[i];
					assert.isEqual(key.type, req.Key.scope_type_key.S.split(`:`)[1], `key.type`);
					assert.isEqual(key.id, req.Key.id.S, `key.id`);
				});
			});
		});
	});

	t.describe(`with throughput error in first call only`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		let count = 0;

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			// Increment the counter.
			count += 1;

			// Make the callback async.
			process.nextTick(() => {
				if (count < 2) {
					// Raise the throughput error if this is the first try.
					callback(new ProvisionedThroughputExceededException(`TEST`));
				}

				callback(null, {UnprocessedItems: {}, foo: `bar`});
			});
		});

		// Create our curried batchRemove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {
			prefix
		});

		// Input parameter.
		const keys = [
			{
				type: `foo`,
				id: `bar`
			}
		];

		let RESULT = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			// Override the backoffMultiplier
			return dynamodbBatchRemoveObjects({backoffMultiplier: 100}, SCOPE, keys).then((res) => {
				RESULT = res;
				ELAPSED = Date.now() - START;
				return done();
			}).catch(done);
		});

		t.it(`calls DynamoDB#batchWriteItem() correct number of times`, () => {
			assert.isOk(dynamodb.batchWriteItem.calledTwice, `batchWriteItem() called twice`);
		});

		t.it(`returns an array of true values`, () => {
			assert.isEqual(keys.length, RESULT.data.length, `same length`);

			RESULT.data.forEach((val) => {
				assert.isEqual(true, val, `value is true`);
			});
		});

		// The log backoff formula defined in dynamodb.js computeBackoffTime() that gives us:
		// `400 === Math.pow(2, 0 + 2) * 100`
		t.it(`consumes more than 400ms for 1 retry`, () => {
			assert.isGreaterThan(400, ELAPSED, `elapsed time`);
		});
	});

	t.describe(`always has UnprocessedItems`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			const UnprocessedItems = clone(params.RequestItems);

			// "Process" 1 item by popping off the first item using tail().
			UnprocessedItems[TABLE_NAME] = tail(UnprocessedItems[TABLE_NAME]);

			// Make the callback async.
			process.nextTick(() => {
				callback(null, {UnprocessedItems});
			});
		});

		// Create our curried batchRemove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {
			backoffMultiplier: 1000,
			prefix
		});

		// Input parameter.
		const keys = range(0, 5).map(createKey);

		let ERROR = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			return dynamodbBatchRemoveObjects({backoffMultiplier: 10}, SCOPE, keys).then((res) => {
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

		// The log backoff formula defined in dynamodb.js computeBackoffTime() that gives us:
		// `600 === [2,3,4,5].reduce((n, i) => n + Math.pow(2, i) * 10, 0)`
		t.it(`consumes more than 600ms for 5 retries`, () => {
			assert.isGreaterThan(600, ELAPSED, `elapsed time`);
		});

		t.it(`calls DynamoDB#batchWriteItem() 5 times`, () => {
			assert.isEqual(5, dynamodb.batchWriteItem.callCount, `batchWriteItem() calls`);

			// Use the sinon .args Array to access each method call.
			dynamodb.batchWriteItem.args.forEach((args, call) => {
				const requests = args[0].RequestItems[TABLE_NAME];

				// Since we "process" 1 item in each call to our mock batchWriteItem() we
				// can conclude the correct number of keys is the total keys minus
				// the call index.
				assert.isEqual(keys.length - call, requests.length, `keys.length === requests.length`);

				requests.forEach((req, i) => {
					req = req.DeleteRequest;
					// Since we "process" 1 item in each call to our mock batchWriteItem()
					// we can conclude the correct key index is the current index added
					// to the call index.
					const key = keys[i + call];
					assert.isEqual(key.type, req.Key.scope_type_key.S.split(`:`)[1], `key.type`);
					assert.isEqual(key.id, req.Key.id.S, `key.id`);
				});
			});
		});
	});

	t.describe(`with UnprocessedItems in first call only`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		let count = 0;

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			count += 1;

			let UnprocessedItems = {};

			if (count <= 1) {
				UnprocessedItems = clone(params.RequestItems);
				// "Process" 1 item by popping off the first item using tail().
				UnprocessedItems[TABLE_NAME] = tail(UnprocessedItems[TABLE_NAME]);
			}

			let res = {
				UnprocessedItems,
				foo: `bar`
			};

			// Make the callback async.
			process.nextTick(() => {
				callback(null, res);
			});
		});

		// Create our curried batchRemove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {
			prefix
		});

		// Input parameter.
		const keys = range(0, 2).map(createKey);

		let RESULT = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			return dynamodbBatchRemoveObjects({backoffMultiplier: 100}, SCOPE, keys).then((res) => {
				RESULT = res;
				ELAPSED = Date.now() - START;
				return done();
			}).catch(done);
		});

		t.it(`calls DynamoDB#batchWriteItem() correct number of times`, () => {
			assert.isOk(dynamodb.batchWriteItem.calledTwice, `batchWriteItem() called twice`);
		});

		t.it(`returns an array of true values`, () => {
			assert.isEqual(keys.length, RESULT.data.length, `same length`);

			RESULT.data.forEach((val) => {
				assert.isEqual(true, val, `value is true`);
			});
		});

		// The log backoff formula defined in dynamodb.js computeBackoffTime() that gives us:
		// `400 === Math.pow(2, 0 + 2) * 100`
		t.it(`consumes more than 400ms for 1 retry`, () => {
			assert.isGreaterThan(400, ELAPSED, `elapsed time`);
		});
	});

	t.describe(`with more than 25 items`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.spy(dynamodb, `batchWriteItem`);

		// Create our curried batchRemove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {prefix});

		// Input parameter.
		const keys = range(0, 26).map(createKey);

		let RESULT = null;

		t.before((done) => {
			return dynamodbBatchRemoveObjects(null, SCOPE, keys).then((res) => {
				RESULT = res;
				return done();
			}).catch(done);
		});

		t.it(`calls DynamoDB#batchWriteItem()`, () => {
			assert.isOk(dynamodb.batchWriteItem.calledTwice, `batchWriteItem() called twice`);
		});

		t.it(`returns an array of true values`, () => {
			assert.isEqual(keys.length, RESULT.data.length, `same length`);

			RESULT.data.forEach((val) => {
				assert.isEqual(true, val, `value is true`);
			});
		});

		t.it(`sends 25 items in first call`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(25, requests.length, `requests.length`);

			requests.forEach((req, i) => {
				req = req.DeleteRequest;
				const key = keys[i];
				assert.isEqual(key.type, req.Key.scope_type_key.S.split(`:`)[1], `key type`);
				assert.isEqual(key.id, req.Key.id.S, `key id`);
			});
		});

		t.it(`sends 1 item in second call`, () => {
			const params = dynamodb.batchWriteItem.args[1][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(1, requests.length, `requests.length`);

			const req = requests[0].DeleteRequest;
			const key = keys[25];

			assert.isEqual(key.type, req.Key.scope_type_key.S.split(`:`)[1], `key type`);
			assert.isEqual(key.id, req.Key.id.S, `key id`);
		});
	});

	t.describe(`with retryLimit set in initial options`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried batchRemove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {
			prefix,
			backoffMultiplier: 1,
			retryLimit: 2
		});

		// Input parameter.
		const keys = range(0, 3).map(createKey);

		t.before((done) => {
			return dynamodbBatchRemoveObjects(null, SCOPE, keys).then((res) => {
				return done(new Error(`should not resolve`));
			}).catch(() => {
				return done();
			});
		});

		t.it(`calls DynamoDB#batchWriteItem() 2 times`, () => {
			assert.isEqual(2, dynamodb.batchWriteItem.callCount, `batchGetItem() calls`);
		});
	});

	t.describe(`with retryLimit set in call options`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried batchRemove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {
			prefix,
			backoffMultiplier: 10000,
			retryLimit: 10
		});

		// Input parameter.
		const keys = range(0, 3).map(createKey);

		t.before((done) => {
			const opts = {
				backoffMultiplier: 1,
				retryLimit: 2
			};

			return dynamodbBatchRemoveObjects(opts, SCOPE, keys).then((res) => {
				return done(new Error(`should not resolve`));
			}).catch(() => {
				return done();
			});
		});

		t.it(`calls DynamoDB#batchWriteItem() 2 times`, () => {
			assert.isEqual(2, dynamodb.batchWriteItem.callCount, `batchGetItem() calls`);
		});
	});

	t.describe(`with ResourceNotFoundException`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Stub the DynamoDB method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ResourceNotFoundException(`TEST`));
			});
		});

		// Create our curried remove function.
		const dynamodbBatchRemoveObjects = ddb.batchRemove(dynamodb, {prefix});

		// Input parameter.
		const keys = [
			{
				type: `foo`,
				id: `bar`
			}
		];

		let ERROR = null;

		t.before((done) => {
			return dynamodbBatchRemoveObjects(null, SCOPE, keys).then((res) => {
				return done(new Error(`should not resolve`));
			}).catch((err) => {
				ERROR = err;
				return done();
			});
		});

		t.it(`rejects with a StackedError`, () => {
			assert.isEqual(`StackedError`, ERROR.name, `error name`);
			assert.isEqual(`ResourceNotFoundException`, ERROR.errors[0].name, `root error name`);
			assert.isEqual(`Missing DynamoDB table "${TABLE_NAME}": TEST`, ERROR.message, `error message`);
		});
	});
};

function createKey() {
	return {
		type: chance.pickone([`foo`, `bar`]),
		id: chance.guid()
	};
}
