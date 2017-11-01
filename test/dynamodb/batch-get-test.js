'use strict';

const ddb = require(`../../lib/dynamodb`);
const DynamoDB = require(`../test-support/mocks/dynamodb`);
const {range} = require(`ramda`);
const {assert} = require(`kixx/library`);
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
		sinon.spy(dynamodb, `batchGetItem`);

		// Create our curried batchGet function.
		const dynamodbBatchGetObjects = ddb.batchGet(dynamodb, {prefix});

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
			return dynamodbBatchGetObjects(null, SCOPE, keys).then((res) => {
				RESULT = res;
				return done();
			}).catch(done);
		});

		t.it(`returns the objects referenced by the keys`, () => {
			assert.isEqual(keys.length, RESULT.data.length, `same length`);

			RESULT.data.forEach((object, i) => {
				assert.isEqual(keys[i].type, object.type, `type property`);
				assert.isEqual(keys[i].id, object.id, `id property`);
				assert.isEqual(`Foo Bar`, object.attributes.title, `title attribute`);
			});
		});

		t.it(`calls DynamoDB#batchGetItem()`, () => {
			assert.isOk(dynamodb.batchGetItem.calledOnce, `batchGetItem() called once`);
		});

		t.it(`sends correct table name in params`, () => {
			const params = dynamodb.batchGetItem.args[0][0];
			assert.isEqual(TABLE_NAME, Object.keys(params.RequestItems)[0], `TableName`);
			assert.isOk(Array.isArray(params.RequestItems[TABLE_NAME].Keys), `RequestItems`);
		});

		t.it(`sends serialized keys in params`, () => {
			const params = dynamodb.batchGetItem.args[0][0];
			const Keys = params.RequestItems[TABLE_NAME].Keys;

			assert.isEqual(keys.length, Keys.length, `keys.length === Keys.length`);

			Keys.forEach((key1, i) => {
				const key = keys[i];
				assert.isEqual(key.type, key1.scope_type_key.S.split(`:`)[1], `key type`);
				assert.isEqual(key.id, key1.id.S, `key id`);
			});
		});

		t.it(`includes scope_type_key attribute`, () => {
			const params = dynamodb.batchGetItem.args[0][0];
			const Keys = params.RequestItems[TABLE_NAME].Keys;

			assert.isEqual(keys.length, Keys.length, `keys.length === Keys.length`);

			Keys.forEach((key1, i) => {
				assert.isEqual(`${SCOPE}:${keys[i].type}`, key1.scope_type_key.S, `scope_type_key`);
			});
		});
	});

	t.describe(`always a throughput error`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.stub(dynamodb, `batchGetItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried batchGet function.
		const dynamodbBatchGetObjects = ddb.batchGet(dynamodb, {
			backoffMultiplier: 10,
			prefix
		});

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

		let ERROR = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			return dynamodbBatchGetObjects(null, SCOPE, keys).then((res) => {
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
	});

	t.describe(`with ResourceNotFoundException`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Stub the DynamoDB method.
		sinon.stub(dynamodb, `batchGetItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ResourceNotFoundException(`TEST`));
			});
		});

		// Create our curried get function.
		const dynamodbBatchGetObjects = ddb.batchGet(dynamodb, {prefix});

		// Input parameter.
		const keys = [
			{
				type: `foo`,
				id: `bar`
			}
		];

		let ERROR = null;

		t.before((done) => {
			return dynamodbBatchGetObjects(null, SCOPE, keys).then((res) => {
				return done(new Error(`should not resolve`));
			}).catch((err) => {
				ERROR = err;
				return done();
			});
		});

		t.it(`rejects with a StackedError`, () => {
			assert.isEqual(`StackedError`, ERROR.name, `error name`);
			assert.isEqual(`ResourceNotFoundException`, ERROR.errors[0].name, `root error name`);
			assert.isEqual(`Missing DynamoDB table "${TABLE_NAME}"`, ERROR.message, `error message`);
		});
	});
};

function createKey() {
	return {
		type: chance.pickone([`foo`, `bar`]),
		id: chance.guid()
	};
}
