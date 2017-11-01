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

	t.describe(`simple use case`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB method.
		sinon.spy(dynamodb, `getItem`);

		// Create our curried set function.
		const dynamodbGetObject = ddb.get(dynamodb, {prefix});

		// Input parameter.
		const key = {
			type: `foo`,
			id: `bar`
		};

		let RESULT = null;

		t.before((done) => {
			return dynamodbGetObject(null, SCOPE, key).then((res) => {
				RESULT = res;
				return done();
			}).catch(done);
		});

		t.it(`returns the object referenced by the key`, () => {
			assert.isEqual(key.type, RESULT.data.type, `type property`);
			assert.isEqual(key.id, RESULT.data.id, `id property`);
			assert.isEqual(`Foo Bar`, RESULT.data.attributes.title, `title attribute`);
		});

		t.it(`includes DynamoDB response in meta object`, () => {
			assert.isEqual(`bar`, RESULT.meta.foo, `random DynamoDB response attribute`);
		});

		t.it(`calls DynamoDB#getItem()`, () => {
			assert.isOk(dynamodb.getItem.calledOnce, `getItem() called once`);
		});

		t.it(`sends correct table name in params`, () => {
			const params = dynamodb.getItem.args[0][0];
			assert.isEqual(TABLE_NAME, params.TableName, `TableName`);
		});

		t.it(`sends serialized key in params`, () => {
			const {Key} = dynamodb.getItem.args[0][0];
			assert.isEqual(2, Object.keys(Key).length, `only has 2 keys`);
			assert.isEqual(key.id, Key.id.S, `Key.id`);
			assert.isEqual(`${SCOPE}:${key.type}`, Key.scope_type_key.S, `Key.scope_type_key`);
		});
	});

	t.describe(`always a throughput error`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Stub the DynamoDB method.
		sinon.stub(dynamodb, `getItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried get function.
		const dynamodbGetObject = ddb.get(dynamodb, {
			prefix
		});

		// Input parameter.
		const key = {
			type: `foo`,
			id: `bar`
		};

		let ERROR = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			// Override the backoffMultiplier
			return dynamodbGetObject({backoffMultiplier: 10}, SCOPE, key).then((res) => {
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

		t.it(`calls DynamoDB#getItem() 5 times`, () => {
			assert.isEqual(5, dynamodb.getItem.callCount, `getItem() calls`);

			// Use the sinon .args Array to access each method call.
			dynamodb.getItem.args.forEach((args) => {
				const {Key} = args[0];
				assert.isEqual(key.id, Key.id.S, `Key.id`);
				assert.isEqual(`${SCOPE}:${key.type}`, Key.scope_type_key.S, `Key.scope_type_key`);
			});
		});
	});

	t.describe(`with throughput error in first call only`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		let count = 0;

		// Stub the DynamoDB method.
		sinon.stub(dynamodb, `getItem`).callsFake((params, callback) => {
			// Increment the counter.
			count += 1;

			// Make the callback async.
			process.nextTick(() => {
				if (count < 2) {
					// Raise the throughput error if this is the first try.
					callback(new ProvisionedThroughputExceededException(`TEST`));
				}

				let res = {
					Item: JSON.parse(JSON.stringify(params.Key))
				};

				res.Item.type = {S: params.Key.scope_type_key.S.split(`:`)[1]};
				res.Item.attributes = {M: {title: {S: `Foo Bar`}}};

				callback(null, res);
			});
		});

		// Create our curried get function.
		const dynamodbGetObject = ddb.get(dynamodb, {
			prefix
		});

		// Input parameter.
		const key = {
			type: `foo`,
			id: `bar`
		};

		let RESULT = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			// Override the backoffMultiplier
			return dynamodbGetObject({backoffMultiplier: 100}, SCOPE, key).then((res) => {
				RESULT = res;
				ELAPSED = Date.now() - START;
				return done();
			}).catch(done);
		});

		t.it(`returns the object referenced by the key`, () => {
			assert.isEqual(key.type, RESULT.data.type, `type property`);
			assert.isEqual(key.id, RESULT.data.id, `id property`);
			assert.isEqual(`Foo Bar`, RESULT.data.attributes.title, `title attribute`);
		});

		t.it(`calls DynamoDB#getItem() correct number of times`, () => {
			assert.isOk(dynamodb.getItem.calledTwice, `getItem() called twice`);
		});

		// The log backoff formula defined in dynamodb.js computeBackoffTime() that gives us:
		// `400 === Math.pow(2, 0 + 2) * 100`
		t.it(`consumes more than 400ms for 1 retry`, () => {
			assert.isGreaterThan(400, ELAPSED, `elapsed time`);
		});
	});

	t.describe(`with ResourceNotFoundException`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Stub the DynamoDB method.
		sinon.stub(dynamodb, `getItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ResourceNotFoundException(`TEST`));
			});
		});

		// Create our curried set function.
		const dynamodbGetObject = ddb.get(dynamodb, {prefix});

		// Input parameter.
		const key = {
			type: `foo`,
			id: `bar`
		};

		let ERROR = null;

		t.before((done) => {
			return dynamodbGetObject(null, SCOPE, key).then((res) => {
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
