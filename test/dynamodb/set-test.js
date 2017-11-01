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

		// Spy on the DynamoDB#putItem() method.
		sinon.spy(dynamodb, `putItem`);

		// Create our curried set function.
		const dynamodbSetObject = ddb.set(dynamodb, {prefix});

		// Input parameter.
		const obj = {
			type: `foo`,
			id: `bar`,
			attributes: {
				title: `Foo Bar`
			}
		};

		let RESULT = null;

		t.before((done) => {
			return dynamodbSetObject(null, SCOPE, obj).then((res) => {
				RESULT = res;
				return done();
			}).catch(done);
		});

		t.it(`returns a copy of the object as result data`, () => {
			assert.isNotEqual(obj, RESULT.data, `objects are not referencially equal`);
			assert.isEqual(obj.type, RESULT.data.type, `type property`);
			assert.isEqual(obj.id, RESULT.data.id, `id property`);
			assert.isEqual(obj.attributes.title, RESULT.data.attributes.title, `title attribute`);
		});

		t.it(`includes DynamoDB response in meta object`, () => {
			assert.isEqual(`bar`, RESULT.meta.foo, `random DynamoDB response attribute`);
			assert.isUndefined(RESULT.meta.Attributes, `filter off the "Attributes" property`);
		});

		t.it(`calls DynamoDB#putItem()`, () => {
			assert.isOk(dynamodb.putItem.calledOnce, `putItem() called once`);
		});

		t.it(`sends correct table name in params`, () => {
			const params = dynamodb.putItem.args[0][0];
			assert.isEqual(TABLE_NAME, params.TableName, `TableName`);
		});

		t.it(`sends serialized item in params`, () => {
			const {Item} = dynamodb.putItem.args[0][0];
			assert.isEqual(obj.type, Item.type.S, `Item.type`);
			assert.isEqual(obj.id, Item.id.S, `Item.id`);
			assert.isEqual(obj.attributes.title, Item.attributes.M.title.S, `Item.attributes`);
		});

		t.it(`includes scope_type_key attribute`, () => {
			const {Item} = dynamodb.putItem.args[0][0];
			assert.isEqual(`${SCOPE}:${obj.type}`, Item.scope_type_key.S, `Item.scope_type_key`);
		});
	});

	t.describe(`always a throughput error`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Stub the DynamoDB#putItem() method.
		sinon.stub(dynamodb, `putItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried set function.
		const dynamodbSetObject = ddb.set(dynamodb, {
			prefix,
			backoffMultiplier: 100
		});

		// Input parameter.
		const obj = {
			type: `foo`,
			id: `bar`,
			attributes: {
				title: `Foo Bar`
			}
		};

		let ERROR = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			// Override the backoffMultiplier
			return dynamodbSetObject({backoffMultiplier: 10}, SCOPE, obj).then((res) => {
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

		t.it(`calls DynamoDB#putItem() 5 times`, () => {
			assert.isEqual(5, dynamodb.putItem.callCount, `putItem() calls`);

			dynamodb.putItem.args.forEach((args) => {
				const {Item} = args[0];
				assert.isEqual(obj.type, Item.type.S, `Item.type`);
				assert.isEqual(obj.id, Item.id.S, `Item.id`);
				assert.isEqual(obj.attributes.title, Item.attributes.M.title.S, `Item.attributes`);
			});
		});
	});

	t.describe(`throughput error on first try`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		let count = 0;

		// Stub the DynamoDB#putItem() method.
		sinon.stub(dynamodb, `putItem`).callsFake((params, callback) => {
			// Increment the counter.
			count += 1;

			// Make the callback async.
			process.nextTick(() => {
				if (count < 2) {
					// Raise the throughput error if this is the first try.
					callback(new ProvisionedThroughputExceededException(`TEST`));
				}
				callback(null, {Attributes: `XXX`, foo: `bar`});
			});
		});

		// Create our curried set function.
		const dynamodbSetObject = ddb.set(dynamodb, {
			prefix,
			backoffMultiplier: 10
		});

		// Input parameter.
		const obj = {
			type: `foo`,
			id: `bar`,
			attributes: {
				title: `Foo Bar`
			}
		};

		let RESULT = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			// Override the backoffMultiplier
			return dynamodbSetObject({backoffMultiplier: 100}, SCOPE, obj).then((res) => {
				RESULT = res;
				ELAPSED = Date.now() - START;
				return done();
			}).catch(done);
		});

		t.it(`returns a copy of the object as result data`, () => {
			assert.isEqual(obj.type, RESULT.data.type, `type property`);
			assert.isEqual(obj.id, RESULT.data.id, `id property`);
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

		// Stub the DynamoDB#putItem() method.
		sinon.stub(dynamodb, `putItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ResourceNotFoundException(`TEST`));
			});
		});

		// Create our curried set function.
		const dynamodbSetObject = ddb.set(dynamodb, {prefix});

		// Input parameter.
		const obj = {
			type: `foo`,
			id: `bar`,
			attributes: {
				title: `Foo Bar`
			}
		};

		let ERROR = null;

		t.before((done) => {
			return dynamodbSetObject(null, SCOPE, obj).then((res) => {
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
