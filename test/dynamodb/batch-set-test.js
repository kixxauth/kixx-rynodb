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
		sinon.spy(dynamodb, `batchWriteItem`);

		// Create our curried batchSet function.
		const dynamodbBatchSetObjects = ddb.batchSet(dynamodb, {prefix});

		// Input parameter.
		const objects = [
			{
				type: `foo`,
				id: `bar`,
				attributes: {
					title: `Foo Bar`
				}
			},
			{
				type: `foo`,
				id: `baz`,
				attributes: {
					title: `Foo Baz`
				}
			}
		];

		let RESULT = null;

		t.before((done) => {
			return dynamodbBatchSetObjects(null, SCOPE, objects).then((res) => {
				RESULT = res;
				return done();
			}).catch(done);
		});

		t.it(`returns copies of the objects as result data`, () => {
			assert.isNotEqual(objects, RESULT.data, `data is not referencially equal`);
			assert.isEqual(objects.length, RESULT.data.length, `same length`);

			RESULT.data.forEach((data, i) => {
				assert.isNotEqual(objects[i], data, `objects are not referencially equal`);
				assert.isEqual(objects[i].type, data.type, `type property`);
				assert.isEqual(objects[i].id, data.id, `id property`);
				assert.isEqual(objects[i].attributes.title, data.attributes.title, `title attribute`);
			});
		});

		t.it(`calls DynamoDB#batchWriteItem()`, () => {
			assert.isOk(dynamodb.batchWriteItem.calledOnce, `batchWriteItem() called once`);
		});

		t.it(`sends correct table name in params`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			assert.includes(TABLE_NAME, Object.keys(params.RequestItems), `TableName`);
			assert.isOk(Array.isArray(params.RequestItems[TABLE_NAME]), `RequestItems`);
		});

		t.it(`send only Put requests`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(objects.length, requests.length, `objects.length === requests.length`);

			requests.forEach((req) => {
				const keys = Object.keys(req);
				assert.isEqual(1, keys.length, `single enumerable key`);
				assert.isEqual(`PutRequest`, keys[0], `request key`);
			});
		});

		t.it(`sends serialized items in params`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(objects.length, requests.length, `objects.length === requests.length`);

			requests.forEach((req, i) => {
				const {Item} = req.PutRequest;
				const obj = objects[i];
				assert.isEqual(obj.type, Item.type.S, `Item.type`);
				assert.isEqual(obj.id, Item.id.S, `Item.id`);
				assert.isEqual(obj.attributes.title, Item.attributes.M.title.S, `Item.attributes`);
			});
		});

		t.it(`includes scope_type_key attribute`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(objects.length, requests.length, `objects.length === requests.length`);

			requests.forEach((req, i) => {
				const {Item} = req.PutRequest;
				assert.isEqual(`${SCOPE}:${objects[i].type}`, Item.scope_type_key.S, `Item.scope_type_key`);
			});
		});
	});

	t.describe(`always a throughput error`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Spy on the DynamoDB#putItem() method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ProvisionedThroughputExceededException(`TEST`));
			});
		});

		// Create our curried batchSet function.
		const dynamodbBatchSetObjects = ddb.batchSet(dynamodb, {
			backoffMultiplier: 10,
			prefix
		});

		// Input parameter.
		const objects = [
			{
				type: `foo`,
				id: `bar`,
				attributes: {
					title: `Foo Bar`
				}
			}
		];

		let ERROR = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			return dynamodbBatchSetObjects(null, SCOPE, objects).then((res) => {
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

			dynamodb.batchWriteItem.args.forEach((args) => {
				const requests = args[0].RequestItems[TABLE_NAME];

				assert.isEqual(objects.length, requests.length, `objects.length === requests.length`);

				requests.forEach((req, i) => {
					const {Item} = req.PutRequest;
					const obj = objects[i];
					assert.isEqual(obj.type, Item.type.S, `Item.type`);
					assert.isEqual(obj.id, Item.id.S, `Item.id`);
					assert.isEqual(obj.attributes.title, Item.attributes.M.title.S, `Item.attributes`);
				});
			});
		});
	});


	t.describe(`with ResourceNotFoundException`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Stub the DynamoDB#putItem() method.
		sinon.stub(dynamodb, `batchWriteItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(new ResourceNotFoundException(`TEST`));
			});
		});

		// Create our curried set function.
		const dynamodbBatchSetObjects = ddb.batchSet(dynamodb, {prefix});

		// Input parameter.
		const objects = [
			{
				type: `foo`,
				id: `bar`,
				attributes: {
					title: `Foo Bar`
				}
			}
		];

		let ERROR = null;

		t.before((done) => {
			return dynamodbBatchSetObjects(null, SCOPE, objects).then((res) => {
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
