'use strict';

const ddb = require(`../../lib/dynamodb`);
const DynamoDB = require(`../test-support/mocks/dynamodb`);
const {assert, range} = require(`kixx/library`);
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
			assert.isEqual(TABLE_NAME, Object.keys(params.RequestItems)[0], `TableName`);
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

		// Spy on the DynamoDB method.
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

			// Use the sinon .args Array to access each method call.
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

		// Create our curried batchSet function.
		const dynamodbBatchSetObjects = ddb.batchSet(dynamodb, {
			backoffMultiplier: 1000,
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

		let RESULT = null;
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			// Override the backoffMultiplier
			return dynamodbBatchSetObjects({backoffMultiplier: 100}, SCOPE, objects).then((res) => {
				RESULT = res;
				ELAPSED = Date.now() - START;
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

		t.it(`calls DynamoDB#batchWriteItem() correct number of times`, () => {
			assert.isOk(dynamodb.batchWriteItem.calledTwice, `batchWriteItem() called twice`);
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
			const UnprocessedItems = {};

			UnprocessedItems[TABLE_NAME] = [
				params.RequestItems[TABLE_NAME][1] || params.RequestItems[TABLE_NAME][0]
			];

			// Make the callback async.
			process.nextTick(() => {
				callback(null, {UnprocessedItems});
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
			},
			{
				type: `foo`,
				id: `baz`,
				attributes: {
					title: `Foo Baz`
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

			// Use the sinon .args Array to access each method call.
			dynamodb.batchWriteItem.args.forEach((args, call) => {
				const requests = args[0].RequestItems[TABLE_NAME];

				if (call === 0) {
					// On the first call all object requests are sent.
					assert.isEqual(objects.length, requests.length, `objects.length === requests.length`);
				} else {
					// On subsequent calls only the object specified in UnprocessedItems is sent.
					assert.isEqual(1, requests.length, `1 === requests.length`);
				}

				requests.forEach((req, i) => {
					const {Item} = req.PutRequest;

					// On the first call all object requests are sent.
					// On subsequent calls only the object specified in UnprocessedItems is sent.
					const obj = call === 0 ? objects[i] : objects[i + 1];

					assert.isEqual(obj.type, Item.type.S, `Item.type`);
					assert.isEqual(obj.id, Item.id.S, `Item.id`);
					assert.isEqual(obj.attributes.title, Item.attributes.M.title.S, `Item.attributes`);
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

			const UnprocessedItems = {};

			if (count <= 1) {
				UnprocessedItems[TABLE_NAME] = [
					params.RequestItems[TABLE_NAME][1] || params.RequestItems[TABLE_NAME][0]
				];
			}

			// Make the callback async.
			process.nextTick(() => {
				callback(null, {UnprocessedItems});
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
		const START = Date.now();
		let ELAPSED;

		t.before((done) => {
			return dynamodbBatchSetObjects(null, SCOPE, objects).then((res) => {
				RESULT = res;
				ELAPSED = Date.now() - START;
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

		t.it(`calls DynamoDB#batchWriteItem() correct number of times`, () => {
			assert.isOk(dynamodb.batchWriteItem.calledTwice, `batchWriteItem() called twice`);
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

		// Create our curried batchSet function.
		const dynamodbBatchSetObjects = ddb.batchSet(dynamodb, {prefix});

		// Input parameter.
		const objects = range(0, 26).map(createObject);

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
			assert.isOk(dynamodb.batchWriteItem.calledTwice, `batchWriteItem() called twice`);
		});

		t.it(`sends 25 items in first call`, () => {
			const params = dynamodb.batchWriteItem.args[0][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(25, requests.length, `requests.length === 25`);

			requests.forEach((req, i) => {
				const {Item} = req.PutRequest;
				const obj = objects[i];
				assert.isEqual(obj.type, Item.type.S, `Item.type`);
				assert.isEqual(obj.id, Item.id.S, `Item.id`);
				assert.isEqual(obj.attributes.title, Item.attributes.M.title.S, `Item.attributes`);
			});
		});

		t.it(`sends 1 item in second call`, () => {
			const params = dynamodb.batchWriteItem.args[1][0];
			const requests = params.RequestItems[TABLE_NAME];

			assert.isEqual(1, requests.length, `requests.length === 1`);

			const req = requests[0];
			const obj = objects[25];
			const {Item} = req.PutRequest;

			assert.isEqual(obj.type, Item.type.S, `Item.type`);
			assert.isEqual(obj.id, Item.id.S, `Item.id`);
			assert.isEqual(obj.attributes.title, Item.attributes.M.title.S, `Item.attributes`);
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

		// Create our curried batchSet function.
		const dynamodbBatchSetObjects = ddb.batchSet(dynamodb, {
			prefix,
			backoffMultiplier: 1,
			retryLimit: 2
		});

		// Input parameter.
		const objects = range(0, 3).map(createObject);

		t.before((done) => {
			return dynamodbBatchSetObjects(null, SCOPE, objects).then((res) => {
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

		// Create our curried batchSet function.
		const dynamodbBatchSetObjects = ddb.batchSet(dynamodb, {
			prefix,
			backoffMultiplier: 10000,
			retryLimit: 10
		});

		// Input parameter.
		const objects = range(0, 3).map(createObject);

		t.before((done) => {
			const opts = {
				backoffMultiplier: 1,
				retryLimit: 2
			};

			return dynamodbBatchSetObjects(opts, SCOPE, objects).then((res) => {
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
			assert.isEqual(`Missing DynamoDB table "${TABLE_NAME}": TEST`, ERROR.message, `error message`);
		});
	});
};

function createObject() {
	return {
		type: chance.pickone([`foo`, `bar`]),
		id: chance.guid(),
		attributes: {
			title: chance.word()
		}
	};
}
