'use strict';

const ddb = require(`../../lib/dynamodb`);
const DynamoDB = require(`../test-support/mocks/dynamodb`);
const {assert} = require(`kixx/library`);
const sinon = require(`sinon`);
const ProvisionedThroughputExceededException = require(`../test-support/mocks/provisioned-throughput-exceeded-exception`);

module.exports = (t) => {
	const prefix = `test`;
	const SCOPE = `SCOPEX`;
	const TABLE_NAME = `${prefix}_entities_master`;

	t.describe(`simple use case`, (t) => {
		// Create a mock AWS.DynamoDB instance.
		const dynamodb = new DynamoDB();

		// Stub the DynamoDB#putItem() method.
		sinon.stub(dynamodb, `putItem`).callsFake((params, callback) => {
			// Make the callback async.
			process.nextTick(() => {
				callback(null, {Attributes: `XXX`, foo: `bar`});
			});
		});

		// Create our curried set function.
		const dynamodbSetObject = ddb.set(dynamodb, {prefix});

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

	t.describe(`always a provision throughput error`, (t) => {
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
			backoffMultiplier: 10
		});

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
			return dynamodbSetObject(null, SCOPE, obj).then((res) => {
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

		t.it(`consumes more than 600ms for 5 retries`, () => {
			assert.isGreaterThan(600, ELAPSED, `elapsed time`);
		});
	});
};
