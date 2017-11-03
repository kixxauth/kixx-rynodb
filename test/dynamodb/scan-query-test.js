'use strict';

const ddb = require(`../../lib/dynamodb`);
const DynamoDB = require(`../test-support/mocks/dynamodb`);
const {assert} = require(`kixx/library`);
const sinon = require(`sinon`);
// const ProvisionedThroughputExceededException = require(`../test-support/mocks/provisioned-throughput-exceeded-exception`);
// const ResourceNotFoundException = require(`../test-support/mocks/resource-not-found-exception`);

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
	});
};
