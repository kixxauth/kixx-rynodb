'use strict';

const ddb = require(`../../lib/dynamodb`);
const DynamoDB = require(`../test-support/mocks/dynamodb`);
const {assert} = require(`kixx/library`);

module.exports = (t) => {
	const prefix = `test`;
	const SCOPE = `SCOPEX`;

	t.describe(`simple use case`, (t) => {
		const dynamodb = new DynamoDB();
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
	});
};
