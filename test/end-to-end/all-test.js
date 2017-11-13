'use strict';

const Promise = require(`bluebird`);
const {EventBus} = require(`kixx`);
const {assert, deepFreeze, range} = require(`kixx/library`);
const AWS = require(`aws-sdk`);

const createDocument = require(`../test-support/create-document`);
const dynaliteServer = require(`../test-support/dynalite-server`);
const {transactionFactory, setupSchema} = require(`../../index`);

const config = require(`./config`);

module.exports = (t) => {
	t.describe(`all`, (t) => {
		let DynaliteServer;

		const events = new EventBus();

		const dynamodb = new AWS.DynamoDB({
			region: config.DYNAMODB_REGION,
			endpoint: config.DYNAMODB_ENDPOINT,
			accessKeyId: config.DYNAMODB_ACCESS_KEY_ID,
			secretAccessKey: config.DYNAMODB_SECRET_ACCESS_KEY
		});

		const params = deepFreeze({
			scope: `all-test-scope`,
			type: `testDoc`,
			events,
			dynamodb
		});

		const documents = range(0, 1000).map(createDocument).map(deepFreeze);

		const resultsA = Object.create(null);
		const resultsB = Object.create(null);

		const createTransaction = transactionFactory({
			events,
			dynamodb,
			dynamodbTablePrefix: `test`
		});

		t.before((done) => {
			const txn = createTransaction();

			return Promise.resolve(null)
				.then(() => {
					const params = {
						port: 4567,
						createTableMs: 1,
						deleteTableMs: 1,
						updateTableMs: 1
					};
					return dynaliteServer(params).then((server) => {
						DynaliteServer = server;
						return null;
					});
				})
				.then(() => {
					return setupSchema({
						dynamodb,
						dynamodbTablePrefix: `test`
					});
				})
				.then(() => {
					return txn.batchSet({scope: params.scope, objects: documents}).then((res) => {
						resultsA.batchSetAllDocuments = res;
						return null;
					});
				})
				.then(() => {
					const keys = documents.map((doc) => {
						return {type: doc.type, id: doc.id};
					});
					return txn.batchGet({scope: params.scope, keys}).then((res) => {
						resultsA.batchGetAllDocuments = res;
						return null;
					});
				})
				.then(txn.commit)
				.then(() => {
					deepFreeze(resultsA);
					deepFreeze(resultsB);
					done();
					return null;
				})
				.catch(done);
		});

		t.after((done) => {
			return Promise.resolve(null)
				.then(() => {
					return new Promise((resolve, reject) => {
						DynaliteServer.close((err) => {
							if (err) return reject(err);
							return resolve(null);
						});
					});
				})
				.then(() => {
					done();
					return null;
				})
				.catch(done);
		});

		t.describe(`batchSet`, (t) => {
			t.it(`returns copies of all set objects`, () => {
				const data = resultsA.batchSetAllDocuments.data;

				assert.isOk(Array.isArray(data), `is Array`);
				assert.isGreaterThan(0, data.length, `length > 0`);
				assert.isEqual(data.length, documents.length, `data.length`);

				data.forEach((obj, i) => {
					assert.isNotEqual(obj, documents[i], `objects are not referencial equal`);
					assert.isEqual(obj.type, documents[i].type, `type matches`);
					assert.isEqual(obj.id, documents[i].id, `id matches`);
				});
			});
		});

		t.describe(`batchGet in same transaction`, (t) => {
			t.it(`returns all documents`, () => {
				const data = resultsA.batchGetAllDocuments.data;

				assert.isOk(Array.isArray(data), `is Array`);
				assert.isGreaterThan(0, data.length, `length > 0`);
				assert.isEqual(data.length, documents.length, `data.length`);
			});
		});
	});
};
