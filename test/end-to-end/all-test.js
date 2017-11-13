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

		const results = Object.create(null);

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
					return setupSchema({
						dynamodb,
						dynamodbTablePrefix: `test`
					});
				})
				.then(() => {
					return txn.batchSet({scope: params.scope, objects: documents}).then((res) => {
						results.batchSetAllDocuments = res;
						return null;
					});
				})
				.then(txn.commit)
				.then(() => {
					deepFreeze(results);
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

		t.it(`is not smoking`, () => {
			assert.isOk(false, `smoking`);
		});
	});
};
