'use strict';

const Promise = require(`bluebird`);
const {EventBus} = require(`kixx`);
const {deepFreeze, range} = require(`kixx/library`);
const AWS = require(`aws-sdk`);

const createDocument = require(`../test-support/create-document`);
const dynaliteServer = require(`../test-support/dynalite-server`);
const {reportFullStackTrace} =require(`../test-support/library`);
const {transactionFactory, setupSchema} = require(`../../index`);

const testGet = require(`./test-get`);

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

		const documents = Object.freeze(range(0, 1000).map(createDocument).map(deepFreeze));

		const collection1 = createDocument({type: `testCollection`});
		const collection2 = createDocument({type: `testCollection`});
		const collection3 = createDocument({type: `testCollection`});

		collection1.relationships = {
			foos: documents.slice(0, 200).map((d) => {
				return {type: d.type, id: d.id};
			})
		};

		collection2.relationships = {
			bars: documents.slice(0, 50).map((d) => {
				return {type: d.type, id: d.id};
			})
		};

		collection3.relationships = {
			foos: documents.slice(0, 10).map((d) => {
				return {type: d.type, id: d.id};
			}),
			bars: documents.slice(10, 20).map((d) => {
				return {type: d.type, id: d.id};
			})
		};

		const collections = Object.freeze([
			deepFreeze(collection1),
			deepFreeze(collection2),
			deepFreeze(collection3)
		]);

		const createTransaction = transactionFactory({
			events,
			dynamodb,
			dynamodbTablePrefix: `test`
		});

		const params = Object.freeze({
			scope: `all-test-scope`,
			events,
			dynamodb,
			createTransaction,
			documents,
			collections
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
					return txn.batchSet({
						scope: params.scope,
						objects: documents
					});
				})
				.then(() => {
					return txn.batchSet({
						scope: params.scope,
						objects: collections
					});
				})
				.then(() => {
					return txn.commit();
				})
				.then(() => {
					return done();
				})
				.catch(reportFullStackTrace(done));
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

		testGet(t, params);
	});
};
