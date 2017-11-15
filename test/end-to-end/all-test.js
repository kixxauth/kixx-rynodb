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

		const documents = range(0, 1000).map(createDocument).map(deepFreeze);

		const createTransaction = transactionFactory({
			events,
			dynamodb,
			dynamodbTablePrefix: `test`
		});

		const params = Object.freeze({
			scope: `all-test-scope`,
			type: `testDoc`,
			events,
			dynamodb,
			createTransaction,
			documents
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
					return txn.batchSet({scope: params.scope, objects: documents});
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
