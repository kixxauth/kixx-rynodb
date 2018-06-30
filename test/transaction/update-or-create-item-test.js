'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const {Transaction, DynamoDb} = require('../../index');
const sinon = require('sinon');
const {assert} = require('kixx/library');

module.exports = function (t) {
	const awsRegion = 'AWS_REGION';
	const awsAccessKey = 'AWS_ACCESS_KEY';
	const awsSecretKey = 'AWS_SECRET_KEY';
	const tablePrefix = 'ttt';

	t.describe('with error in getEntity()', (t) => {
		const emitter = new EventEmitter();

		const dynamodb = DynamoDb.create({
			emitter,
			awsRegion,
			awsAccessKey,
			awsSecretKey,
			tablePrefix
		});

		const txn = Transaction.create({dynamodb});

		sinon.stub(dynamodb.client, '_request').callsFake(function (target) {
			if (target === 'GetItem') {
				return Promise.reject(new Error('test error'));
			}
			throw new Error('unexpected call');
		});

		const object = {
			scope: 'some_scope',
			type: 'some_type',
			id: 'some_id'
		};

		let error;

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				throw new Error('should not have resolved');
			}).catch(function (err) {
				error = err;
				return null;
			}).then(done);
		});

		t.it('rejects the expected exception', () => {
			assert.isEqual('Error during Transaction#updateOrCreateItem(): Error in DynamoDB#getEntity(): test error', error.message);
		});
	});

	t.describe('with error in setEntity() during create', (t) => {
		const emitter = new EventEmitter();

		const dynamodb = DynamoDb.create({
			emitter,
			awsRegion,
			awsAccessKey,
			awsSecretKey,
			tablePrefix
		});

		const txn = Transaction.create({dynamodb});

		sinon.stub(dynamodb.client, '_request').callsFake(function (target) {
			switch (target) {
			case 'GetItem':
				return Promise.resolve({});
			case 'PutItem':
				return Promise.reject(new Error('test error'));
			default:
				return Promise.reject(new Error(`unexpected _request() target: '${target}'`));
			}
		});

		const object = {
			scope: 'some_scope',
			type: 'some_type',
			id: 'some_id'
		};

		let error;

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				throw new Error('should not have resolved');
			}).catch(function (err) {
				error = err;
				return null;
			}).then(done);
		});

		t.it('rejects with the expected exception', () => {
			assert.isEqual('Error during Transaction#updateOrCreateItem(): Error in DynamoDB#setEntity(): test error', error.message);
		});
	});

	t.describe('with error in setEntity() during update', (t) => {
		const emitter = new EventEmitter();

		const dynamodb = DynamoDb.create({
			emitter,
			awsRegion,
			awsAccessKey,
			awsSecretKey,
			tablePrefix
		});

		const txn = Transaction.create({dynamodb});

		sinon.stub(dynamodb.client, '_request').callsFake(function (target) {
			switch (target) {
			case 'GetItem':
				return Promise.resolve({Item: {}});
			case 'PutItem':
				return Promise.reject(new Error('test error'));
			default:
				return Promise.reject(new Error(`unexpected _request() target: '${target}'`));
			}
		});

		const object = {
			scope: 'some_scope',
			type: 'some_type',
			id: 'some_id'
		};

		let error;

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				throw new Error('should not have resolved');
			}).catch(function (err) {
				error = err;
				return null;
			}).then(done);
		});

		t.it('rejects with the expected exception', () => {
			assert.isEqual('Error during Transaction#updateOrCreateItem(): Error in DynamoDB#setEntity(): test error', error.message);
		});
	});

	t.describe('with error in query() during update', (t) => {
		const emitter = new EventEmitter();

		const dynamodb = DynamoDb.create({
			emitter,
			awsRegion,
			awsAccessKey,
			awsSecretKey,
			tablePrefix
		});

		const txn = Transaction.create({dynamodb});

		sinon.stub(dynamodb.client, '_request').callsFake(function (target) {
			switch (target) {
			case 'GetItem':
				return Promise.resolve({Item: {}});
			case 'PutItem':
				return Promise.resolve({});
			case 'Query':
				return Promise.reject(new Error('test error'));
			default:
				return Promise.reject(new Error(`unexpected _request() target: '${target}'`));
			}
		});

		const object = {
			scope: 'some_scope',
			type: 'some_type',
			id: 'some_id'
		};

		let error;


		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				throw new Error('should not have resolved');
			}).catch(function (err) {
				error = err;
				return null;
			}).then(done);
		});

		t.it('rejects with the expected exception', () => {
			assert.isEqual('Error during Transaction#updateOrCreateItem(): Error in DynamoDB#getIndexEntries(): test error', error.message);
		});
	});

	t.describe('with error in BatchWriteItem during create', (t) => {
		const emitter = new EventEmitter();

		const dynamodb = DynamoDb.create({
			emitter,
			awsRegion,
			awsAccessKey,
			awsSecretKey,
			tablePrefix
		});

		const indexes = {
			some_type: {
				byTitle(item, emit) {
					emit(item.attributes.title);
				}
			}
		};

		const txn = Transaction.create({dynamodb, indexes});

		sinon.stub(dynamodb.client, '_request').callsFake(function (target) {
			switch (target) {
			case 'GetItem':
				return Promise.resolve({});
			case 'PutItem':
				return Promise.resolve({});
			case 'BatchWriteItem':
				return Promise.reject(new Error('test error'));
			default:
				return Promise.reject(new Error(`unexpected _request() target: '${target}'`));
			}
		});

		const object = {
			scope: 'some_scope',
			type: 'some_type',
			id: 'some_id',
			attributes: {
				title: 'Foo'
			}
		};

		let error;

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				throw new Error('should not have resolved');
			}).catch(function (err) {
				error = err;
				return null;
			}).then(done);
		});

		t.it('rejects with the expected exception', () => {
			assert.isEqual('Error during Transaction#updateOrCreateItem(): Error in DynamoDB#updateIndexEntries(): test error', error.message);
		});
	});

	t.describe('with error in BatchWriteItem during update', (t) => {
		const emitter = new EventEmitter();

		const dynamodb = DynamoDb.create({
			emitter,
			awsRegion,
			awsAccessKey,
			awsSecretKey,
			tablePrefix
		});

		const indexes = {
			some_type: {
				byTitle(item, emit) {
					emit(item.attributes.title);
				}
			}
		};

		const txn = Transaction.create({dynamodb, indexes});

		sinon.stub(dynamodb.client, '_request').callsFake(function (target) {
			switch (target) {
			case 'GetItem':
				return Promise.resolve({Item: {
					_scope: {S: 'some_scope'},
					_type: {S: 'some_type'},
					_id: {S: 'some_id'},
					_created: {S: new Date().toISOString()},
					_updated: {S: new Date().toISOString()}
				}});
			case 'PutItem':
				return Promise.resolve({});
			case 'Query':
				return Promise.resolve({Items: []});
			case 'BatchWriteItem':
				return Promise.reject(new Error('test error'));
			default:
				return Promise.reject(new Error(`unexpected _request() target: '${target}'`));
			}
		});

		const object = {
			scope: 'some_scope',
			type: 'some_type',
			id: 'some_id',
			attributes: {
				title: 'Foo'
			}
		};

		let error;

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				throw new Error('should not have resolved');
			}).catch(function (err) {
				error = err;
				return null;
			}).then(done);
		});

		t.it('rejects with the expected exception', () => {
			assert.isEqual('Error during Transaction#updateOrCreateItem(): Error in DynamoDB#updateIndexEntries(): test error', error.message);
		});
	});
};
