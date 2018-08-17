'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const {Transaction, DynamoDb} = require('../../index');
const sinon = require('sinon');
const {assert, isObject} = require('kixx/library');

module.exports = function (t) {
	const awsRegion = 'AWS_REGION';
	const awsAccessKey = 'AWS_ACCESS_KEY';
	const awsSecretKey = 'AWS_SECRET_KEY';
	const tablePrefix = 'ttt';

	t.describe('create item with index entries', (t) => {
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
				return Promise.resolve({UnprocessedItems: {}});
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

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				done();
				return null;
			}, done);
		});

		t.it('calls DynamoDbClient#_request() expected number of times', () => {
			assert.isEqual(3, dynamodb.client._request.callCount);
		});

		t.it('calls _request() to GetItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(0).args;

			assert.isEqual('GetItem', target);
			assert.isOk(isObject(options));

			const {TableName, Key} = params;

			assert.isEqual(`${tablePrefix}_root_entities`, TableName);
			assert.isEqual('some_id', Key._id.S);
			assert.isEqual('some_scope:some_type', Key._scope_type_key.S);
		});

		t.it('calls _request() to PutItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(1).args;

			assert.isEqual('PutItem', target);
			assert.isOk(isObject(options));

			const {TableName, Item} = params;

			assert.isEqual(`${tablePrefix}_root_entities`, TableName);
			assert.isEqual('some_id', Item._id.S);
			assert.isEqual('some_scope', Item._scope.S);
			assert.isEqual('some_type', Item._type.S);
			assert.isEqual('some_scope:some_type', Item._scope_type_key.S);
			assert.isEqual('Foo', Item.title.S);
		});

		t.it('calls _request() to BatchWriteItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(2).args;

			assert.isEqual('BatchWriteItem', target);
			assert.isOk(isObject(options));

			const entries = params.RequestItems.ttt_index_entries;

			assert.isEqual(1, entries.length);

			const {Item} = entries[0].PutRequest;

			assert.isEqual('some_id', Item._id.S);
			assert.isEqual('some_scope', Item._scope.S);
			assert.isEqual('some_type', Item._type.S);
			assert.isEqual('Foo', Item.title.S);
			assert.isEqual('byTitle', Item._index_name.S);
			assert.isEqual('Foo', Item._index_key.S);
			assert.isEqual('some_scope:some_type:some_id', Item._subject_key.S);
			assert.isEqual('byTitle:Foo', Item._unique_key.S);
			assert.isEqual('some_scope:byTitle', Item._scope_index_name.S);
		});
	});

	t.describe('update item with new index entries', (t) => {
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
					_scope_type_key: {S: 'some_scope:some_type'},
					_created: {S: new Date().toISOString()},
					_updated: {S: new Date().toISOString()},
					_meta: {M: {}}
				}});
			case 'Query':
				return Promise.resolve({Items: []});
			case 'PutItem':
				return Promise.resolve({});
			case 'BatchWriteItem':
				return Promise.resolve({UnprocessedItems: {}});
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

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				done();
				return null;
			}, done);
		});

		t.it('calls DynamoDbClient#_request() expected number of times', () => {
			assert.isEqual(4, dynamodb.client._request.callCount);
		});

		t.it('calls _request() to GetItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(0).args;

			assert.isEqual('GetItem', target);
			assert.isOk(isObject(options));

			const {TableName, Key} = params;

			assert.isEqual(`${tablePrefix}_root_entities`, TableName);
			assert.isEqual('some_id', Key._id.S);
			assert.isEqual('some_scope:some_type', Key._scope_type_key.S);
		});

		t.it('calls _request() to PutItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(1).args;

			assert.isEqual('PutItem', target);
			assert.isOk(isObject(options));

			const {TableName, Item} = params;

			assert.isEqual(`${tablePrefix}_root_entities`, TableName);
			assert.isEqual('some_id', Item._id.S);
			assert.isEqual('some_scope', Item._scope.S);
			assert.isEqual('some_type', Item._type.S);
			assert.isEqual('some_scope:some_type', Item._scope_type_key.S);
			assert.isEqual('Foo', Item.title.S);
		});

		t.it('calls _request() to Query', () => {
			const [target, options, params] = dynamodb.client._request.getCall(2).args;

			assert.isEqual('Query', target);
			assert.isOk(isObject(options));

			const {
				TableName,
				ExpressionAttributeNames,
				ExpressionAttributeValues,
				ExclusiveStartKey,
				KeyConditionExpression
			} = params;

			assert.isEqual(`${tablePrefix}_index_entries`, TableName);
			assert.isEqual('_subject_key', ExpressionAttributeNames['#sk']);
			assert.isEqual('some_scope:some_type:some_id', ExpressionAttributeValues[':sk'].S);
			assert.isEqual(null, ExclusiveStartKey);
			assert.isEqual('#sk = :sk', KeyConditionExpression);
		});

		t.it('calls _request() to BatchWriteItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(3).args;

			assert.isEqual('BatchWriteItem', target);
			assert.isOk(isObject(options));

			const entries = params.RequestItems.ttt_index_entries;

			assert.isEqual(1, entries.length);

			const {Item} = entries[0].PutRequest;

			assert.isEqual('some_id', Item._id.S);
			assert.isEqual('some_scope', Item._scope.S);
			assert.isEqual('some_type', Item._type.S);
			assert.isEqual('Foo', Item.title.S);
			assert.isEqual('byTitle', Item._index_name.S);
			assert.isEqual('Foo', Item._index_key.S);
			assert.isEqual('some_scope:some_type:some_id', Item._subject_key.S);
			assert.isEqual('byTitle:Foo', Item._unique_key.S);
			assert.isEqual('some_scope:byTitle', Item._scope_index_name.S);
		});
	});

	t.describe('update item with same index entries', (t) => {
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
					_scope_type_key: {S: 'some_scope:some_type'},
					_created: {S: new Date().toISOString()},
					_updated: {S: new Date().toISOString()},
					_meta: {M: {}}
				}});
			case 'Query':
				return Promise.resolve({Items: [{
					_scope: {S: 'some_scope'},
					_type: {S: 'some_type'},
					_id: {S: 'some_id'},
					_index_name: {S: 'byTitle'},
					_index_key: {S: 'Foo'},
					_subject_key: {S: 'some_scope:some_type:some_id'},
					_unique_key: {S: 'byTitle:Foo'},
					_scope_index_name: {S: 'some_scope:byTitle'},
					_created: {S: new Date().toISOString()},
					_updated: {S: new Date().toISOString()},
					_meta: {M: {}}
				}]});
			case 'PutItem':
				return Promise.resolve({});
			case 'BatchWriteItem':
				return Promise.resolve({UnprocessedItems: {}});
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

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				done();
				return null;
			}, done);
		});

		t.it('calls DynamoDbClient#_request() expected number of times', () => {
			assert.isEqual(4, dynamodb.client._request.callCount);
		});

		t.it('calls _request() to GetItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(0).args;

			assert.isEqual('GetItem', target);
			assert.isOk(isObject(options));

			const {TableName, Key} = params;

			assert.isEqual(`${tablePrefix}_root_entities`, TableName);
			assert.isEqual('some_id', Key._id.S);
			assert.isEqual('some_scope:some_type', Key._scope_type_key.S);
		});

		t.it('calls _request() to PutItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(1).args;

			assert.isEqual('PutItem', target);
			assert.isOk(isObject(options));

			const {TableName, Item} = params;

			assert.isEqual(`${tablePrefix}_root_entities`, TableName);
			assert.isEqual('some_id', Item._id.S);
			assert.isEqual('some_scope', Item._scope.S);
			assert.isEqual('some_type', Item._type.S);
			assert.isEqual('some_scope:some_type', Item._scope_type_key.S);
			assert.isEqual('Foo', Item.title.S);
		});

		t.it('calls _request() to Query', () => {
			const [target, options, params] = dynamodb.client._request.getCall(2).args;

			assert.isEqual('Query', target);
			assert.isOk(isObject(options));

			const {
				TableName,
				ExpressionAttributeNames,
				ExpressionAttributeValues,
				ExclusiveStartKey,
				KeyConditionExpression
			} = params;

			assert.isEqual(`${tablePrefix}_index_entries`, TableName);
			assert.isEqual('_subject_key', ExpressionAttributeNames['#sk']);
			assert.isEqual('some_scope:some_type:some_id', ExpressionAttributeValues[':sk'].S);
			assert.isEqual(null, ExclusiveStartKey);
			assert.isEqual('#sk = :sk', KeyConditionExpression);
		});

		// Even if the indexes are the same, we still need to re-insert the index entries
		// because the entity attributes may have been updated.
		t.it('calls _request() to BatchWriteItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(3).args;

			assert.isEqual('BatchWriteItem', target);
			assert.isOk(isObject(options));

			const entries = params.RequestItems.ttt_index_entries;

			// There is no delete request.
			assert.isEqual(1, entries.length);

			const {Item} = entries[0].PutRequest;

			assert.isEqual('some_id', Item._id.S);
			assert.isEqual('some_scope', Item._scope.S);
			assert.isEqual('some_type', Item._type.S);
			assert.isEqual('Foo', Item.title.S);
			assert.isEqual('byTitle', Item._index_name.S);
			assert.isEqual('Foo', Item._index_key.S);
			assert.isEqual('some_scope:some_type:some_id', Item._subject_key.S);
			assert.isEqual('byTitle:Foo', Item._unique_key.S);
			assert.isEqual('some_scope:byTitle', Item._scope_index_name.S);
		});
	});

	t.describe('update item with different index entries', (t) => {
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
					_scope_type_key: {S: 'some_scope:some_type'},
					_created: {S: new Date().toISOString()},
					_updated: {S: new Date().toISOString()},
					_meta: {M: {}}
				}});
			case 'Query':
				return Promise.resolve({Items: [{
					_scope: {S: 'some_scope'},
					_type: {S: 'some_type'},
					_id: {S: 'some_id'},
					_index_name: {S: 'byTitle'},
					_index_key: {S: 'Bar'},
					_subject_key: {S: 'some_scope:some_type:some_id'},
					_unique_key: {S: 'byTitle:Bar'},
					_scope_index_name: {S: 'some_scope:byTitle'},
					_created: {S: new Date().toISOString()},
					_updated: {S: new Date().toISOString()},
					_meta: {M: {}}
				}]});
			case 'PutItem':
				return Promise.resolve({});
			case 'BatchWriteItem':
				return Promise.resolve({UnprocessedItems: {}});
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

		t.before(function (done) {
			txn.updateOrCreateItem(object).then(function () {
				done();
				return null;
			}, done);
		});

		t.it('calls DynamoDbClient#_request() expected number of times', () => {
			assert.isEqual(4, dynamodb.client._request.callCount);
		});

		t.it('calls _request() to GetItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(0).args;

			assert.isEqual('GetItem', target);
			assert.isOk(isObject(options));

			const {TableName, Key} = params;

			assert.isEqual(`${tablePrefix}_root_entities`, TableName);
			assert.isEqual('some_id', Key._id.S);
			assert.isEqual('some_scope:some_type', Key._scope_type_key.S);
		});

		t.it('calls _request() to PutItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(1).args;

			assert.isEqual('PutItem', target);
			assert.isOk(isObject(options));

			const {TableName, Item} = params;

			assert.isEqual(`${tablePrefix}_root_entities`, TableName);
			assert.isEqual('some_id', Item._id.S);
			assert.isEqual('some_scope', Item._scope.S);
			assert.isEqual('some_type', Item._type.S);
			assert.isEqual('some_scope:some_type', Item._scope_type_key.S);
			assert.isEqual('Foo', Item.title.S);
		});

		t.it('calls _request() to Query', () => {
			const [target, options, params] = dynamodb.client._request.getCall(2).args;

			assert.isEqual('Query', target);
			assert.isOk(isObject(options));

			const {
				TableName,
				ExpressionAttributeNames,
				ExpressionAttributeValues,
				ExclusiveStartKey,
				KeyConditionExpression
			} = params;

			assert.isEqual(`${tablePrefix}_index_entries`, TableName);
			assert.isEqual('_subject_key', ExpressionAttributeNames['#sk']);
			assert.isEqual('some_scope:some_type:some_id', ExpressionAttributeValues[':sk'].S);
			assert.isEqual(null, ExclusiveStartKey);
			assert.isEqual('#sk = :sk', KeyConditionExpression);
		});

		t.it('calls _request() to BatchWriteItem', () => {
			const [target, options, params] = dynamodb.client._request.getCall(3).args;

			assert.isEqual('BatchWriteItem', target);
			assert.isOk(isObject(options));

			const entries = params.RequestItems.ttt_index_entries;

			assert.isEqual(2, entries.length);

			const {Key} = entries[0].DeleteRequest;
			const {Item} = entries[1].PutRequest;

			assert.isEqual('some_scope:some_type:some_id', Key._subject_key.S);
			assert.isEqual('byTitle:Bar', Key._unique_key.S);

			assert.isEqual('some_id', Item._id.S);
			assert.isEqual('some_scope', Item._scope.S);
			assert.isEqual('some_type', Item._type.S);
			assert.isEqual('Foo', Item.title.S);
			assert.isEqual('byTitle', Item._index_name.S);
			assert.isEqual('Foo', Item._index_key.S);
			assert.isEqual('some_scope:some_type:some_id', Item._subject_key.S);
			assert.isEqual('byTitle:Foo', Item._unique_key.S);
			assert.isEqual('some_scope:byTitle', Item._scope_index_name.S);
		});
	});

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
			assert.isEqual('Error during Transaction#updateOrCreateItem(): Error in DynamoDB#createEntity(): test error', error.message);
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
			assert.isEqual('Error during Transaction#updateOrCreateItem(): Error in DynamoDB#updateEntity(): test error', error.message);
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
