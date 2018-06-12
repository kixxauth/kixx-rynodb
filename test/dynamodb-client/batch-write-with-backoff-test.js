'use strict';

const Promise = require('bluebird');
const EventEmitter = require('events');
const {DynamoDbClient} = require('../../index');
const sinon = require('sinon');
const {assert} = require('kixx/library');

module.exports = function (t) {
	t.describe('nominal case', (t) => {
		let emitter;
		let client;
		let requestContext;

		const warningListener = sinon.spy();

		const response = Object.freeze({UnprocessedItems: {}});

		const target = 'SomeTarget';
		const params = Object.freeze({RequestItems: {SomeTable: 1}});
		const options = Object.freeze({OPTIONS: true});

		let result;


		t.before(function (done) {
			emitter = new EventEmitter();
			emitter.on('warning', warningListener);
			client = DynamoDbClient.create({emitter});

			sinon.stub(client, '_request').callsFake(function () {
				requestContext = this;
				return Promise.resolve(response);
			});

			return client.batchWriteWithBackoff(target, params, options)
				.then(function (res) {
					result = res;
					return done();
				})
				.catch(done);
		});

		t.it('calls _request() only once', () => {
			assert.isOk(client._request.calledOnce);
		});

		t.it('calls _request() with expected args', () => {
			const [targetArg, optionsArg, paramsArg] = client._request.args[0];
			assert.isEqual(target, targetArg, 'target');
			assert.isEqual(emitter, optionsArg.emitter, 'options.emitter');
			assert.isEqual(client.requestTimeout, optionsArg.requestTimeout, 'options.requestTimeout');
			assert.isEqual(client.operationTimeout, optionsArg.operationTimeout, 'options.operationTimeout');
			assert.isEqual(client.backoffMultiplier, optionsArg.backoffMultiplier, 'options.backoffMultiplier');
			assert.isEqual(params, paramsArg, 'params');
		});

		t.it('calls _request() with expected context', () => {
			assert.isEqual(client, requestContext);
		});

		t.it('returns the response', () => {
			assert.isEqual(response, result);
		});

		t.it('does not emit a warning', () => {
			assert.isEqual(0, warningListener.callCount);
		});
	});

	t.describe('with unexpected error', (t) => {
		let emitter;
		let client;

		const warningListener = sinon.spy();

		const error = new Error('Rejected Error');

		const target = 'SomeTarget';
		const params = Object.freeze({RequestItems: {SomeTable: 1}});
		const options = {};

		let result;

		t.before(function (done) {
			emitter = new EventEmitter();
			emitter.on('warning', warningListener);
			client = DynamoDbClient.create({emitter});

			sinon.stub(client, '_request').returns(Promise.reject(error));

			return client.batchWriteWithBackoff(target, params, options)
				.then(function (res) {
					throw new Error('Should not call success callback');
				})
				.catch(function (err) {
					result = err;
					return done();
				});
		});

		t.it('rejects with the error', () => {
			assert.isEqual(error, result);
		});

		t.it('does not emit a warning', () => {
			assert.isEqual(0, warningListener.callCount);
		});
	});

	t.describe('with ProvisionedThroughputExceededException and UnprocessedItems', (t) => {
		let emitter;
		let client;

		const warningListener = sinon.spy();

		const error = new Error('Throughput Error');
		error.code = 'ProvisionedThroughputExceededException';

		const response = Object.freeze({UnprocessedItems: {}});

		const target = 'SomeTarget';
		const params = Object.freeze({RequestItems: {SomeTable: 1}});
		const options = {};

		let delay1 = 0;
		let delay2 = 0;
		let delay3 = 0;
		let delay4 = 0;
		let delay5 = 0;

		let result;

		t.before(function (done) {
			const start = Date.now();

			emitter = new EventEmitter();
			emitter.on('warning', warningListener);
			client = DynamoDbClient.create({emitter});

			sinon.stub(client, '_request')
				.onCall(0).callsFake(() => {
					return Promise.reject(error);
				})
				.onCall(1).callsFake(() => {
					delay1 = Date.now() - start;
					return Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}}));
				})
				.onCall(2).callsFake(() => {
					delay2 = Date.now() - start;
					return Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}}));
				})
				.onCall(3).callsFake(() => {
					delay3 = Date.now() - start;
					return Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}}));
				})
				.onCall(4).callsFake(() => {
					delay4 = Date.now() - start;
					return Promise.reject(error);
				})
				.onCall(5).callsFake(() => {
					delay5 = Date.now() - start;
					return Promise.resolve(response);
				});

			return client.batchWriteWithBackoff(target, params, options)
				.then(function (res) {
					result = res;
					return done();
				})
				.catch(done);
		});

		t.it('returns the response', () => {
			assert.isEqual(response, result);
		});

		t.it('emits 2 warnings', () => {
			assert.isEqual(2, warningListener.callCount);
			const [args1, args2] = warningListener.args;
			assert.isEqual('DynamoDB ProvisionedThroughputExceededException during SomeTarget on table SomeTable', args1[0].message);
			assert.isEqual('DynamoDB UnprocessedItems during SomeTarget on table SomeTable', args2[0].message);
		});

		t.it('uses backoff delays', () => {
			assert.isGreaterThan(100, delay1);
			assert.isLessThan(150, delay1);

			assert.isGreaterThan(100 + 200, delay2);
			assert.isLessThan(350, delay2);

			assert.isGreaterThan(100 + 200 + 400, delay3);
			assert.isLessThan(750, delay3);

			assert.isGreaterThan(100 + 200 + 400 + 800, delay4);
			assert.isLessThan(1550, delay4);

			assert.isGreaterThan(100 + 200 + 400 + 800 + 1600, delay5);
			assert.isLessThan(3150, delay5);
		});
	});

	t.describe('with UnprocessedItems => operation timeout', (t) => {
		let emitter;
		let client;

		const warningListener = sinon.spy();

		const throughputError = new Error('Throughput Error');
		throughputError.code = 'ProvisionedThroughputExceededException';

		let error;

		const target = 'SomeTarget';
		const params = Object.freeze({RequestItems: {SomeTable: 1}});
		const options = {operationTimeout: 1};

		const response = Object.freeze({UnprocessedItems: {}});

		t.before(function (done) {
			emitter = new EventEmitter();
			emitter.on('warning', warningListener);
			client = DynamoDbClient.create({emitter});

			sinon.stub(client, '_request')
				.onCall(0).returns(Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}})))
				.onCall(1).returns(Promise.resolve(response));

			return client.batchWriteWithBackoff(target, params, options)
				.then(function () {
					throw new Error('Should not resolve');
				})
				.catch(function (err) {
					error = err;
					return done();
				});
		});

		t.it('rejects with an OPERATION_TIMEOUT Error', () => {
			assert.isDefined(error, 'is defined');
			assert.isEqual('Error', error.name);
			assert.isEqual('OPERATION_TIMEOUT', error.code);
			assert.isEqual('DynamoDB client operation timeout error during SomeTarget due to throttling on table SomeTable', error.message);
		});
	});

	t.describe('with ProvisionedThroughputExceededException => operation timeout', (t) => {
		let emitter;
		let client;

		const warningListener = sinon.spy();

		const throughputError = new Error('Throughput Error');
		throughputError.code = 'ProvisionedThroughputExceededException';

		let error;

		const target = 'SomeTarget';
		const params = Object.freeze({RequestItems: {SomeTable: 1}});
		const options = {operationTimeout: 1};

		t.before(function (done) {
			emitter = new EventEmitter();
			emitter.on('warning', warningListener);
			client = DynamoDbClient.create({emitter});

			sinon.stub(client, '_request')
				.onCall(0).returns(Promise.reject(throughputError))
				.onCall(1).returns(Promise.resolve({}));

			return client.batchWriteWithBackoff(target, params, options)
				.then(function () {
					throw new Error('Should not resolve');
				})
				.catch(function (err) {
					error = err;
					return done();
				});
		});

		t.it('rejects with an OPERATION_TIMEOUT Error', () => {
			assert.isDefined(error, 'is defined');
			assert.isEqual('Error', error.name);
			assert.isEqual('OPERATION_TIMEOUT', error.code);
			assert.isEqual('DynamoDB client operation timeout error during SomeTarget due to throttling on table SomeTable', error.message);
		});
	});

	t.describe('with ProvisionedThroughputExceededException and UnprocessedItems => operation timeout = 4s', (t) => {
		let emitter;
		let client;

		const warningListener = sinon.spy();

		const throughputError = new Error('Throughput Error');
		throughputError.code = 'ProvisionedThroughputExceededException';

		const response = Object.freeze({UnprocessedItems: {}});

		let result;
		let duration = 0;

		const target = 'SomeTarget';
		const params = Object.freeze({RequestItems: {SomeTable: 1}});
		const options = {operationTimeout: 4000};

		t.before(function (done) {
			const start = Date.now();

			emitter = new EventEmitter();
			emitter.on('warning', warningListener);
			client = DynamoDbClient.create({emitter});

			sinon.stub(client, '_request')
				.onCall(0).callsFake(() => Promise.reject(throughputError))
				.onCall(1).returns(Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}})))
				.onCall(2).returns(Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}})))
				.onCall(3).callsFake(() => Promise.reject(throughputError))
				.onCall(4).returns(Promise.resolve(response));

			return client.batchWriteWithBackoff(target, params, options)
				.then(function (res) {
					result = res;
					duration = Date.now() - start;
					return done();
				})
				.catch(done);
		});

		t.it('has a backoff delay', () => {
			assert.isGreaterThan(100 + 200 + 400 + 800, duration);
			assert.isLessThan(1550, duration);
		});

		t.it('returns the response', () => {
			assert.isEqual(response, result);
		});

		t.it('emits 2 warnings', () => {
			assert.isEqual(2, warningListener.callCount);
			const [args1, args2] = warningListener.args;
			assert.isEqual('DynamoDB ProvisionedThroughputExceededException during SomeTarget on table SomeTable', args1[0].message);
			assert.isEqual('DynamoDB UnprocessedItems during SomeTarget on table SomeTable', args2[0].message);
		});
	});

	t.describe('with ProvisionedThroughputExceededException and UnprocessedItems => operation timeout fail', (t) => {
		let emitter;
		let client;

		const warningListener = sinon.spy();

		const throughputError = new Error('Throughput Error');
		throughputError.code = 'ProvisionedThroughputExceededException';

		const target = 'SomeTarget';
		const params = Object.freeze({RequestItems: {SomeTable: 1}});
		const options = {operationTimeout: 3000};

		let delay1 = 0;
		let delay2 = 0;
		let delay3 = 0;
		let delay4 = 0;

		let resultError;

		t.before(function (done) {
			const start = Date.now();

			emitter = new EventEmitter();
			emitter.on('warning', warningListener);
			client = DynamoDbClient.create({emitter});

			sinon.stub(client, '_request')
				.onCall(0).returns(Promise.reject(throughputError))
				.onCall(1).callsFake(() => {
					delay1 = Date.now() - start;
					return Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}}));
				})
				.onCall(2).callsFake(() => {
					delay2 = Date.now() - start;
					return Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}}));
				})
				.onCall(3).callsFake(() => {
					delay3 = Date.now() - start;
					return Promise.reject(throughputError);
				})
				.onCall(4).callsFake(() => {
					delay4 = Date.now() - start;
					return Promise.resolve(Object.freeze({UnprocessedItems: {SomeTable: 1}}));
				});

			return client.batchWriteWithBackoff(target, params, options)
				.then(function (res) {
					throw new Error('Should not resolve');
				})
				.catch(function (err) {
					resultError = err;
					return done();
				});
		});

		t.it('calls _request() 5 times', () => {
			assert.isEqual(5, client._request.callCount);
		});

		t.it('rejects with an OPERATION_TIMEOUT Error', () => {
			assert.isDefined(resultError, 'is defined');
			assert.isEqual('Error', resultError.name);
			assert.isEqual('OPERATION_TIMEOUT', resultError.code);
			assert.isEqual('DynamoDB client operation timeout error during SomeTarget due to throttling on table SomeTable', resultError.message);
		});

		t.it('emits 2 warnings', () => {
			assert.isEqual(2, warningListener.callCount);
			const [args1, args2] = warningListener.args;
			assert.isEqual('DynamoDB ProvisionedThroughputExceededException during SomeTarget on table SomeTable', args1[0].message);
			assert.isEqual('DynamoDB UnprocessedItems during SomeTarget on table SomeTable', args2[0].message);
		});

		t.it('uses backoff delays', () => {
			assert.isGreaterThan(100, delay1);
			assert.isLessThan(150, delay1);

			assert.isGreaterThan(100 + 200, delay2);
			assert.isLessThan(350, delay2);

			assert.isGreaterThan(100 + 200 + 400, delay3);
			assert.isLessThan(750, delay3);

			assert.isGreaterThan(100 + 200 + 400 + 800, delay4);
			assert.isLessThan(1550, delay4);
		});
	});
};
