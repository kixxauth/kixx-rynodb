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

			return client.requestWithBackoff(target, params, options)
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
};
