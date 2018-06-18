'use strict';

const Promise = require('bluebird');
const Rynodb = require('../index');
const {reportFullStackTrace} = require('kixx');
const {assert, clone, deepFreeze} = require('kixx/library');
const tools = require('./tools');

const TABLE_PREFIX = tools.TABLE_PREFIX;

const debug = tools.debug('index-and-query');

const {awsAccessKey, awsSecretKey, awsRegion} = tools.getAwsCredentials();

const indexes = {
	test_item: {
		byTag: function (item, emit) {
			item.attributes.tags.forEach(emit);
		},
		byName: function (item, emit) {
			emit(item.attributes.name.slice(0, 10).toLowerCase().padEnd(10, '0'));
		}
	}
};

const {createTransaction, query} = Rynodb.create({
	indexes,
	tablePrefix: TABLE_PREFIX,
	awsRegion,
	awsAccessKey,
	awsSecretKey
});

const uid = (function () {
	let n = 0;
	return function uid() {
		n += 1;
		return `some-uuid-thingy-123-idx-${n}`;
	};
}());

const item1 = deepFreeze({
	scope: 'some_scope',
	type: 'test_item',
	id: uid(),
	attributes: {
		name: 'Daffy Duck',
		tags: ['foo', 'bar']
	}
});

const item2 = deepFreeze({
	scope: 'some_scope',
	type: 'test_item',
	id: uid(),
	attributes: {
		name: 'Porky Pig',
		tags: ['foo', 'baz']
	}
});

const tests = [];

tests.push(function createItem() {
	debug('create items to index');

	const txn = createTransaction();

	// If the target items already exist, this will update them back to their
	// original state.
	return Promise.all([
		txn.updateOrCreateItem(item1),
		txn.updateOrCreateItem(item2)
	]);
});

tests.push(function queryByTag() {
	debug('query items by tag');

	const params = {
		scope: 'some_scope',
		index: 'byTag',
		operator: 'equals',
		value: 'foo',
		cursor: null,
		limit: 1
	};

	debug('query with limit: 1');
	return query(params).then((res) => {
		const {items, cursor} = res;

		assert.isEqual(1, res.items.length);

		const [item] = items;

		assert.isEqual(2, Object.keys(item.attributes).length);

		assert.isEqual('byTag:foo', cursor._unique_key.S);
		assert.isEqual('some_scope:byTag', cursor._scope_index_name.S);
		assert.isEqual('foo', cursor._index_key.S);
		assert.isEqual('some_scope:test_item:some-uuid-thingy-123-idx-2', cursor._subject_key.S);

		const params = {
			scope: 'some_scope',
			index: 'byTag',
			operator: 'equals',
			value: 'foo',
			cursor
		};

		debug('query with cursor');

		return query(params).then((res) => {
			const {items, cursor} = res;

			assert.isEqual(1, res.items.length);

			const [item] = items;

			assert.isEqual(2, Object.keys(item.attributes).length);

			assert.isEqual(null, cursor);
			return null;
		});
	});
});

tests.push(function queryByName() {
	debug('query items by name');

	const params = {
		scope: 'some_scope',
		index: 'byName',
		operator: 'begins_with',
		value: 'p'
	};

	return query(params).then((res) => {
		const {items, cursor} = res;

		assert.isEqual(1, res.items.length);

		const [item] = items;
		const {attributes} = item;

		assert.isEqual(2, Object.keys(attributes).length);
		assert.isEqual('Porky Pig', attributes.name);

		assert.isEqual(null, cursor);

		return null;
	});
});

tests.push(function updateAnItemAndQuery() {
	debug('update item and query for it');

	const updatedItem = clone(item1);
	updatedItem.attributes.tags = ['baz'];

	const txn = createTransaction();

	return txn.updateOrCreateItem(updatedItem).then(() => {
		const params = {
			scope: 'some_scope',
			index: 'byTag',
			operator: 'equals',
			value: 'bar'
		};

		return query(params).then((res) => {
			const {items, cursor} = res;

			assert.isEqual(0, items.length);
			assert.isEqual(null, cursor);

			return null;
		});
	});
});

tests.push(function deleteAnItemAndQuery() {
	debug('delete item and query for it');

	const txn = createTransaction();

	const {scope, type, id} = item2;
	const key = {scope, type, id};

	return txn.deleteItem(key).then(() => {
		const params = {
			scope: 'some_scope',
			index: 'byName',
			operator: 'begins_with',
			value: 'p'
		};

		return query(params).then((res) => {
			const {items, cursor} = res;

			assert.isEqual(0, items.length);
			assert.isEqual(null, cursor);

			return null;
		});
	});
});

exports.main = function main() {
	return tests.reduce((promise, test) => {
		return promise.then(() => test());
	}, Promise.resolve(null));
};

/* eslint-disable no-console */
if (require.main === module) {
	exports.main().then(() => {
		console.log('Done :-)');
		return null;
	}).catch((err) => {
		console.error('Runtime Error:');
		reportFullStackTrace(err);
	});
}
/* eslint-enable */
