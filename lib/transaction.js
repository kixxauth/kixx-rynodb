'use strict';

const Record = require('./record');
const IndexEntry = require('./index-entry');
const KixxAssert = require('kixx-assert');
const {assertIsObject} = require('./');
const {createIndexEntries} = require('./helpers');

const {assert} = KixxAssert;

class Transaction {
	// spec.dynamodb
	// spec.redis
	// spec.indexes
	constructor(spec) {
		Object.defineProperties(this, {
			_dynamodb: {
				value: spec.dynamodb
			},
			_redis: {
				value: spec.redis
			},
			_indexes: {
				value: spec.indexes
			}
		});
	}

	get(args, options = {}) {
		const {scope, type, id} = args || {};
		assert.isNonEmptyString(scope, 'args.scope');
		assert.isMatch(/^[a-zA-Z0-9-]{3,240}$/, scope, 'args.scope');
		assert.isNonEmptyString(type, 'args.type');
		assert.isMatch(/^[a-zA-Z0-9-]{3,240}$/, type, 'args.type');
		assert.isNonEmptyString(id, 'args.id');
		assert.isMatch(/^[a-zA-Z0-9-]{3,240}$/, id, 'args.id');

		const key = Record.create({scope, type, id}).toTableKey();

		return this._dynamodb.getEntity(key, options).then((res) => {
			return Record.fromDatabaseRecord(res.item).toPublic();
		});
	}

	set(args, options = {}) {
		const {scope, type, id, attributes, meta} = args || {};
		assert.isNonEmptyString(scope, 'args.scope');
		assert.isMatch(/^[a-zA-Z0-9-]{3,240}$/, scope, 'args.scope');
		assert.isNonEmptyString(type, 'args.type');
		assert.isMatch(/^[a-zA-Z0-9-]{3,240}$/, type, 'args.type');
		assert.isNonEmptyString(id, 'args.id');
		assert.isMatch(/^[a-zA-Z0-9-]{3,240}$/, id, 'args.id');
		assertIsObject(attributes, 'args.attributes');
		assertIsObject(meta, 'args.meta');

		const record = Record.fromPublic({
			scope,
			type,
			id,
			attributes,
			meta
		});

		const subjectKey = record.getSubjectKey();

		const indexEntries = createIndexEntries(this._indexes, record);

		const dynamodb = this._dynamodb;

		return dynamodb.getIndexEntriesBySubject(subjectKey, options)
			.then((res) => {
				if (!res.items || res.items.length === 0) return null;

				const keys = res.items
					.map(IndexEntry.fromDatabaseRecord)
					.map((entry) => entry.toTableKey());

				return dynamodb.batchRemoveIndexEntries(keys, options);
			})
			.then(() => {
				return dynamodb.setEntity(record.toDatabaseRecord(), options);
			})
			.then(() => {
				return dynamodb.batchSetIndexEntries(indexEntries, options);
			})
			.then(() => {
				return record.toPublic();
			});
	}

	// spec.dynamodb
	// spec.redis
	// spec.indexes
	static create(options) {
		return new Transaction(options);
	}
}

module.exports = Transaction;
