'use strict';

const {StackedError} = require('kixx');
const Entity = require('./entity');
const {assert, mergeDeep} = require('kixx/library');


const INDEX_KEY_TYPE_ERROR = 'INDEX_KEY_TYPE_ERROR';


function getEntity(object, options) {
	const {scope, type, id} = object;
	const key = Entity.createKey(scope, type, id);

	return this.dynamodb.getEntity(key, options).then((res) => {
		const item = Entity.fromDatabaseRecord(res.entity);
		return {item};
	});
}

function saveEntity(object, options) {
	const entity = Entity.fromPublicObject(object);

	return this.dynamodb.setEntity(entity, options).then((res) => {
		const item = Entity.fromDatabaseRecord(res.entity);
		return {item};
	});
}

function getIndexEntries(object, options) {
	const {scope, type, id} = object;
	const key = IndexEntry.createKey(scope, type, id);

	return this.dynamodb.getIndexEntries(key, options).then((res) => {
		const items = res.entries.map(IndexEntry.fromDatabaseRecord);
		return {items};
	});
}

function createIndexEntries(entries, options) {
	return this.dynamodb.updateIndexEntries([], entries, options);
}

function updateIndexEntries(toRemove, toCreate, options) {
	const toRemoveHash = {};
	const entriesToCreate = [];

	for (let i = toRemove.length - 1; i >= 0; i--) {
		const entry = toRemove[i];
		toRemoveHash[entry.getFullKey()] = entry;
	}

	for (let i = toCreate.length - 1; i >= 0; i--) {
		const entry = toCreate[i];
		const key = entry.getFullKey();
		if (toRemoveHash[key]) {
			delete toRemoveHash[key];
		} else {
			entriesToCreate.push(entry);
		}
	}

	const entriesToRemove = Object.keys(toRemoveHash).map((key) => toRemoveHash[key]);

	return this.dynamodb.updateIndexEntries(entriesToRemove, entriesToCreate, options);
}

function composeIndexEntries(object, options) {
	const {scope, type, id, attributes, meta} = object;
	const indexes = this.indexes[type];

	const indexNames = Object.keys(indexes);

	// Use a hash to collect entries so we don't get any duplicates.
	const entries = {};

	for (let i = indexNames.length - 1; i >= 0; i--) {
		const indexName = indexNames[i];
		const mapper = indexes[indexName];

		mapper(clone(object), function emit(indexKey) {
			const type = typeof indexKey;
			if (type !== 'string' && type !== 'number') {
				const error = new TypeError('An index key value must be a String or Number');
				error.code = INDEX_KEY_TYPE_ERROR;
				throw error;
			}

			const entry = IndexEntry.create({
				scope,
				type,
				id,
				attributes,
				meta,
				indexName,
				indexKey
			});

			entries[entry.getFullKey()] = entry;
		});
	}

	// Convert entries hash into an Array.
	return Object.keys(entries).map((key) => entries[key]);
}

class Transaction {
	constructor(options) {
		Object.defineProperties(this, {
			dynamodb: {
				enumerable: true,
				value: options.dynamodb
			},
			indexes: {
				value: options.indexes
			},
			_getEntity: {
				value: getEntity.bind(this)
			},
			_saveEntity: {
				value: saveEntity.bind(this)
			}
		});
	}

	updateOrCreateItem(object, options = {}) {
		const {scope, type, id, attributes} = object;

		assert.isNonEmptyString(scope, 'updateOrCreateItem() object.scope');
		assert.isNonEmptyString(type, 'updateOrCreateItem() object.type');
		assert.isNonEmptyString(id, 'updateOrCreateItem() object.id');

		// Get the entity.
		// If there is no existing entity:
		//  - Run the entity through the index mapping functions.
		//  - Create it.
		//  - Create the index entries.
		// If there is an existing entity:
		//  - Merge it.
		//  - Save it.
		//  - Query old index entries.
		//  - Run new entity through index mapping functions.
		//  - Remove existing entries and create new ones in single batch write operation.

		return getEntity().then((entity) => {
			if (entity) {
				const meta = Object.assign({}, entity.meta, {
					updated: new Date().toISOString()
				});

				return saveEntity({
					scope,
					type,
					id,
					attributes: mergeDeep(entity.attributes, attributes),
					meta
				});
			}
		});
	}

	getItem(object, options = {}) {
		const {scope, type, id} = object;

		assert.isNonEmptyString(scope, 'getItem() object.scope');
		assert.isNonEmptyString(type, 'getItem() object.type');
		assert.isNonEmptyString(id, 'getItem() object.id');

		const key = Entity.createKey(scope, type, id);

		return this.dynamodb.getEntity(key, options).then((res) => {
			const item = res.entity ? Entity.fromDatabaseRecord(res.entity).toPublicItem() : null;
			return {item};
		}, (err) => {
			throw new StackedError(
				`DynamoDB error during Transaction#getItem()`,
				err
			);
		});
	}
}

module.exports = Transaction;
