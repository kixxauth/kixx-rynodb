'use strict';

const Promise = require('bluebird');
const {StackedError} = require('kixx');
const Entity = require('./entity');
const IndexEntry = require('./index-entry');
const {assert, clone} = require('kixx/library');


const INDEX_KEY_TYPE_ERROR = 'INDEX_KEY_TYPE_ERROR';


function getEntity(scope, type, id, options) {
	const key = Entity.createKey(scope, type, id);

	return this.dynamodb.getEntity(key, options).then(function (res) {
		return res.entity ? Entity.fromDatabaseRecord(res.entity) : null;
	});
}

function saveEntity(entity, options) {
	return this.dynamodb.setEntity(entity, options).then(function (res) {
		return Entity.fromDatabaseRecord(res.entity);
	});
}

function getIndexEntries(scope, type, id, options) {
	const key = IndexEntry.partitionKey(scope, type, id);

	return this.dynamodb.getIndexEntries(key, options).then(function (res) {
		return res.entries.map(IndexEntry.fromDatabaseRecord);
	});
}

function createIndexEntries(entries, options) {
	return this.dynamodb.updateIndexEntries([], entries, options);
}

function deleteIndexEntries(entries, options) {
	return this.dynamodb.updateIndexEntries(entries, [], options);
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

function composeIndexEntries(object) {
	const {scope, type, id, attributes, meta} = object;
	const indexes = this.indexes[type] || {};

	const indexNames = Object.keys(indexes);

	// Use a hash to collect entries so we don't get any duplicates.
	const entries = {};

	for (let i = indexNames.length - 1; i >= 0; i--) {
		const indexName = indexNames[i];
		const mapper = indexes[indexName];

		mapper(clone(object), function emit(indexKey) {
			const jsType = typeof indexKey;
			if (jsType !== 'string' && jsType !== 'number') {
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
				value: options.indexes || {}
			},
			_getEntity: {
				value: getEntity.bind(this)
			},
			_saveEntity: {
				value: saveEntity.bind(this)
			},
			_getIndexEntries: {
				value: getIndexEntries.bind(this)
			},
			_createIndexEntries: {
				value: createIndexEntries.bind(this)
			},
			_deleteIndexEntries: {
				value: deleteIndexEntries.bind(this)
			},
			_updateIndexEntries: {
				value: updateIndexEntries.bind(this)
			},
			_composeIndexEntries: {
				value: composeIndexEntries.bind(this)
			}
		});
	}

	updateOrCreateItem(object, options = {}) {
		const {scope, type, id} = object;

		assert.isNonEmptyString(scope, 'updateOrCreateItem() object.scope');
		assert.isNonEmptyString(type, 'updateOrCreateItem() object.type');
		assert.isNonEmptyString(id, 'updateOrCreateItem() object.id');

		const {
			_getEntity,
			_saveEntity,
			_composeIndexEntries,
			_getIndexEntries,
			_updateIndexEntries,
			_createIndexEntries
		} = this;

		function updateEntity(existingEntity, newEntity) {
			const entity = existingEntity.mergeIn(newEntity).setUpdate();

			const promises = [
				_saveEntity(entity, options),
				_getIndexEntries(scope, type, id, options)
			];

			return Promise.all(promises).then(function ([entity, existingIndexEntries]) {
				const newIndexEntries = _composeIndexEntries(entity.toPlainObject());

				return _updateIndexEntries(existingIndexEntries, newIndexEntries, options).then(function () {
					return {item: entity.toPublicItem()};
				});
			});
		}

		function createEntity(newEntity) {
			const indexEntries = _composeIndexEntries(newEntity.toPlainObject());

			// Save the new entity and its index entries in parallel.
			const promises = [
				_saveEntity(newEntity, options),
				_createIndexEntries(indexEntries, options)
			];

			return Promise.all(promises).then(function ([entity]) {
				return {item: entity.toPublicItem()};
			});
		}

		return _getEntity(scope, type, id, options)
			.then(function (existingEntity) {
				if (existingEntity) {
					return updateEntity(existingEntity, Entity.fromPublicObject(object), options);
				} else {
					return createEntity(Entity.fromPublicObject(object), options);
				}
			})
			.catch(function (err) {
				return Promise.reject(new StackedError(
					`Error during Transaction#updateOrCreateItem()`,
					err
				));
			});
	}

	getItem(object, options = {}) {
		const {scope, type, id} = object;

		assert.isNonEmptyString(scope, 'getItem() object.scope');
		assert.isNonEmptyString(type, 'getItem() object.type');
		assert.isNonEmptyString(id, 'getItem() object.id');

		const key = Entity.createKey(scope, type, id);

		return this.dynamodb.getEntity(key, options)
			.then(function (res) {
				const item = res.entity ? Entity.fromDatabaseRecord(res.entity).toPublicItem() : null;
				return {item};
			})
			.catch(function (err) {
				return Promise.reject(new StackedError(
					`DynamoDB error during Transaction#getItem()`,
					err
				));
			});
	}

	deleteItem(object, options = {}) {
		const {scope, type, id} = object;

		assert.isNonEmptyString(scope, 'deleteItem() object.scope');
		assert.isNonEmptyString(type, 'deleteItem() object.type');
		assert.isNonEmptyString(id, 'deleteItem() object.id');

		const {dynamodb, _deleteIndexEntries, _getIndexEntries} = this;

		return _getIndexEntries(scope, type, id, options)
			.then(function (indexEntries) {
				const key = Entity.createKey(scope, type, id);

				const promises = [
					_deleteIndexEntries(indexEntries, options),
					dynamodb.deleteEntity(key, options)
				];

				return Promise.all(promises).then(function () { return true; });
			})
			.catch(function (err) {
				return Promise.reject(new StackedError(
					`DynamoDB error during Transaction#deleteItem()`,
					err
				));
			});
	}

	static create(options) {
		return new Transaction(options);
	}
}

module.exports = Transaction;
