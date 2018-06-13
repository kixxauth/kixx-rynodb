'use strict';

const {assert, mergeDeep} = require('kixx/library');

const PROPRIETARY_PROPS = Object.freeze([
	'_scope',
	'_type',
	'_id',
	'_index_name',
	'_index_key',
	'_subject_key',
	'_unique_key',
	'_scope_index_name',
	'_created',
	'_updated',
	'_meta'
]);

const PROPRIETARY_META_PROPS = Object.freeze([
	'created',
	'updated'
]);

class IndexEntry {
	constructor(props) {
		Object.assign(this, props);
	}

	getFullKey() {
		return `${this._subject_key}:${this._unique_key}`;
	}

	toPublicItem() {
		return Object.freeze({
			scope: this._scope,
			type: this._type,
			id: this._id,
			attributes: pickAttributes(this),
			meta: mergeDeep(this._meta, {
				created: this._created,
				updated: this._updated
			})
		});
	}

	static partitionKey(scope, type, id) {
		return {_subject_key: `${scope}:${type}:${id}`};
	}

	static fromDatabaseRecord(spec) {
		return new IndexEntry(spec);
	}

	static create(spec) {
		const {scope, type, id, meta, indexName, indexKey} = spec;

		const attributes = spec.attributes || Object.create(null);

		assert.isNonEmptyString(meta.created, 'IndexEntry.create() spec.meta.created');
		assert.isNonEmptyString(meta.updated, 'Entity.create() spec.meta.updated');

		return new IndexEntry(Object.assign(Object.create(null), attributes, {
			_scope: scope,
			_type: type,
			_id: id,
			_index_name: indexName,
			_index_key: indexKey,
			_subject_key: `${scope}:${type}:${id}`,
			_unique_key: `${indexName}:${indexKey}`,
			_scope_index_name: `${scope}:${indexName}`,
			_created: meta.created,
			_updated: meta.updated,
			_meta: pickMeta(meta)
		}));
	}
}

module.exports = IndexEntry;

function pickAttributes(object) {
	return Object.keys(object).reduce((target, key) => {
		if (PROPRIETARY_PROPS.includes(key)) return target;
		return Object.defineProperty(target, key, {
			enumerable: true,
			value: object[key]
		});
	}, Object.create(null));
}

function pickMeta(object) {
	return Object.keys(object).reduce((target, key) => {
		if (PROPRIETARY_META_PROPS.includes(key)) return target;
		return Object.defineProperty(target, key, {
			enumerable: true,
			value: object[key]
		});
	}, Object.create(null));
}
