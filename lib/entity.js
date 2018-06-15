'use strict';

const {assert, mergeDeep} = require('kixx/library');

const PROPRIETARY_PROPS = Object.freeze([
	'_scope',
	'_type',
	'_id',
	'_scope_type_key',
	'_created',
	'_updated',
	'_meta'
]);

const PROPRIETARY_META_PROPS = Object.freeze([
	'created',
	'updated'
]);

class Entity {
	constructor(props) {
		Object.assign(this, props);
	}

	mergeIn(entity) {
		const attributes = mergeDeep(
			pickAttributes(this),
			pickAttributes(entity)
		);

		const meta = mergeDeep(
			this._meta,
			entity._meta
		);

		return new Entity(Object.assign(Object.create(null), attributes, {
			_scope: this._scope,
			_type: this._type,
			_id: this._id,
			_scope_type_key: this._scope_type_key,
			_created: this._created,
			_updated: this._updated,
			_meta: pickMeta(meta)
		}));
	}

	setUpdate() {
		return new Entity(Object.assign(Object.create(null), this, {
			_updated: new Date().toISOString()
		}));
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

	toPlainObject() {
		return this.toPublicItem();
	}

	static createKey(scope, type, id) {
		return {_id: id, _scope_type_key: `${scope}:${type}`};
	}

	static fromDatabaseRecord(spec) {
		return new Entity(spec);
	}

	static fromPublicObject(spec) {
		assert.isNonEmptyString(spec.scope, 'Entity.create() spec.scope');
		assert.isNonEmptyString(spec.type, 'Entity.create() spec.type');
		assert.isNonEmptyString(spec.id, 'Entity.create() spec.id');

		const {scope, type, id} = spec;

		const attributes = spec.attributes || Object.create(null);
		const meta = spec.meta || Object.create(null);

		return new Entity(Object.assign(Object.create(null), attributes, {
			_scope: scope,
			_type: type,
			_id: id,
			_scope_type_key: `${scope}:${type}`,
			_created: meta.created || new Date().toISOString(),
			_updated: meta.updated || new Date().toISOString(),
			_meta: pickMeta(meta)
		}));
	}
}

module.exports = Entity;

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
