'use strict';

const IndexEntry = require('./index-entry');
const {clone, omitKeys} = require('./');

const omitDatabaseRecordKeys = omitKeys([
	'_scope',
	'_type',
	'_id',
	'_scope_type_key',
	'_created',
	'_updated',
	'_meta'
]);

class Record {
	// - spec.scope
	// - spec.type
	// - spec.id
	// - spec.created
	// - spec.updated
	// - spec.attributes
	// - spec.meta
	constructor(spec) {
		this.scope = spec.scope;
		this.type = spec.type;
		this.id = spec.id;
		this.created = spec.created;
		this.updated = spec.updated;
		this.attributes = spec.attributes || {};
		this.meta = spec.meta || {};

		Object.freeze(this);
	}

	getSubjectKey() {
		return IndexEntry.prototype.getSubjectKey.call(this);
	}

	toTableKey() {
		return {
			_id: this.id,
			_scope_type_key: `${this.scope}:${this.type}`
		};
	}

	toPublic() {
		const meta = Object.assign({}, this.meta, {
			created: this.created,
			updated: this.updated
		});

		return {
			scope: this.scope,
			type: this.type,
			id: this.id,
			attributes: clone(this.attributes),
			meta
		};
	}

	toDatabaseRecord() {
		return Object.assign({}, this.attributes, {
			_scope: this.scope,
			_type: this.type,
			_id: this.id,
			_scope_type_key: `${this.scope}:${this.type}`,
			_created: this.created,
			_updated: new Date().toISOString(),
			_meta: this.meta
		});
	}

	createIndexEntry(index_name, index_key, attributes) {
		const {scope, type, id, created} = this;

		return IndexEntry.create({
			scope,
			type,
			id,
			index_name,
			index_key,
			attributes,
			created
		});
	}

	static fromPublic(resource) {
		const {scope, type, id} = resource;
		const attributes = resource.attributes ? clone(resource.attributes) : {};
		const meta = resource.meta ? clone(resource.meta) : {};
		const created = meta.created || new Date().toISOString();
		const updated = meta.updated || null;

		delete meta.created;
		delete meta.updated;

		return Record.create({
			scope,
			type,
			id,
			created,
			updated,
			attributes,
			meta
		});
	}

	static fromDatabaseRecord(record) {
		return Record.create(Object.assign({
			scope: record._scope,
			type: record._type,
			id: record._id,
			created: record._created,
			updated: record._updated,
			attributes: omitDatabaseRecordKeys(record),
			meta: record._meta
		}, record));
	}

	// - spec.scope
	// - spec.type
	// - spec.id
	// - spec.created
	// - spec.updated
	// - spec.attributes
	// - spec.meta
	static create(spec) {
		return new Record(spec);
	}
}

module.exports = Record;
