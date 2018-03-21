'use strict';

const {clone, omitKeys} = require('./');

const omitDatabaseRecordKeys = omitKeys([
	'_scope',
	'_type',
	'_id',
	'_index_name',
	'_index_key',
	'_subject_key',
	'_unique_key',
	'_scope_index_name',
	'_created'
]);

class IndexEntry {
	// - spec.scope
	// - spec.type
	// - spec.id
	// - spec.index_name
	// - spec.index_key
	// - spec.attributes
	constructor(spec) {
		this.scope = spec.scope;
		this.type = spec.type;
		this.id = spec.id;
		this.index_name = spec.index_name;
		this.index_key = spec.index_key;
		this.attributes = spec.attributes;
		this.created = spec.created;

		Object.freeze(this);
	}

	getSubjectKey() {
		return `${this.scope}:${this.type}:${this.id}`;
	}

	toTableKey() {
		return {
			_subject_key: this.getSubjectKey(),
			_unique_key: `${this.index_name}:${this.index_key}`
		};
	}

	toPublic() {
		const meta = Object.assign({}, this.meta, {
			updated: this.created
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
			_index_name: this.index_name,
			_index_key: this.index_key,
			_subject_key: `${this.scope}:${this.type}:${this.id}`,
			_unique_key: `${this.index_name}:${this.index_key}`,
			_scope_index_name: `${this.scope}:${this.index_name}`,
			_created: new Date().toISOString()
		});
	}

	static fromDatabaseRecord(record) {
		return IndexEntry.create({
			scope: record._scope,
			type: record._type,
			id: record._id,
			index_name: record._index_name,
			index_key: record._index_key,
			attributes: omitDatabaseRecordKeys(record),
			created: record._created
		});
	}

	// - spec.scope
	// - spec.type
	// - spec.id
	// - spec.index_name
	// - spec.index_key
	// - spec.attributes
	static create(spec) {
		return new IndexEntry(spec);
	}
}

module.exports = IndexEntry;
