'use strict';

const {assert, deepFreeze} = require(`kixx/library`);

class Record {
	constructor(spec) {
		spec = spec || {};

		Object.defineProperties(this, {
			scope: {
				enumerable: true,
				value: spec.scope
			},
			type: {
				enumerable: true,
				value: spec.type
			},
			id: {
				enumerable: true,
				value: spec.id
			},
			created: {
				enumerable: true,
				value: spec.meta.created || new Date().toISOString()
			},
			updated: {
				enumerable: true,
				value: new Date().toISOString()
			},
			attributes: {
				enumerable: true,
				value: deepFreeze(spec.attributes || Object.create(null))
			},
			relationships: {
				enumerable: true,
				value: deepFreeze(spec.relationships || Object.create(null))
			},
			meta: {
				enumerable: true,
				value: deepFreeze(spec.meta || Object.create(null))
			},
			foreignKeys: {
				enumerable: true,
				value: deepFreeze(spec.foreignKeys || [])
			}
		});
	}

	static create(scope, spec) {
		spec = spec || {};

		assert.isNonEmptyString(scope, `A Record must have a scope`);
		assert.isNonEmptyString(spec.type, `A Record must have a type`);
		assert.isNonEmptyString(spec.id, `A Record must have an id`);

		return new Record(Object.assign({meta: {}}, spec, {scope}));
	}

	static createWithScope(scope) {
		return function (spec) {
			return Record.create(scope, spec);
		};
	}
}

module.exports = Record;
