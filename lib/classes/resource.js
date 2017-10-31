'use strict';

class Resource {
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
				value: spec.created || new Date().toISOString()
			},
			updated: {
				enumerable: true,
				value: spec.updated || new Date().toISOString()
			},
			attributes: {
				enumerable: true,
				value: spec.attributes || Object.create(null)
			},
			relationships: {
				enumerable: true,
				value: spec.relationships || Object.create(null)
			},
			foreignKeys: {
				enumerable: true,
				value: spec.foreignKeys || []
			}
		});
	}

	static create(spec) {
		return new Resource(spec);
	}
}

module.exports = Resource;
