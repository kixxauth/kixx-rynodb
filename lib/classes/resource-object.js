'use strict';

class ResourceObject {
	constructor(spec) {
		spec = spec || {};

		Object.defineProperties(this, {
			type: {
				enumerable: true,
				value: spec.type
			},
			id: {
				enumerable: true,
				value: spec.id
			},
			attributes: {
				enumerable: true,
				value: spec.attributes || Object.create(null)
			},
			relationships: {
				enumerable: true,
				value: spec.relationships || Object.create(null)
			},
			meta: {
				enumerable: true,
				value: Object.defineProperties(Object.create(null), {
					created: {
						enumerable: true,
						value: spec.created || new Date().toISOString()
					},
					updated: {
						enumerable: true,
						value: spec.updated || new Date().toISOString()
					}
				})
			}
		});
	}

	static create(spec) {
		return new ResourceObject(spec);
	}
}

module.exports = ResourceObject;
