'use strict';

class ResourceObject {
	constructor(spec) {
		const meta = Object.assign(Object.create(null), spec.meta || {});

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
				value: Object.defineProperties(meta, {
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
		if (!spec) {
			return null;
		}

		return new ResourceObject(spec);
	}
}

module.exports = ResourceObject;
