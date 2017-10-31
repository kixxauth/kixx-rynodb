'use strict';

class ReturnValues {
	constructor(spec) {
		spec = spec || {};

		return Object.defineProperties(this, {
			error: {
				enumerable: true,
				value: Boolean(spec.error)
			},
			data: {
				enumerable: true,
				value: spec.data || null
			},
			included: {
				enumerable: true,
				value: spec.included || []
			},
			cursor: {
				enumerable: true,
				value: spec.cursor || null
			},
			meta: {
				enumerable: true,
				meta: spec.meta || {}
			}
		});
	}

	static create(spec) {
		return new ReturnValues(spec);
	}
}

module.exports = ReturnValues;
