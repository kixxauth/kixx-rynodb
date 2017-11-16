'use strict';

const {assoc, compact} = require(`kixx/library`);

class Response {
	constructor(spec) {
		spec = spec || {};

		return Object.defineProperties(this, {
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
				value: spec.meta || []
			}
		});
	}

	static create(spec) {
		spec = assoc(
			`meta`,
			compact(spec.meta),
			spec
		);
		return new Response(spec);
	}
}

module.exports = Response;
