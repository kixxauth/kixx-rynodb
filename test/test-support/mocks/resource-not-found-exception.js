'use strict';

class ResourceNotFoundException extends Error {
	constructor(message) {
		super(message);

		Object.defineProperties(this, {
			name: {
				enumerable: true,
				value: `ResourceNotFoundException`
			},
			message: {
				enumerable: true,
				value: message
			},
			code: {
				enumerable: true,
				value: `ResourceNotFoundException`
			}
		});
	}
}

module.exports = ResourceNotFoundException;
