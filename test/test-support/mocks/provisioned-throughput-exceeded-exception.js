'use strict';

class ProvisionedThroughputExceededException extends Error {
	constructor(message) {
		super(message);

		Object.defineProperties(this, {
			name: {
				enumerable: true,
				value: `ProvisionedThroughputExceededException`
			},
			message: {
				enumerable: true,
				value: message
			},
			code: {
				enumerable: true,
				value: `ProvisionedThroughputExceededException`
			}
		});
	}
}

module.exports = ProvisionedThroughputExceededException;
