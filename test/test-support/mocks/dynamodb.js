'use strict';

class DynamoDB {
	putItem(params, callback) {
		process.nextTick(() => {
			callback(null, {Attributes: `XXX`, foo: `bar`});
		});
	}
}

module.exports = DynamoDB;
