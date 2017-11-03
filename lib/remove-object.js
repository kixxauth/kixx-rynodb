'use strict';

module.exports = function removeObject(api, options, scope, key) {
	return api.dynamodbRemoveObject(options.dynamodb, scope, key).then(() => {
		return true;
	});
};
