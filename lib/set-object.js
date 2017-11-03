'use strict';

module.exports = function setObject(api, options, scope, object) {
	return api.dynamodbSetObject(options.dynamodb, scope, object);
};
