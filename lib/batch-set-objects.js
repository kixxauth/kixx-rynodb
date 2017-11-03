'use strict';

module.exports = function batchSetObjects(api, options, scope, objects) {
	return api.dynamodbBatchSetObjects(options.dynamodb, scope, objects).then((res) => {
		return res.data;
	});
};
