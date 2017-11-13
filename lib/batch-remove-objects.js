'use strict';

module.exports = function batchRemoveObjects(api, options, scope, keys) {
	return api.dynamodbBatchRemoveObjects(options.dynamodb, scope, keys).then((res) => {
		const {meta, data} = res;
		return {
			data,
			meta: {dynamodb: meta}
		};
	});
};
