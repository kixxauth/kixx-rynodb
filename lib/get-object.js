'use strict';

module.exports = function getObject(api, options, scope, key) {
	return api.dynamodbGetObject(options.dynamodb, scope, key).then((res) => {
		const {meta, data} = res;
		return {
			data,
			meta: {dynamodb: meta}
		};
	});
};
