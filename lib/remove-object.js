'use strict';

module.exports = function removeObject(api, options, scope, key) {
	return api.dynamodbRemoveObject(options.dynamodb, scope, key).then((res) => {
		const {meta} = res;
		return {
			data: true,
			meta: {dynamodb: meta}
		};
	});
};
