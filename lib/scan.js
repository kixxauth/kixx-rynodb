'use strict';

module.exports = function scan(self, type, args, options) {
	const {cursor, limit} = args;

	const params = {
		type,
		ExclusiveStartKey: cursor,
		Limit: limit
	};

	return dynamodb.scanEntitiesByType(params).catch((err) => {
		return Promise.reject(new StackedError(
			`Error attempting to scan type "${type}" from DynamoDB`,
			err
		));
	});
};
