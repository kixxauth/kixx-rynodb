'use strict';

const DynamoDB = require(`./dynamodb`);

module.exports = function createInternalApi(options, dynamodb) {
	return {
		dynamodbGetObject: DynamoDB.get(dynamodb, options.dynamodb),
		dynamodbBatchGetObjects: DynamoDB.batchGet(dynamodb, options.dynamodb),
		dynamodbSetObject: DynamoDB.set(dynamodb, options.dynamodb),
		dynamodbBatchSetObjects: DynamoDB.batchSet(dynamodb, options.dynamodb),
		dynamodbRemoveObject: DynamoDB.remove(dynamodb, options.dynamodb),
		dynamodbBatchRemoveObjects: DynamoDB.batchRemove(dynamodb, options.dynamodb),
		dynamodbScanQuery: DynamoDB.scanQuery(dynamodb, options.dynamodb)
	};
};
