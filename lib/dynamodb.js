const {curry} = require(`ramda`);
const Promise = require(`bluebird`);
const Kixx = require(`kixx`);

const {StackedError} = Kixx;

const set = curry(function dynamodbSet(dynamodb, options, scope, object) {
	function execute() {
		return new Promise((resolve, reject) => {
			dynamodb.foo(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	return execute().then((res) => {
	}).catch((err) => {
		if (err.name === `ProvisionedThroughputExceededException`) {
			if (retryCount < retryLimit) {
				retryCount += 1;
				return Promise.delay(computeBackoffTime(retryCount)).then(execute);
			}
			return reject(new StackedError(`Throughput exceeded in dynamodbSet()`, err, set));
		}
		if (err.name === `ResourceNotFoundException`) {
			return reject(new StackedError(`Missing DynamoDB table "${TableName}"`, err, set));
		}
		return reject(new StackedError(`Error in dynamodbSet()`, err, set));
	});
});

exports.set = set;

const batchSet = curry(function dynamodbBatchSet(dynamodb, options, args, scope, objects) {
	const composedOptions = Object.assign({}, options, args || {});
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : 5;
	const tableName = composeEntityTableName(options.prefix);

	const operationOptions = pick([
		`ReturnConsumedCapacity`,
		`ReturnItemCollectionMetrics`
	], composedOptions);

	const constantParams = Object.assign({}, operationOptions);

	function executeAsync(objects) {
		const params = Object.assign({}, constantParams, {RequestItems});

		return new Promise((resolve, reject) => {
			dynamodb.batchWriteItem(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	function tryOperation(retryCount, RequestItems) {
		return executeAsync(RequestItems).then((res) => {
		}).catch((err) => {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount < retryLimit) {
				return Promise.delay(computeBackoffTime(retryCount)).then(() => {
					return tryOperation(retryCount + 1, RequestItems);
				});
			}
			return Promise.reject(err);
		});
	}

	const chunkPromise = chunks.reduce((promise, objects) => {
		const RequestItems = {};
		RequestItems[tableName] = objects.map((object) => {
			return {
				PutRequest: {Item: serializeObject(toNativeRecord(object))}
			};
		});

		return promise.then(() => {
			return tryOperation(0, RequestItems);
		});
	}, Promise.resolve(null));

	return chunkPromise.then((res) => {
		const items = res.Items || [];

		return {
			data: items.map(deserializeObject),
			cursor: res.LastEvaluatedKey || null,
			meta: omit([`Items`, `LastEvaluatedKey`], res)
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodbBatchSet()`,
					err,
					batchSet
				));
			case `ResourceNotFoundException`:
				return reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					batchSet
				));
			default:
				return reject(new StackedError(`Error in dynamodbBatchSet()`, err, batchSet));
		}
	});
});

exports.batchSet = batchSet;

const get = curry(function dynamodbGet(dynamodb, options, scope, key) {
	function execute() {
		return new Promise((resolve, reject) => {
			dynamodb.foo(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	return execute().then((res) => {
	}).catch((err) => {
		if (err.name === `ProvisionedThroughputExceededException`) {
			if (retryCount < retryLimit) {
				retryCount += 1;
				return Promise.delay(computeBackoffTime(retryCount)).then(execute);
			}
			return reject(new StackedError(`Throughput exceeded in dynamodbGet()`, err, get));
		}
		if (err.name === `ResourceNotFoundException`) {
			return reject(new StackedError(`Missing DynamoDB table "${TableName}"`, err, get));
		}
		return reject(new StackedError(`Error in dynamodbGet()`, err, get));
	});
});

exports.get = get;

const batchGet = curry(function dynamodbBatchGet(dynamodb, options, scope, keys) {
	function execute() {
		return new Promise((resolve, reject) => {
			dynamodb.foo(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	return execute().then((res) => {
	}).catch((err) => {
		if (err.name === `ProvisionedThroughputExceededException`) {
			if (retryCount < retryLimit) {
				retryCount += 1;
				return Promise.delay(computeBackoffTime(retryCount)).then(execute);
			}
			return reject(new StackedError(`Throughput exceeded in dynamodbBatchGet()`, err, batchGet));
		}
		if (err.name === `ResourceNotFoundException`) {
			return reject(new StackedError(`Missing DynamoDB table "${TableName}"`, err, batchGet));
		}
		return reject(new StackedError(`Error in dynamodbBatchGet()`, err, batchGet));
	});
});

exports.batchGet = batchGet;

const remove = curry(function dynamodbRemove(dynamodb, options, scope, key) {
	function execute() {
		return new Promise((resolve, reject) => {
			dynamodb.foo(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	return execute().then((res) => {
	}).catch((err) => {
		if (err.name === `ProvisionedThroughputExceededException`) {
			if (retryCount < retryLimit) {
				retryCount += 1;
				return Promise.delay(computeBackoffTime(retryCount)).then(execute);
			}
			return reject(new StackedError(`Throughput exceeded in dynamodbRemove()`, err, remove));
		}
		if (err.name === `ResourceNotFoundException`) {
			return reject(new StackedError(`Missing DynamoDB table "${TableName}"`, err, remove));
		}
		return reject(new StackedError(`Error in dynamodbRemove()`, err, remove));
	});
});

exports.remove = remove;

const scanQuery = curry(function dynamodbScanQuery(dynamodb, options, args, scope, type) {
	const composedOptions = Object.assign({}, options, args || {});
	const {cursor, limit} = composedOptions;
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : 5;

	const operationOptions = pick([
		`ReturnConsumedCapacity`,
		`ConsistentRead`,
		`ProjectionExpression`
	], composedOptions);

	const params = Object.assign({
		TableName: composeEntityTableName(options.prefix),
		IndexName: composeEntityTypeIndexName(options.prefix),
		ExpressionAttributeValues: {
			':key': {S: `${scope}:${type}`}
		},
		KeyConditionExpression: `scope_type_key = :key`,
		Limit: isNumber(limit) ? limit : 10,
		ExclusiveStartKey: cursor || null
	}, operationOptions);

	function executeAsync() {
		return new Promise((resolve, reject) => {
			dynamodb.query(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	function tryOperation(retryCount) {
		return executeAsync().catch((err) => {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount < retryLimit) {
				return Promise.delay(computeBackoffTime(retryCount)).then(() => {
					return tryOperation(retryCount + 1);
				});
			}
			return Promise.reject(err);
		});
	}

	return tryOperation().then((res) => {
		const items = res.Items || [];

		return {
			data: items.map((item) => fromNativeRecord(deserializeObject(item))),
			cursor: res.LastEvaluatedKey || null,
			meta: omit([`Items`, `LastEvaluatedKey`], res)
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodbScanQuery()`,
					err,
					scanQuery
				));
			case `ResourceNotFoundException`:
				return reject(new StackedError(
					`Missing DynamoDB table "${TableName}" or index "${IndexName}"`,
					err,
					scanQuery
				));
			default:
				return reject(new StackedError(`Error in dynamodbScanQuery()`, err, scanQuery));
		}
	});
});

exports.scanQuery = scanQuery;

function composeEntityTableName(prefix) {
	return `${prefix}_entities_master`;
}

function composeEntityTypeIndexName(prefix) {
	return `${prefix}_entities_by_type`;
}

function computeBackoffTime(times) {
	return Math.pow(2, times) * 1000;
}
