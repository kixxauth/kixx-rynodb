const {curry} = require(`ramda`);
const Promise = require(`bluebird`);
const Kixx = require(`kixx`);

const {StackedError} = Kixx;

const set = curry(function dynamodbSet(dynamodb, options, args, scope, object) {
	const composedOptions = Object.assign({}, options, args || {});
	const TableName = composeEntityTableName(options.prefix);
	object = assoc(`scope`, scope, object);

	const operationOptions = pick([
		`ReturnConsumedCapacity`,
		`ConditionExpression`,
		`ExpressionAttributeNames`,
		`ExpressionAttributeValues`
	], composedOptions);

	const params = Object.assign({
		TableName,
		Item: serializeToNativeRecord(object)
	}, operationOptions);

	function executeAsync() {
		return new Promise((resolve, reject) => {
			dynamodb.putItem(params, (err, res) => {
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

	return tryOperation(0).then((res) => {
		return {
			data: JSON.parse(JSON.stringify(object)),
			meta: res
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodb set()`,
					err,
					set
				));
			case `ResourceNotFoundException`:
				return reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					set
				));
			default:
				return reject(new StackedError(`Error in dynamodb set()`, err, set));
		}
	});
});

exports.set = set;

const batchSet = curry(function dynamodbBatchSet(dynamodb, options, args, scope, objects) {
	const composedOptions = Object.assign({}, options, args || {});
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : 5;
	const tableName = composeEntityTableName(options.prefix);

	objects = objects.map((object) => {
		return assoc(`scope`, scope, object);
	});

	const chunks = splitEvery(25, objects);

	const operationOptions = pick([
		`ReturnConsumedCapacity`
	], composedOptions);

	const constantParams = Object.assign({}, operationOptions);

	function executeAsync(RequestItems) {
		const params = Object.assign({}, constantParams, {RequestItems});

		return new Promise((resolve, reject) => {
			dynamodb.batchWriteItem(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	function tryOperation(results, retryCount, RequestItems) {
		return executeAsync(RequestItems).then((res) => {
			if (Array.isArray(res.ConsumedCapacity)) {
				results.ConsumedCapacity = res.ConsumedCapacity.reduce((total, cc) => {
					return total + cc.CapacityUnits;
				}, results.ConsumedCapacity);
			}
			if (res.UnprocessedItems && Object.keys(res.UnprocessedItems).length > 0) {
				if (retryCount < retryLimit) {
					return Promise.delay(computeBackoffTime(retryCount)).then(() => {
						return tryOperation(results, retryCount + 1, res.UnprocessedItems);
					});
				}
				return Promise.reject(new ProvisionedThroughputExceededException(
					`Provisioned throughput exceeded after ${retryCount} tries.`
				));
			}
			return results;
		}).catch((err) => {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount < retryLimit) {
				return Promise.delay(computeBackoffTime(retryCount)).then(() => {
					return tryOperation(results, retryCount + 1, RequestItems);
				});
			}
			return Promise.reject(err);
		});
	}

	const chunkPromise = chunks.reduce((promise, objects) => {
		const RequestItems = {};
		RequestItems[tableName] = objects.map((object) => {
			return {
				PutRequest: {Item: serializeToNativeRecord(object)}
			};
		});

		return promise.then((results) => {
			return tryOperation(results, 0, RequestItems);
		});
	}, Promise.resolve({ConsumedCapacity: 0}));

	return chunkPromise.then((res) => {
		const meta = {};
		if (operationOptions.ReturnConsumedCapacity && operationOptions.ReturnConsumedCapacity !== `NONE`) {
			meta.ConsumedCapacity = res.ConsumedCapacity;
		}

		return {
			data: JSON.parse(JSON.stringify(objects)),
			meta
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodb batchSet()`,
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
				return reject(new StackedError(`Error in dynamodb batchSet()`, err, batchSet));
		}
	});
});

exports.batchSet = batchSet;

const get = curry(function dynamodbGet(dynamodb, options, scope, key) {
	const composedOptions = Object.assign({}, options, args || {});
	const TableName = composeEntityTableName(options.prefix);
	key = assoc(`scope`, scope, key);

	const operationOptions = pick([
		`ReturnConsumedCapacity`,
		`ConsistentRead`,
		`ProjectionExpression`,
		`ExpressionAttributeNames`
	], composedOptions);

	const params = Object.assign({
		TableName,
		Key: pick(
			[`id`, `scope_type_key`],
			serializeToNativeRecord(key)
		)
	}, operationOptions);

	function executeAsync() {
		return new Promise((resolve, reject) => {
			dynamodb.getItem(params, (err, res) => {
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

	return tryOperation(0).then((res) => {
		const meta = omit([`Item`], res);
		const data = res.Item ? deserializeFromNativeRecord(res.Item) : null;
		return {meta, data};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodb get()`,
					err,
					get
				));
			case `ResourceNotFoundException`:
				return reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					get
				));
			default:
				return reject(new StackedError(`Error in dynamodb get()`, err, get));
		}
	});
});

exports.get = get;

const batchGet = curry(function dynamodbBatchGet(dynamodb, options, args, scope, keys) {
	const composedOptions = Object.assign({}, options, args || {});
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : 5;
	const tableName = composeEntityTableName(options.prefix);

	keys = keys.map((key) => {
		return assoc(`scope`, scope, key);
	});

	const chunks = splitEvery(25, keys);

	const operationOptions = pick([
		`ConsistentRead`,
		`ExpressionAttributeNames`,
		`ProjectionExpression`
	], composedOptions);

	const constantParams = {
		ReturnConsumedCapacity: composedOptions.ReturnConsumedCapacity || `NONE`
	};

	function executeAsync(RequestItems) {
		const params = Object.assign({}, constantParams, {RequestItems});

		return new Promise((resolve, reject) => {
			dynamodb.batchGetItem(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	function tryOperation(results, retryCount, RequestItems) {
		return executeAsync(RequestItems).then((res) => {
			if (res.Responses && Array.isArray(res.Responses[tableName])) {
				results.Responses = results.Responses.concat(res.Responses[tableName]);
			}
			if (Array.isArray(res.ConsumedCapacity)) {
				results.ConsumedCapacity = res.ConsumedCapacity.reduce((total, cc) => {
					return total + cc.CapacityUnits;
				}, results.ConsumedCapacity);
			}
			if (res.UnprocessedItems && Object.keys(res.UnprocessedItems).length > 0) {
				if (retryCount < retryLimit) {
					return Promise.delay(computeBackoffTime(retryCount)).then(() => {
						return tryOperation(results, retryCount + 1, res.UnprocessedItems);
					});
				}
				return Promise.reject(new ProvisionedThroughputExceededException(
					`Provisioned throughput exceeded after ${retryCount} tries.`
				));
			}
			return results;
		}).catch((err) => {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount < retryLimit) {
				return Promise.delay(computeBackoffTime(retryCount)).then(() => {
					return tryOperation(results, retryCount + 1, RequestItems);
				});
			}
			return Promise.reject(err);
		});
	}

	const chunkPromise = chunks.reduce((promise, keys) => {
		const RequestItems = {};

		RequestItems[tableName] = clone(operationOptions);

		RequestItems[tableName].Keys = keys.map((key) => {
			return pick(
				[`id`, `scope_type_key`],
				serializeToNativeRecord(key)
			);
		});

		return promise.then((results) => {
			return tryOperation(results, 0, RequestItems);
		});
	}, Promise.resolve({Responses: [], ConsumedCapacity: 0}));

	return chunkPromise.then((res) => {
		const meta = {};
		if (constantParams.ReturnConsumedCapacity !== `NONE`) {
			meta.ConsumedCapacity = res.ConsumedCapacity;
		}

		return {
			data: res.Responses.map(deserializeFromNativeRecord),
			meta
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodb batchGet()`,
					err,
					batchGet
				));
			case `ResourceNotFoundException`:
				return reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					batchGet
				));
			default:
				return reject(new StackedError(`Error in dynamodb batchGet()`, err, batchGet));
		}
	});
});

exports.batchGet = batchGet;

const remove = curry(function dynamodbRemove(dynamodb, options, scope, key) {
	const composedOptions = Object.assign({}, options, args || {});
	const TableName = composeEntityTableName(options.prefix);

	const operationOptions = pick([
		`ReturnConsumedCapacity`
	], composedOptions);

	const params = Object.assign({
		TableName
	}, operationOptions);

	function executeAsync() {
		return new Promise((resolve, reject) => {
			dynamodb.foo(params, (err, res) => {
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

	return tryOperation(0).then((res) => {
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodb ()`,
					err,
				));
			case `ResourceNotFoundException`:
				return reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
				));
			default:
				return reject(new StackedError(`Error in dynamodb ()`, err, foo));
		}
	});
});

exports.remove = remove;

const batchRemove = curry(function dynamodbBatchRemove(dynamodb, options, args, scope, keys) {
	const composedOptions = Object.assign({}, options, args || {});
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : 5;
	const tableName = composeEntityTableName(options.prefix);

	keys = keys.map((key) => {
		return assoc(`scope`, scope, key);
	});

	const chunks = splitEvery(25, keys);

	const operationOptions = pick([
		`ReturnConsumedCapacity`
	], composedOptions);

	const constantParams = Object.assign({}, operationOptions);

	function executeAsync(RequestItems) {
		const params = Object.assign({}, constantParams, {RequestItems});

		return new Promise((resolve, reject) => {
			dynamodb.batchWriteItem(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	function tryOperation(results, retryCount, RequestItems) {
		return executeAsync(RequestItems).then((res) => {
			if (Array.isArray(res.ConsumedCapacity)) {
				results.ConsumedCapacity = res.ConsumedCapacity.reduce((total, cc) => {
					return total + cc.CapacityUnits;
				}, results.ConsumedCapacity);
			}
			if (res.UnprocessedItems && Object.keys(res.UnprocessedItems).length > 0) {
				if (retryCount < retryLimit) {
					return Promise.delay(computeBackoffTime(retryCount)).then(() => {
						return tryOperation(results, retryCount + 1, res.UnprocessedItems);
					});
				}
				return Promise.reject(new ProvisionedThroughputExceededException(
					`Provisioned throughput exceeded after ${retryCount} tries.`
				));
			}
			return results;
		}).catch((err) => {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount < retryLimit) {
				return Promise.delay(computeBackoffTime(retryCount)).then(() => {
					return tryOperation(results, retryCount + 1, RequestItems);
				});
			}
			return Promise.reject(err);
		});
	}

	const chunkPromise = chunks.reduce((promise, keys) => {
		const RequestItems = {};
		RequestItems[tableName] = keys.map((key) => {
			return {
				DeleteRequest: {
					Key: pick(
						[`id`, `scope_type_key`],
						serializeToNativeRecord(key)
					)
				}
			};
		});

		return promise.then((results) => {
			return tryOperation(results, 0, RequestItems);
		});
	}, Promise.resolve({ConsumedCapacity: 0}));

	return chunkPromise.then((res) => {
		const meta = {};
		if (operationOptions.ReturnConsumedCapacity && operationOptions.ReturnConsumedCapacity !== `NONE`) {
			meta.ConsumedCapacity = res.ConsumedCapacity;
		}

		return {
			data: true,
			meta
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodb batchRemove()`,
					err,
					batchRemove
				));
			case `ResourceNotFoundException`:
				return reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					batchRemove
				));
			default:
				return reject(new StackedError(`Error in dynamodb batchRemove()`, err, batchRemove));
		}
	});
});

exports.batchRemove = batchRemove;

const scanQuery = curry(function dynamodbScanQuery(dynamodb, options, args, scope, type) {
	const composedOptions = Object.assign({}, options, args || {});
	const {cursor, limit} = composedOptions;
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : 5;
	const TableName = composeEntityTableName(options.prefix);
	const IndexName = composeEntityTypeIndexName(options.prefix);

	const operationOptions = pick([
		`ReturnConsumedCapacity`,
		`ConsistentRead`,
		`ExpressionAttributeNames`,
		`ProjectionExpression`
	], composedOptions);

	const params = Object.assign({
		TableName,
		IndexName,
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

	return tryOperation(0).then((res) => {
		const items = res.Items || [];
		const meta = omit([`Items`, `LastEvaluatedKey`], res);

		return {
			data: items.map((item) => deserializeFromNativeRecord(item)),
			cursor: res.LastEvaluatedKey || null,
			meta
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return reject(new StackedError(
					`Throughput exceeded in dynamodb scanQuery()`,
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
				return reject(new StackedError(`Error in dynamodb scanQuery()`, err, scanQuery));
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
