'use strict';

const Promise = require(`bluebird`);
const {curry, clone, pick, omit, assoc, splitEvery} = require(`ramda`);
const Kixx = require(`kixx`);
const {assert, isNumber} = require(`kixx/library`);
const {assertIsArray} = require(`./library`);

const {StackedError} = Kixx;

const hasOwn = Object.prototype.hasOwnProperty;

const DEFAULT_BACKOFF = 1000;
const DEFAULT_MAX_RETRIES = 5;

class ProvisionedThroughputExceededException extends Error {
	constructor(message) {
		super(message);

		Object.defineProperties(this, {
			name: {
				enumerable: true,
				value: `ProvisionedThroughputExceededException`
			},
			message: {
				enumerable: true,
				value: message
			},
			code: {
				enumerable: true,
				value: `ProvisionedThroughputExceededException`
			}
		});
	}
}

const set = curry(function dynamodbSet(dynamodb, options, args, scope, object) {
	options = options || {};
	args = args || {};
	object = object || {};

	assert.isNonEmptyString(
		options.prefix,
		`All dynamodb operations require the options argument to have a prefix String property.`
	);
	assert.isNonEmptyString(
		scope,
		`dynamodb set() requires a scope argument`
	);
	assert.isNonEmptyString(
		object.type,
		`dynamodb set() requires object argument to have a type`
	);
	assert.isNonEmptyString(
		object.id,
		`dynamodb set() requires object argument to have an id`
	);

	const composedOptions = Object.assign({}, options, args);
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : DEFAULT_MAX_RETRIES;
	const backoff = computeBackoffTime(
		isNumber(composedOptions.backoffMultiplier) ? composedOptions.backoffMultiplier : DEFAULT_BACKOFF
	);
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
			if (err.name === `ProvisionedThroughputExceededException` && retryCount + 1 < retryLimit) {
				return Promise.delay(backoff(retryCount)).then(() => {
					return tryOperation(retryCount + 1);
				});
			}
			return Promise.reject(err);
		});
	}

	return tryOperation(0).then((res) => {
		const meta = omit([`Attributes`], res);
		return {
			data: JSON.parse(JSON.stringify(object)),
			meta
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return Promise.reject(new StackedError(
					`Throughput exceeded in dynamodb set()`,
					err,
					set
				));
			case `ResourceNotFoundException`:
				return Promise.reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					set
				));
			default:
				return Promise.reject(new StackedError(`Error in dynamodb set()`, err, set));
		}
	});
});

exports.set = set;

const batchSet = curry(function dynamodbBatchSet(dynamodb, options, args, scope, objects) {
	options = options || {};
	args = args || {};

	assert.isNonEmptyString(
		options.prefix,
		`All dynamodb operations require the options argument to have a prefix String property.`
	);
	assert.isNonEmptyString(
		scope,
		`dynamodb batchSet() requires a scope argument`
	);
	assertIsArray(
		objects,
		`dynamodb batchSet() expects objects argument to be an Array`
	);
	objects.forEach((object, i) => {
		assert.isNonEmptyString(
			object.type,
			`dynamodb batchSet() requires object at [${i}] to have a type`
		);
		assert.isNonEmptyString(
			object.id,
			`dynamodb batchSet() requires object at [${i}] to have an id`
		);
	});

	if (objects.length === 0) {
		return Promise.resolve({
			data: [],
			meta: {}
		});
	}

	const composedOptions = Object.assign({}, options, args);
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : DEFAULT_MAX_RETRIES;
	const backoff = computeBackoffTime(
		isNumber(composedOptions.backoffMultiplier) ? composedOptions.backoffMultiplier : DEFAULT_BACKOFF
	);
	const TableName = composeEntityTableName(options.prefix);

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
		function onSuccess(res) {
			if (Array.isArray(res.ConsumedCapacity)) {
				results.ConsumedCapacity = res.ConsumedCapacity.reduce((total, cc) => {
					return total + cc.CapacityUnits;
				}, results.ConsumedCapacity);
			}
			if (res.UnprocessedItems && Object.keys(res.UnprocessedItems).length > 0) {
				if (retryCount + 1 < retryLimit) {
					return Promise.delay(backoff(retryCount)).then(() => {
						return tryOperation(results, retryCount + 1, res.UnprocessedItems);
					});
				}
				return Promise.reject(new ProvisionedThroughputExceededException(
					`Provisioned throughput exceeded after ${retryCount} tries.`
				));
			}
			return results;
		}

		function onError(err) {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount + 1 < retryLimit) {
				return Promise.delay(backoff(retryCount)).then(() => {
					return tryOperation(results, retryCount + 1, RequestItems);
				});
			}
			return Promise.reject(err);
		}

		// This slightly unconventional way of handling a promise (by using the
		// second onError handler instead of a .catch() handler) helps prevent an
		// infinite callback loop.
		return executeAsync(RequestItems).then(onSuccess, onError);
	}

	const chunkPromise = chunks.reduce((promise, objects) => {
		const RequestItems = {};
		RequestItems[TableName] = objects.map((object) => {
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
				return Promise.reject(new StackedError(
					`Throughput exceeded in dynamodb batchSet()`,
					err,
					batchSet
				));
			case `ResourceNotFoundException`:
				return Promise.reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					batchSet
				));
			default:
				return Promise.reject(new StackedError(`Error in dynamodb batchSet()`, err, batchSet));
		}
	});
});

exports.batchSet = batchSet;

const get = curry(function dynamodbGet(dynamodb, options, args, scope, key) {
	options = options || {};
	args = args || {};
	key = key || {};

	assert.isNonEmptyString(
		options.prefix,
		`All dynamodb operations require the options argument to have a prefix String property.`
	);
	assert.isNonEmptyString(
		scope,
		`dynamodb get() requires a scope argument`
	);
	assert.isNonEmptyString(
		key.type,
		`dynamodb get() requires key argument to have a type`
	);
	assert.isNonEmptyString(
		key.id,
		`dynamodb get() requires key argument to have an id`
	);

	const composedOptions = Object.assign({}, options, args);
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : DEFAULT_MAX_RETRIES;
	const backoff = computeBackoffTime(
		isNumber(composedOptions.backoffMultiplier) ? composedOptions.backoffMultiplier : DEFAULT_BACKOFF
	);
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
			if (err.name === `ProvisionedThroughputExceededException` && retryCount + 1 < retryLimit) {
				return Promise.delay(backoff(retryCount)).then(() => {
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
				return Promise.reject(new StackedError(
					`Throughput exceeded in dynamodb get()`,
					err,
					get
				));
			case `ResourceNotFoundException`:
				return Promise.reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					get
				));
			default:
				return Promise.reject(new StackedError(`Error in dynamodb get()`, err, get));
		}
	});
});

exports.get = get;

const batchGet = curry(function dynamodbBatchGet(dynamodb, options, args, scope, keys) {
	options = options || {};
	args = args || {};

	assert.isNonEmptyString(
		options.prefix,
		`All dynamodb operations require the options argument to have a prefix String property.`
	);
	assert.isNonEmptyString(
		scope,
		`dynamodb batchGet() requires a scope argument`
	);
	assertIsArray(
		keys,
		`dynamodb batchGet() expects keys argument to be an Array`
	);
	keys.forEach((key, i) => {
		assert.isNonEmptyString(
			key.type,
			`dynamodb batchGet() requires key at [${i}] to have a type`
		);
		assert.isNonEmptyString(
			key.id,
			`dynamodb batchGet() requires key at [${i}] to have an id`
		);
	});

	if (keys.length === 0) {
		return Promise.resolve({
			data: [],
			meta: {}
		});
	}

	const composedOptions = Object.assign({}, options, args);
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : DEFAULT_MAX_RETRIES;
	const backoff = computeBackoffTime(
		isNumber(composedOptions.backoffMultiplier) ? composedOptions.backoffMultiplier : DEFAULT_BACKOFF
	);
	const TableName = composeEntityTableName(options.prefix);

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
		function onSuccess(res) {
			if (res.Responses && Array.isArray(res.Responses[TableName])) {
				results.Responses = results.Responses.concat(res.Responses[TableName]);
			}
			if (Array.isArray(res.ConsumedCapacity)) {
				results.ConsumedCapacity = res.ConsumedCapacity.reduce((total, cc) => {
					return total + cc.CapacityUnits;
				}, results.ConsumedCapacity);
			}
			if (res.UnprocessedItems && Object.keys(res.UnprocessedItems).length > 0) {
				if (retryCount + 1 < retryLimit) {
					return Promise.delay(backoff(retryCount)).then(() => {
						return tryOperation(results, retryCount + 1, res.UnprocessedItems);
					});
				}
				return Promise.reject(new ProvisionedThroughputExceededException(
					`Provisioned throughput exceeded after ${retryCount} tries.`
				));
			}
			return results;
		}

		function onError(err) {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount + 1 < retryLimit) {
				return Promise.delay(backoff(retryCount)).then(() => {
					return tryOperation(results, retryCount + 1, RequestItems);
				});
			}
			return Promise.reject(err);
		}

		// This slightly unconventional way of handling a promise (by using the
		// second onError handler instead of a .catch() handler) helps prevent an
		// infinite callback loop.
		return executeAsync(RequestItems).then(onSuccess, onError);
	}

	const chunkPromise = chunks.reduce((promise, keys) => {
		const RequestItems = {};

		RequestItems[TableName] = clone(operationOptions);

		RequestItems[TableName].Keys = keys.map((key) => {
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
				return Promise.reject(new StackedError(
					`Throughput exceeded in dynamodb batchGet()`,
					err,
					batchGet
				));
			case `ResourceNotFoundException`:
				return Promise.reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					batchGet
				));
			default:
				return Promise.reject(new StackedError(`Error in dynamodb batchGet()`, err, batchGet));
		}
	});
});

exports.batchGet = batchGet;

const remove = curry(function dynamodbRemove(dynamodb, options, args, scope, key) {
	options = options || {};
	args = args || {};
	key = key || {};

	assert.isNonEmptyString(
		options.prefix,
		`All dynamodb operations require the options argument to have a prefix String property.`
	);
	assert.isNonEmptyString(
		scope,
		`dynamodb remove() requires a scope argument`
	);
	assert.isNonEmptyString(
		key.type,
		`dynamodb remove() requires key argument to have a type`
	);
	assert.isNonEmptyString(
		key.id,
		`dynamodb remove() requires key argument to have an id`
	);

	const composedOptions = Object.assign({}, options, args);
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : DEFAULT_MAX_RETRIES;
	const backoff = computeBackoffTime(
		isNumber(composedOptions.backoffMultiplier) ? composedOptions.backoffMultiplier : DEFAULT_BACKOFF
	);
	const TableName = composeEntityTableName(options.prefix);
	key = assoc(`scope`, scope, key);

	const operationOptions = pick([
		`ReturnConsumedCapacity`,
		`ConditionExpression`,
		`ExpressionAttributeNames`,
		`ExpressionAttributeValues`
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
			dynamodb.deleteItem(params, (err, res) => {
				if (err) return reject(err);
				resolve(res);
			});
		});
	}

	function tryOperation(retryCount) {
		return executeAsync().catch((err) => {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount + 1 < retryLimit) {
				return Promise.delay(backoff(retryCount)).then(() => {
					return tryOperation(retryCount + 1);
				});
			}
			return Promise.reject(err);
		});
	}

	return tryOperation(0).then((res) => {
		const meta = omit([`Attributes`], res);
		return {
			data: true,
			meta
		};
	}).catch((err) => {
		switch (err.name) {
			case `ProvisionedThroughputExceededException`:
				return Promise.reject(new StackedError(
					`Throughput exceeded in dynamodb remove()`,
					err,
					remove
				));
			case `ResourceNotFoundException`:
				return Promise.reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					remove
				));
			default:
				return Promise.reject(new StackedError(`Error in dynamodb remove()`, err, remove));
		}
	});
});

exports.remove = remove;

const batchRemove = curry(function dynamodbBatchRemove(dynamodb, options, args, scope, keys) {
	options = options || {};
	args = args || {};

	assert.isNonEmptyString(
		options.prefix,
		`All dynamodb operations require the options argument to have a prefix String property.`
	);

	const composedOptions = Object.assign({}, options, args);
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : DEFAULT_MAX_RETRIES;
	const backoff = computeBackoffTime(
		isNumber(composedOptions.backoffMultiplier) ? composedOptions.backoffMultiplier : DEFAULT_BACKOFF
	);
	const TableName = composeEntityTableName(options.prefix);

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
				if (retryCount + 1 < retryLimit) {
					return Promise.delay(backoff(retryCount)).then(() => {
						return tryOperation(results, retryCount + 1, res.UnprocessedItems);
					});
				}
				return Promise.reject(new ProvisionedThroughputExceededException(
					`Provisioned throughput exceeded after ${retryCount} tries.`
				));
			}
			return results;
		}).catch((err) => {
			if (err.name === `ProvisionedThroughputExceededException` && retryCount + 1 < retryLimit) {
				return Promise.delay(backoff(retryCount)).then(() => {
					return tryOperation(results, retryCount + 1, RequestItems);
				});
			}
			return Promise.reject(err);
		});
	}

	const chunkPromise = chunks.reduce((promise, keys) => {
		const RequestItems = {};
		RequestItems[TableName] = keys.map((key) => {
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
				return Promise.reject(new StackedError(
					`Throughput exceeded in dynamodb batchRemove()`,
					err,
					batchRemove
				));
			case `ResourceNotFoundException`:
				return Promise.reject(new StackedError(
					`Missing DynamoDB table "${TableName}"`,
					err,
					batchRemove
				));
			default:
				return Promise.reject(new StackedError(`Error in dynamodb batchRemove()`, err, batchRemove));
		}
	});
});

exports.batchRemove = batchRemove;

const scanQuery = curry(function dynamodbScanQuery(dynamodb, options, args, scope, type) {
	assert.isNonEmptyString(
		options.prefix,
		`All dynamodb operations require the options argument to have a prefix String property.`
	);

	const composedOptions = Object.assign({}, options, args);
	const {cursor, limit} = composedOptions;
	const retryLimit = isNumber(composedOptions.retryLimit) ? composedOptions.retryLimit : DEFAULT_MAX_RETRIES;
	const backoff = computeBackoffTime(
		isNumber(composedOptions.backoffMultiplier) ? composedOptions.backoffMultiplier : DEFAULT_BACKOFF
	);
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
			if (err.name === `ProvisionedThroughputExceededException` && retryCount + 1 < retryLimit) {
				return Promise.delay(backoff(retryCount)).then(() => {
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
				return Promise.reject(new StackedError(
					`Throughput exceeded in dynamodb scanQuery()`,
					err,
					scanQuery
				));
			case `ResourceNotFoundException`:
				return Promise.reject(new StackedError(
					`Missing DynamoDB table "${TableName}" or index "${IndexName}"`,
					err,
					scanQuery
				));
			default:
				return Promise.reject(new StackedError(`Error in dynamodb scanQuery()`, err, scanQuery));
		}
	});
});

exports.scanQuery = scanQuery;

function serializeToNativeRecord(object) {
	object = object || {};

	assert.isNonEmptyString(
		object.scope,
		`object must have scope before being written to DynamoDB by RynoDB`
	);
	assert.isNonEmptyString(
		object.type,
		`object must have type before being written to DynamoDB by RynoDB`
	);

	const scope_type_key = `${object.scope}:${object.type}`;
	const record = assoc(`scope_type_key`, scope_type_key, object);

	return serializeItem(record);
}

function deserializeFromNativeRecord(record) {
	const item = deserializeRecord(record);
	delete item.scope_type_key;
	return item;
}

function serializeItem(record) {
	return Object.keys(record).reduce((rec, key) => {
		const val = serializeObject(record[key]);
		if (val) {
			rec[key] = val;
		}
		return rec;
	}, {});
}

function serializeObject(obj) {
	switch (typeof obj) {
		case `string`:
			if (obj.length === 0) {
				return null;
			}
			return {S: obj};
		case `number`:
			if (isNaN(obj)) {
				return null;
			}
			return {N: obj.toString()};
		case `boolean`:
			return {BOOL: obj};
		case `function`:
		case `undefined`:
			return null;
		default:
			if (!obj) {
				return {NULL: true};
			}
			return Array.isArray(obj) ? serializeArray(obj) : serializeMap(obj);
	}
}

function serializeArray(obj) {
	return {L: obj.map(serializeObject)};
}

function serializeMap(obj) {
	const keys = Object.keys(obj);
	const rv = {M: {}};

	if (keys.length === 0) {
		return rv;
	}

	rv.M = keys.reduce(function (M, key) {
		const val = serializeObject(obj[key]);
		if (val) {
			M[key] = val;
		}
		return M;
	}, rv.M);

	return rv;
}

function deserializeRecord(record) {
	return Object.keys(record).reduce(function (rv, key) {
		rv[key] = deserializeObject(record[key]);
		return rv;
	}, Object.create(null));
}

function deserializeObject(val) {
	if (hasOwn.call(val, `S`)) {
		return val.S.toString();
	} else if (hasOwn.call(val, `N`)) {
		return parseFloat(val.N);
	} else if (val.SS || val.NS) {
		return val.SS || val.NS;
	} else if (hasOwn.call(val, `BOOL`)) {
		return Boolean(val.BOOL);
	} else if (hasOwn.call(val, `M`)) {
		return deserializeRecord(val.M);
	} else if (hasOwn.call(val, `L`)) {
		return val.L.map(deserializeObject);
	} else if (hasOwn.call(val, `NULL`)) {
		return null;
	}
}

function composeEntityTableName(prefix) {
	return `${prefix}_entities_master`;
}

function composeEntityTypeIndexName(prefix) {
	return `${prefix}_entities_by_type`;
}

const computeBackoffTime = curry((multiplier, times) => {
	times += 2;
	return Math.pow(2, times) * multiplier;
});
