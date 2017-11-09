'use strict';

module.exports = function batchGetObjects(api, options, scope, keys) {
	return api.dynamodbBatchGetObjects(options.dynamodb, scope, keys).then((res) => {
		const {meta, data} = res;
		return {
			data,
			meta: {dynamodb: meta}
		};
	});
};

// module.exports = function batchGetObjects(api, scope, keys, options) {
// 	const {skipCache, resetCache, timeToLive, dynamodbBatchGetObjectsOptions} = options;

// 	let promise = Promise.resolve([]);

// 	if (!skipCache) {
// 		promise = api.redisBatchGetObjects(scope, keys);
// 	}

// 	promise = promise.then((cacheResults) => {
// 		if (all(identity, cacheResults)) {
// 			cacheResults.forEach((res) => {
// 				const {type, id} = res;
// 				api.emit(`info`, {
// 					message: `get object cache hit`,
// 					object: {scope, type, id}
// 				});
// 			});
// 			return cacheResults;
// 		}

// 		const missedKeys = keys.filter((key) => {
// 			return !find(hasKey(key), cacheResults);
// 		});

// 		const hitKeys = keys.filter((key) => {
// 			return find(hasKey(key), cacheResults);
// 		});

// 		if (!skipCache) {
// 			missedKeys.forEach((key) => {
// 				const {type, id} = key;
// 				api.emit(`info`, {
// 					message: `get object cache miss`,
// 					object: {scope, type, id}
// 				});
// 			});
// 			hitKeys.forEach((key) => {
// 				const {type, id} = key;
// 				api.emit(`info`, {
// 					message: `get object cache hit`,
// 					object: {scope, type, id}
// 				});
// 			});
// 		}

// 		return api.dynamodbBatchGetObjects(scope, missedKeys, dynamodbBatchGetObjectsOptions).then((res) => {
// 			if (hitKeys.length === 0) {
// 				return res.data;
// 			}

// 			return keys.map((key, i) => {
// 				if (cacheResults[i]) {
// 					return cacheResults[i];
// 				}
// 				return find(hasKey(key), res.data);
// 			});
// 		});
// 	});

// 	if (resetCache) {
// 		promise.then((objects) => {
// 			return api.redisBatchSetObjects(scope, objects, {timeToLive});
// 		}).catch((error) => {
// 			api.emit(`error`, {
// 				message: `error in redisBatchSetObjects`,
// 				error
// 			});
// 		});
// 	}

// 	return promise.catch((err) => {
// 		return Promise.reject(new StackedError(
// 			`Error in RynoDB batchGetObjects()`,
// 			err,
// 			batchGetObjects
// 		));
// 	});
// };
