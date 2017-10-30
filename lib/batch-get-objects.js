module.exports = batchGetObjects(api, scope, keys, options) {
	const {skipCache, resetCache, timeToLive, dynamodbBatchGetObjectsOptions} = options;

	const promise = Promise.resolve([]);

	if (!skipCache) {
		promise = api.redisBatchGetObjects(scope, keys);
	}

	promise = promise.then((cacheResults) => {
		if (all(identity, cacheResults)) {
			cacheResults.forEach((res) => {
				const {type, id} = res;
				api.emit(`info`, {
					message: `get object cache hit`,
					object: {scope, type, id}
				});
			});
			return cacheResults;
		}

		const missedKeys = keys.filter((key) => {
			return !find(hasKey(key), cacheResults);
		});

		const hitKeys = keys.filter((key) => {
			return find(hasKey(key), cacheResults);
		});

		if (!skipCache) {
			missedKeys.forEach((key) => {
				const {type, id} = key;
				api.emit(`info`, {
					message: `get object cache miss`,
					object: {scope, type, id}
				});
			});
			hitKeys.forEach((key) => {
				const {type, id} = key;
				api.emit(`info`, {
					message: `get object cache hit`,
					object: {scope, type, id}
				});
			});
		}

		return api.dynamodbBatchGetObjects(scope, missedKeys, dynamodbBatchGetObjectsOptions).then((res) => {
			api.emit(`info`, {
				message: `dynamodb response meta`,
				method: `getItem`
				object: {scope, type, id},
				meta: res.meta
			});

			if (hitKeys.length === 0) {
				return res.data;
			}

			return keys.map((key, i) => {
				if (cacheResults[i]) {
					return cacheResults[i];
				}
				return find(hasKey(key), res.data);
			});
		});
	});

	if (resetCache) {
		promise.then((objects) => {
			return api.redisBatchSetObjects(scope, objects, {timeToLive});
		}).catch((error) => {
			api.emit(`error`, {
				message: `error in redisBatchSetObjects`,
				object: {scope, type, id},
				error
			});
		});
	}

	return promise.catch((err) => {
		return Promise.reject(append(
			new Error(`Error in RynoDB batchGetObjects()`),
			castToArray(err)
		));
	});
};
