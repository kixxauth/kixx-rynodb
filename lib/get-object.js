module.exports = function getObject(api, scope, key, options) {
	const {type, id} = key;
	const {skipCache, resetCache, timeToLive, dynamodbGetObjectOptions} = options;

	const promise = Promise.resolve(null);

	if (!skipCache) {
		promise = api.redisGetObject(scope, key);
	}

	promise = promise.then((cachedObject) => {
		if (cachedObject) {
			api.emit(`info`, {
				message: `get object cache hit`,
				object: {scope, type, id}
			});
			return cachedObject;
		}

		if (!skipCache) {
			api.emit(`info`, {
				message: `get object cache miss`,
				object: {scope, type, id}
			});
		}

		return api.dynamodbGetObject(scope, key, dynamodbGetObjectOptions).then((res) => {
			api.emit(`info`, {
				message: `dynamodb response meta`,
				method: `getItem`
				object: {scope, type, id},
				meta: res.meta
			});

			return res.data;
		});
	});

	if (resetCache) {
		promise.then((object) => {
			return api.redisSetObject(scope, object, {timeToLive});
		}).catch((error) => {
			api.emit(`error`, {
				message: `error in redisSetObject`,
				object: {scope, type, id},
				error
			});
		});
	}

	return promise.catch((err) => {
		return Promise.reject(append(
			new Error(`Error in RynoDB getObject()`),
			castToArray(err)
		));
	});
};
