module.exports = batchGetObjects(api, scope, keys, options) {
	const {skipCache, resetCache, timeToLive, dynamodbGetItemOptions} = options;

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

		const Keys = missedKeys.map((key) => {
			const {type, id} = key;
			return api.dynamodbObjectKey(scope, type, id);
		});

		const TableName = api.dynamodbObjectTableName();

		const {TableName, Keys} = foo;

		const {Key, TableName} = ;
		const params = Object.assign({Key, TableName}, dynamodbGetItem || {});

		return api.dynamodbGetItem(params).then(api.deserializeDynamoDbResponse).then((res) => {
			api.emit(`info`, {
				message: `dynamodb response meta`,
				method: `getItem`
				object: {scope, type, id},
				meta: res.meta
			});

			return res.item;
		});
	});
};
