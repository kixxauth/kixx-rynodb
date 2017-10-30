module.exports = function removeObject(api, scope, key) {
	const promise = api.dynamodbRemoveObject(scope, object);

	api.redisRemoveObject(scope, object).catch((error) => {
		const {type, id} = key;
		api.emit(`error`, {
			message: `error in redisRemoveObject`,
			error,
			object: {scope, type, id}
		});
	});

	return promise.then(always(clone(object))).catch((err) => {
		return Promise.reject(append(
			new Error(`Error in RynoDB removeObject()`),
			castToArray(err)
		));
	});
};
