module.exports = function setObject(api, scope, object, options) {
	const {dynamodbSetObjectOptions} = options;

	const promise = api.dynamodbSetObject(scope, objects, dynamodbSetObjectOptions);

	api.redisSetObject(scope, object).catch((error) => {
		const {type, id} = object;
		api.emit(`error`, {
			message: `error id redisSetObject`,
			error,
			object: {scope, type, id}
		});
	});

	return promise.then(always(clone(object))).catch((err) => {
		return Promise.reject(append(
			new Error(`Error in RynoDB setObject()`),
			castToArray(err)
		));
	});
};
