'use strict';

const Promise = require(`bluebird`);
const {always, clone} = require(`ramda`);
const Kixx = require(`kixx`);

const {StackedError} = Kixx;

module.exports = function setObject(api, scope, object, options) {
	const {dynamodbSetObjectOptions} = options;

	const promise = api.dynamodbSetObject(dynamodbSetObjectOptions, scope, object);

	api.redisSetObject(null, scope, object).catch((error) => {
		const {type, id} = object;
		api.emit(`error`, {
			message: `error id redisSetObject`,
			error,
			object: {scope, type, id}
		});
	});

	return promise.then(always(clone(object))).catch((err) => {
		return Promise.reject(new StackedError(
			`Error in RynoDB setObject()`,
			err,
			setObject
		));
	});
};
