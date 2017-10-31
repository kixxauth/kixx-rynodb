'use strict';

const Promise = require(`bluebird`);
const {always} = require(`ramda`);
const Kixx = require(`kixx`);

const {StackedError} = Kixx;

module.exports = function removeObject(api, scope, key) {
	const promise = api.dynamodbRemoveObject(null, scope, key);

	api.redisRemoveObject(null, scope, key).catch((error) => {
		const {type, id} = key;
		api.emit(`error`, {
			message: `error in redisRemoveObject`,
			error,
			object: {scope, type, id}
		});
	});

	return promise.then(always(true)).catch((err) => {
		return Promise.reject(new StackedError(
			`Error in RynoDB removeObject()`,
			err,
			removeObject
		));
	});
};
