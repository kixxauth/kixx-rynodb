'use strict';

const Promise = require(`bluebird`);
const {StackedError} = require(`kixx`);
const DynamoDB = require(`./dynamodb`);

module.exports = function getEntity(dynamodb, redis, options, emitRollback, subject) {
	const {scope, type, id} = subject;

	return dynamodb.getEntity(options, {scope, type, id}).catch((err) => {
		return Promise.reject(new StackedError(`Error in RynoDB getEntity()`, err));
	});
};
