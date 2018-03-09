'use strict';

const Promise = require(`bluebird`);
const {StackedError} = require(`kixx`);
const DynamoDB = require(`./dynamodb`);

module.exports = function getEntity(dynamodb, redis, options, emitRollback, subject) {
	const {prefix} = options;
	const {scope, type, id} = subject;

	const TableName = dynamodb.entitiesMasterTableName(prefix);

	const Key = DynamoDB.newRootKey({
		scope,
		type,
		id
	});

	const params = {
		TableName,
		Key
	};

	return dynamodb.getItem(options, params).then((res) => {
		const data = res.data;
		const meta = {
			entity: res.meta
		};
		return {data, meta};
	}).catch((err) => {
		return Promise.reject(new StackedError(
			`Error in RynoDB createEntity()`,
			err
		));
	});
};
