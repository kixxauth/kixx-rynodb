'use strict';

const Promise = require(`bluebird`);
const {StackedError} = require(`kixx`);
const {assert} = require(`kixx/library`);
const DynamoDB = require(`./dynamodb`);

module.exports = function createRelationshipEntries(dynamodb, redis, options, emitRollback, subject, keys) {
	const {prefix} = options;

	const TableName = dynamodb.relationshipEntriesTableName(prefix);

	const Items = keys.map((key, i) => {
		assert.isNotEmpty(key, `key[${i}]`);
		assert.isNonEmptyString(key.scope, `key[${i}].predicate`);
		assert.isNonEmptyString(key.type, `key[${i}].predicate`);
		assert.isNonEmptyString(key.id, `key[${i}].predicate`);
		assert.isNonEmptyString(key.predicate, `key[${i}].predicate`);
		assert.isOk(Number.isInteger(key.index), `key[${i}].index`);

		const {predicate, index} = key;
		const object = key;
		return DynamoDB.newRelationshipRecord(subject, predicate, index, object);
	});

	return dynamodb.batchSetItems(options, {TableName, Items}).then((res) => {
		emitRollback(function batchSetItemsRollback() {
			const Keys = Items.map(DynamoDB.keyFromRelationshipRecord);
			return dynamodb.batchDeleteItems(options, {TableName, Keys});
		});
		return res;
	}).catch((err) => {
		return Promise.reject(new StackedError(
			`Error in RynoDB createRelationshipEntries()`,
			err
		));
	});
};
