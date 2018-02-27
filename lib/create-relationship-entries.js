'use strict';

const {StackedError} = require(`kixx`);
const {assert, pick} = require(`kixx/library`);

module.exports = function createRelationshipEntries(dynamodb, redis, options, emitRollback, subject, keys) {
	const {prefix} = options;
	const {scope, type, id} = subject;

	const TableName = dynamodb.relationshipEntriesTableName(prefix);

	const Items = keys.map((key, i) => {
		assert.isNotEmpty(key, `key[${i}]`);
		assert.isNonEmptyString(key.scope, `key[${i}].predicate`);
		assert.isNonEmptyString(key.type, `key[${i}].predicate`);
		assert.isNonEmptyString(key.id, `key[${i}].predicate`);
		assert.isNonEmptyString(key.predicate, `key[${i}].predicate`);
		assert.isOk(Number.isInteger(key.index), `key[${i}].index`);

		return {
			predicate: key.predicate,
			object_scope: key.scope,
			object_type: key.type,
			object_id: key.id,
			index: key.index,
			subject_key: `${scope}:${type}:${id}`,
			object_key: `${key.scope}:${key.type}:${key.id}`,
			predicate_key: `${key.predicate}:${key.type}:${key.id}:${key.index}`
		};
	});

	return dynamodb.batchSetItems(options, {TableName, Items}).then((res) => {
		emitRollback(function batchSetItemsRollback() {
			const Keys = Items.map(pick([`subject_key`, `predicate_key`]));
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
