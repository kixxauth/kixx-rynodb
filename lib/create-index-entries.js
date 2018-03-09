'use strict';

const Promise = require(`bluebird`);
const {StackedError} = require(`kixx`);
const {assert, pick} = require(`kixx/library`);

module.exports = function createIndexEntries(dynamodb, redis, options, emitRollback, subject, entries) {
	const {prefix} = options;
	const {scope, type, id} = subject;

	const TableName = dynamodb.indexEntriesTableName(prefix);

	const Items = entries.map((entry, i) => {
		assert.isNotEmpty(entry, `entry[${i}]`);
		assert.isNonEmptyString(entry.index_name, `entry[${i}].index_name`);
		assert.isNonEmptyString(entry.compound_key, `entry[${i}].compound_key`);

		const {index_name, compound_key} = entry;

		return {
			scope,
			type,
			id,
			index_name,
			compound_key,
			subject_key: `${scope}:${type}:${id}`,
			unique_key: `${index_name}:${compound_key}`
		};
	});

	return dynamodb.batchSetItems(options, {TableName, Items}).then((res) => {
		emitRollback(function batchSetItemsRollback() {
			const Keys = Items.map(pick([`subject_key`, `unique_key`]));
			return dynamodb.batchDeleteItems(options, {TableName, Keys});
		});
		return res;
	}).catch((err) => {
		return Promise.reject(new StackedError(
			`Error in RynoDB createIndexEntries()`,
			err
		));
	});
};
