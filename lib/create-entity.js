'use strict';

const {StackedError} = require(`kixx`);
const {assert, complement, isEmpty, isObject, pick} = require(`kixx/library`);
const {waitForAllPromises} = require(`./library`);
const createRelationshipEntries = require(`./create-relationship-entries`);
const createIndexEntries = require(`./create-index-entries`);

const isNotEmpty = complement(isEmpty);
const isNotObject = complement(isObject);

function relationshipsHashToKeys(relationships) {
	if (isEmpty(relationships) || isNotObject(relationships)) return [];

	return Object.keys(relationships).reduce((keys, key) => {
		const predicate = key;
		const objects = relationships[key];

		return keys.concat(objects.map((object, index) => {
			assert.isNotEmpty(object, `relationship link object[${index}]`);
			assert.isNonEmptyString(object.scope, `relationship link object[${index}].scope String`);
			assert.isNonEmptyString(object.type, `relationship link object[${index}].type String`);
			assert.isNonEmptyString(object.id, `relationship link object[${index}].id String`);

			return {
				predicate,
				scope: object.scope,
				type: object.type,
				id: object.id,
				index
			};
		}));
	}, []);
}

module.exports = function createEntity(dynamodb, redis, options, emitRollback, subject) {
	assert.isNotEmpty(options, `options`);
	assert.isNotEmpty(subject, `subject`);

	assert.isOk(Array.isArray(options.mappers), `options.mappers Array`);
	assert.isNonEmptyString(options.prefix, `options.prefix String`);

	assert.isNonEmptyString(subject.scope, `subject.scope String`);
	assert.isNonEmptyString(subject.type, `subject.type String`);
	assert.isNonEmptyString(subject.id, `subject.id String`);

	const {mappers, prefix} = options;
	const {scope, type, id} = subject;

	const created = new Date().toISOString();
	const updated = created;

	const attributes = subject.attributes || {};
	const meta = Object.assign({}, subject.meta || {}, {created, updated});

	const TableName = dynamodb.entitiesMasterTableName(prefix);

	const Item = {
		scope,
		type,
		id,
		created,
		updated,
		scope_type_key: `${scope}:${type}`,
		attributes,
		meta
	};

	const relationshipEntries = relationshipsHashToKeys(subject.relationships);

	const indexEntries = mappers.reduce((indexEntries, mapper) => {
		return indexEntries.concat(mapper(subject));
	}, []);

	const promises = [];

	promises.push(
		dynamodb.createItem(options, {
			TableName,
			PrimaryPartitionKey: `id`,
			Item
		}).then(function (res) {
			emitRollback(function createItemRollback() {
				const Key = pick([`id`, `scope_type_key`], Item);
				return dynamodb.deleteItem({TableName, Key});
			});
			return res;
		})
	);

	if (isNotEmpty(relationshipEntries)) {
		promises.push(
			createRelationshipEntries(
				dynamodb,
				redis,
				options,
				emitRollback,
				subject,
				relationshipEntries
			)
		);
	}

	if (isNotEmpty(indexEntries)) {
		promises.push(
			createIndexEntries(
				dynamodb,
				redis,
				options,
				emitRollback,
				subject,
				indexEntries
			)
		);
	}

	return waitForAllPromises(promises).then((results) => {
		const [entity, relationships, index] = results;
		const data = entity.data;
		const meta = {
			entity: entity.meta,
			relationships: relationships.meta,
			index: index.meta
		};
		return {data, meta};
	}).catch((err) => {
		return Promise.reject(new StackedError(
			`Error in RynoDB createEntity()`,
			err
		));
	});
};
