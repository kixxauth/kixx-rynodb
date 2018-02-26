'use strict';

// function relationshipsHashToKeys(relationships) {
// 	if (isEmpty(relationships) || isNotObject(relationships)) return [];

// 	return Object.keys(relationships).reduce((keys, key) => {
// 		const predicate = key;
// 		const objects = relationships[key];

// 		return keys.concat(objects.map((object, index) => {
// 			return {
// 				predicate,
// 				scope: object.scope,
// 				type: object.type,
// 				id: object.id,
// 				index
// 			};
// 		}));
// 	}, []);
// }

// function createRelationshipEntries(dynamodb, redis, options, scope, subject, keys) {
// 	const {prefix} = options;
// 	const {type, id} = subject;

// 	const TableName = composeRelationshipEntriesTableName(prefix);

// 	const Items = keys.map((key) => {
// 		return {
// 			predicate: key.predicate,
// 			object_scope: key.scope,
// 			object_type: key.type,
// 			object_id: key.id,
// 			index: key.index,
// 			subject_key: `${scope}:${type}:${id}`,
// 			object_key: `${key.scope}:${key.type}:${key.id}`,
// 			predicate_key: `${key.predicate}:${key.type}:${key.id}:${key.index}`
// 		};
// 	});

// 	const batchSetItems = dynamodb.batchSetItems(options, {TableName, Items});

// 	function rollbackFunction() {
// 		const Keys = Items.map(pick([`subject_key`, `predicate_key`]));
// 		return dynamodb.batchRemoveItems(options, {TableName, Keys});
// 	}

// 	return batchSetItems.then(() => {
// 		return {rollbacks: [rollbackFunction]};
// 	}).catch((err) => {
// 		return Promise.reject(new StackedError(
// 			`Error in RynoDB createRelationshipEntries()`,
// 			err
// 		));
// 	});
// }

// function createIndexEntries(dynamodb, redis, options, scope, subject, entries) {
// 	const {prefix} = options;
// 	const {type, id} = subject;

// 	const TableName = composeIndexEntriesTableName(prefix);

// 	const Items = entries.map((entry) => {
// 		const {index_name, compound_key} = entry;
// 		return {
// 			scope,
// 			type,
// 			id,
// 			index_name,
// 			compound_key,
// 			subject_key: `${scope}:${type}:${id}`,
// 			unique_key: `${index_name}:${compound_key}`
// 		};
// 	});

// 	const batchSetItems = dynamodb.batchSetItems(options, {TableName, Items});

// 	function rollbackFunction() {
// 		const Keys = Items.map(pick([`subject_key`, `unique_key`]));
// 		return dynamodb.batchRemoveItems(options, {TableName, Keys});
// 	}

// 	return batchSetItems.then(() => {
// 		return {rollbacks: [rollbackFunction]};
// 	}).catch((err) => {
// 		return Promise.reject(new StackedError(
// 			`Error in RynoDB createRelationshipEntries()`,
// 			err
// 		));
// 	});
// }

// function createEntity(dynamodb, redis, options, scope, subject) {
// 	const {mappers, prefix} = options;
// 	const {type, id} = subject;

// 	let {attributes, meta} = subject;
// 	attributes = attributes || {};
// 	meta = meta || {};

// 	const created = new Date().toISOString();
// 	const updated = created;

// 	meta = Object.assign({}, meta, {created, updated});

// 	const TableName = composeEntitiesMasterTableName(prefix);

// 	const Item = {
// 		scope,
// 		type,
// 		id,
// 		created,
// 		updated,
// 		scope_type_key: composeScopeTypeKey(scope, type, id),
// 		attributes,
// 		meta
// 	};

// 	const relationships = relationshipsHashToKeys(subject.relationships);

// 	const indexEntries = mappers.reduce((indexEntries, mapper) => {
// 		return indexEntries.concat(mapper(subject));
// 	}, []);

// 	const rollbacks = [];

// 	function rollbackFunction() {
// 		const Key = pick([`id`, `scope_type_key`], Item);
// 	}

// 	const params = {
// 		TableName,
// 		PrimaryPartitionKey: `id`,
// 		Item
// 	};

// 	return dynamodb.createItem(options, params)
// 		.then(() => {
// 			rollbacks.push(rollbackFunction);

// 			const parallelUpdates = [];

// 			// TODO: Test if one of these fails before the other resolves, do we
// 			// still get the rollbacks from the one that resolves ... do we wait
// 			// for the second promise?

// 			if (isNotEmpty(relationships)) {
// 				parallelUpdates.push(createRelationshipEntries(
// 					dynamodb,
// 					redis,
// 					options,
// 					scope,
// 					subject,
// 					relationships
// 				).then(appendRollbacks(rollbacks)));
// 			}

// 			if (isNotEmpty(indexEntries)) {
// 				parallelUpdates.push(createIndexEntries(
// 					dynamodb,
// 					redis,
// 					options,
// 					scope,
// 					subject,
// 					indexEntries
// 				).then(appendRollbacks(rollbacks)));
// 			}

// 			if (isNotEmpty(parallelUpdates)) {
// 				return Promise.all(parallelUpdates);
// 			}

// 			return [];
// 		})
// 		.then((responses) => {
// 			const allRollbacks = responses.reduce((rollbacks, res) => {
// 				return rollbacks.concat(res.rollbacks);
// 			}, rollbacks);

// 			return {rollbacks: allRollbacks};
// 		})
// 		.catch((err) => {
// 			const newError = new StackedError(
// 				`Error in RynoDB createEntity()`,
// 				err
// 			);

// 			newError.rollbacks = rollbacks;

// 			return Promise.reject(newError);
// 		});
// }
