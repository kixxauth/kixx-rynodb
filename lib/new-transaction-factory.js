'use strict';

const {complement, deepFreeze, isEmpty, mergeDeepRight} = require(`kixx/library`);
const {waitForAllPromises} = require(`./library`);
const createEntity = require(`./create-entity`);
const getEntityWithIncludes = require(`./get-entity-with-includes`);
const getEntity = require(`./get-entity`);
const updateEntity = require(`./update-entity`);
const removeEntity = require(`./remove-entity`);
const replaceEntityRelationships = require(`./replace-entity-relationships`);
const appendEntityRelationships = require(`./append-entity-relationships`);
const removeEntityRelationships = require(`./remove-entity-relationships`);
const batchGetEntities = require(`./batch-get-entities`);
const batchSetEntities = require(`./batch-set-entities`);
const batchRemoveEntities = require(`./batch-remove-entities`);
const scanEntities = require(`./scan-entities`);
const queryIndex = require(`./query-index`);

const isNotEmpty = complement(isEmpty);

module.exports = function transactionFactory() {
	const defaultOptions = deepFreeze({
		prefix: null
	});

	const dynamodb = null;
	const redis = null;

	// TODO: Add transaction memory cache.
	// TODO: Add redis cache.
	// TODO: Add application memory cache.
	// TODO: Allow options.commit to force commit within a transaction.

	return function createTransaction() {
		const rollbacks = [];
		const operations = [];

		function emitRollbackFunction(rollbackFunction) {
			rollbacks.push(rollbackFunction);
			return null;
		}

		function create(subject, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			operations.push(function () {
				return createEntity(dynamodb, redis, options, emitRollbackFunction, subject);
			});

			return operations.length - 1;
		}

		function get(key, options = {}) {
			options = mergeDeepRight(defaultOptions, options);
			const {includes} = options;

			if (isNotEmpty(includes)) {
				return getEntityWithIncludes(dynamodb, redis, options, key, includes);
			}
			return getEntity(dynamodb, redis, options, key);
		}

		function update(subject, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			operations.push(function () {
				return updateEntity(dynamodb, redis, options, emitRollbackFunction, subject);
			});

			return operations.length - 1;
		}

		function remove(key, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			operations.push(function () {
				return removeEntity(dynamodb, redis, options, emitRollbackFunction, key);
			});

			return operations.length - 1;
		}

		function replaceRelationships({subject, predicate, objects}, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			const params = {subject, predicate, objects};

			operations.push(function () {
				return replaceEntityRelationships(dynamodb, redis, options, emitRollbackFunction, params);
			});

			return operations.length - 1;
		}

		function appendRelationships({subject, predicate, objects}, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			const params = {subject, predicate, objects};

			operations.push(function () {
				return appendEntityRelationships(dynamodb, redis, options, emitRollbackFunction, params);
			});

			return operations.length - 1;
		}

		function removeRelationships({subject, predicate, objects}, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			const params = {subject, predicate, objects};

			operations.push(function () {
				return removeEntityRelationships(dynamodb, redis, options, emitRollbackFunction, params);
			});

			return operations.length - 1;
		}

		function batchGet(keys, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			return batchGetEntities(dynamodb, redis, options, keys);
		}

		function batchSet(subjects, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			operations.push(function () {
				return batchSetEntities(dynamodb, redis, options, emitRollbackFunction, subjects);
			});

			return operations.length - 1;
		}

		function batchRemove(keys, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			operations.push(function () {
				return batchRemoveEntities(dynamodb, redis, options, emitRollbackFunction, keys);
			});

			return operations.length - 1;
		}

		function scan({scope, type, cursor, limit}, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			const params = {scope, type, cursor, limit};

			return scanEntities(dynamodb, redis, options, params);
		}

		function query({scope, index, query, cursor, limit}, options = {}) {
			options = mergeDeepRight(defaultOptions, options);

			const params = {scope, index, query, cursor, limit};

			return queryIndex(dynamodb, redis, options, params);
		}

		function commit() {
			const promises = operations.map((op) => op());
			return waitForAllPromises(promises);
		}

		function rollback() {
			const promises = rollbacks.map((rollback) => rollback());
			return Promise.all(promises);
		}

		return {
			create,
			get,
			update,
			remove,
			replaceRelationships,
			appendRelationships,
			removeRelationships,
			batchGet,
			batchSet,
			batchRemove,
			scan,
			query,
			commit,
			rollback
		};
	};
};

