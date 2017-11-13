'use strict';

const Promise = require(`bluebird`);
const {StackedError, ProgrammerError, InvariantError} = require(`kixx`);
const {
	isObject,
	isFunction,
	isNonEmptyString,
	assoc,
	complement,
	compact,
	clone,
	find,
	append,
	all,
	omit,
	unnest,
	differenceWith,
	uniqWith,
	identity} = require(`kixx/library`);
const {hasKey} = require(`./library`);

const Record = require(`./classes/record`);
const ResourceObject = require(`./classes/resource-object`);
const Response = require(`./classes/response`);

const createInternalApi = require(`./create-internal-api`);

const setObject = require(`./set-object`);
const getObject = require(`./get-object`);
const removeObject = require(`./remove-object`);
const batchSetObjects = require(`./batch-set-objects`);
const batchGetObjects = require(`./batch-get-objects`);
const batchRemoveObjects = require(`./batch-remove-objects`);

const differenceByKey = differenceWith(hasKey);
const uniqueByKey = uniqWith(hasKey);

module.exports = function transactionFactory(options) {
	if (!isObject(options)) {
		throw new ProgrammerError(
			`transactionFactory() options must be a plain Object`
		);
	}

	const events = options.events;
	const dynamodb = options.dynamodb;
	const dynamodbTablePrefix = options.dynamodbTablePrefix;

	const globalOptions = omit([
		`events`,
		`dynamodb`,
		`dynamodbTablePrefix`
	], options);

	if (!events || !isFunction(events.broadcast)) {
		throw new ProgrammerError(
			`transactionFactory() options must include a valid events channel (Object)`
		);
	}
	if (!isObject(dynamodb)) {
		throw new ProgrammerError(
			`transactionFactory() options must include a valid AWS DynamoDB instance as .dynamodb`
		);
	}
	if (!isNonEmptyString(dynamodbTablePrefix) || !/^[a-z_]+$/.test(dynamodbTablePrefix)) {
		throw new ProgrammerError(
			`transactionFactory() options must include a valid options.dynamodbTablePrefix String`
		);
	}

	function emitWarning(err) {
		events.broadcast({
			type: `RYNODB`,
			pattern: `warning`,
			error: err
		});
	}

	function emitError(err) {
		events.broadcast({
			type: `RYNODB`,
			pattern: `error`,
			error: err
		});
	}

	const api = createInternalApi(
		{
			dynamodb: {prefix: options.dynamodbTablePrefix}
		},
		dynamodb
	);

	return function createTransaction() {
		const transactionCache = createTransactionCache();

		function get(args) {
			const {scope, key, include} = args;
			const {type, id} = key;

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: true},
				args.options
			);

			let returnMeta = [];

			let promise;
			const cachedRes = transactionCache.get(scope, key);
			if (cachedRes.data) {
				promise = Promise.resolve(cachedRes);
			} else {
				returnMeta = append(cachedRes.meta, returnMeta);
				promise = getObject(api, options, scope, key);
			}

			return promise.then((res) => {
				const {data, meta} = res;
				returnMeta = append(meta, returnMeta);

				// If the resource was not found, just return here.
				if (!data) {
					return Response.create({
						data: ResourceObject.create(data),
						meta: returnMeta
					});
				}

				// Set the item in the transaction cache in case it's used again during
				// the transaction.
				transactionCache.set(scope, data);

				// Handle an includes query.
				if (data.relationships && include && include.length > 0) {

					// Aggregate the object keys we'll need to fetch.
					const keys = include.reduce((keys, rname) => {
						return keys.concat(data.relationships[rname] || []);
					}, []);

					// If there aren't any keys to fetch, just fall through to the bottom
					// return statement.
					if (keys.length > 0) {

						// Fetch the objects in batch mode, resetting the cache if there is
						// a cache miss on the get.
						return batchGetObjects(api, options, scope, keys).then((res) => {
							const {data, meta} = res;

							returnMeta = append(meta, returnMeta);

							const items = data.filter((item, i) => {
								if (!item) {
									const key = keys[i];
									emitWarning(new InvariantError(
										`RynoDB corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
									));
									return false;
								}

								// Set each item in the transaction cache in case it's used
								// again during the transaction.
								transactionCache.set(scope, item);
								return true;
							});

							return Response.create({
								data: ResourceObject.create(data),
								included: items.map(ResourceObject.create),
								meta: returnMeta
							});
						});
					}
				}

				// No includes query? Just return the resource object.
				return Response.create({
					data: ResourceObject.create(data),
					meta: returnMeta
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB get()`,
					err,
					get
				));
			});
		}

		function set(args) {
			const {scope, object} = args;
			const record = Record.create(scope, object);

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: false},
				args.options
			);

			let returnMeta = [];

			// Check the transaction cache for this object so we can get the delta
			// for foreign key updates.
			const cachedRes = transactionCache.get(scope, record);
			returnMeta = append(cachedRes.meta, returnMeta);

			return setObject(api, options, scope, record).then((res) => {
				const {data, meta} = res;
				returnMeta = append(meta, returnMeta);

				transactionCache.set(scope, data);

				// When an object is updated, we need to determine the foreignKeys
				// added/removed from the relationships Hash, find each related
				// object, and update its foreignKeys Set.
				//
				// Here we update foreign keys in the background while allowing the
				// response to be returned.
				updateForeignKeys(scope, cachedRes.data, data).catch((err) => {
					return emitError(new StackedError(
						`Error in RynoDB set() while updating foreign keys in the background`,
						err,
						set
					));
				});

				return Response.create({
					data: ResourceObject.create(data),
					meta: returnMeta
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB set()`,
					err,
					set
				));
			});
		}

		function remove(args) {
			const {scope, key} = args;

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: false},
				args.options
			);

			let returnMeta = [];

			// Typically the caller will not call transaction.get() before calling
			// transaction.remove(), thus we never get a chance to cache the object in the
			// transactionCache, requiring us to fetch it here before removing it.
			let promise;
			const cachedRes = transactionCache.get(scope, key);
			if (cachedRes.data) {
				promise = Promise.resolve(cachedRes);
			} else {
				returnMeta = append(cachedRes.meta, returnMeta);
				promise = getObject(api, options, scope, key);
			}

			return promise.then((res) => {
				const {meta, data} = res;
				returnMeta = append(meta, returnMeta);

				// If the resource was not found, just return false.
				if (!data) {
					return Response.create({
						data: false,
						meta: returnMeta
					});
				}

				// Remove relationship links from other objects which reference
				// this one. A list of object ids which hold references is stored
				// in the foreignKeys Set.
				return removeForeignKeyRelationships(scope, data).then(() => {
					// Once we remove all references from foreign key relationships
					// it's time to remove the object itself.
					return removeObject(api, options, scope, key).then((res) => {
						const {meta} = res;
						returnMeta = append(meta, returnMeta);

						transactionCache.remove(scope, key);

						return Response.create({
							data: true,
							meta: returnMeta
						});
					});
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB remove()`,
					err,
					remove
				));
			});
		}

		function batchGet(args) {
			const {scope, keys} = args;

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: true},
				args.options
			);

			let returnMeta = [];

			const cachedResults = transactionCache.batchGet(scope, keys);
			returnMeta = append(cachedResults.meta, returnMeta);

			if (all(identity, cachedResults)) {
				return Response.create({
					data: cachedResults.data.map(ResourceObject.create),
					meta: returnMeta
				});
			}

			const missedKeys = keys.filter((key) => {
				return !find(hasKey(key), cachedResults.data);
			});

			return batchGetObjects(api, options, scope, missedKeys).then((res) => {
				const {meta} = res;
				returnMeta = append(meta, returnMeta);

				transactionCache.batchSet(scope, res.data);

				if (missedKeys.length === keys.length) {
					return Response.create({
						data: res.data.map(ResourceObject.create),
						meta: returnMeta
					});
				}

				const data = keys.map((key, i) => {
					if (cachedResults.data[i]) {
						return cachedResults.data[i];
					}
					return find(hasKey(key), res.data);
				});

				return Response.create({
					data: data.map(ResourceObject.create),
					meta: returnMeta
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB batchGet()`,
					err,
					batchGet
				));
			});
		}

		function batchSet(args) {
			const {scope, objects} = args;

			const records = objects.map(Record.createWithScope(scope));

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: false},
				args.options
			);

			let returnMeta = [];

			const cachedResults = transactionCache.batchGet(scope, records);
			returnMeta = append(cachedResults.meta, returnMeta);

			return batchSetObjects(api, options, scope, records).then((res) => {
				const {meta, data} = res;
				returnMeta = append(meta, returnMeta);

				transactionCache.batchSet(scope, data);

				// When an object is updated, we need to determine the foreignKeys
				// added/removed from the relationships Hash, find each related
				// object, and update its foreignKeys Set.
				//
				// Here we update foreign keys in the background while allowing the
				// response to be returned.
				data.reduce((promise, obj, i) => {
					return promise.then(() => {
						return updateForeignKeys(
							scope,
							cachedResults.data[i],
							obj
						);
					});
				}, Promise.resolve(null)).catch((err) => {
					return emitError(new StackedError(
						`Error in RynoDB batchSet() while updating foreign keys in the background`,
						err,
						batchSet
					));
				});

				return Response.create({
					data,
					meta: returnMeta
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB batchSet()`,
					err,
					batchSet
				));
			});
		}

		function batchRemove(args) {
			const {scope, keys} = args;

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: false},
				args.options
			);

			let returnMeta = [];

			// Typically the caller will not call transaction.get() before calling
			// transaction.remove(), thus we never get a chance to cache the object in the
			// transactionCache, requiring us to fetch it here before removing it.
			const cachedResults = transactionCache.batchGet(scope, keys);
			returnMeta = append(cachedResults.meta, returnMeta);

			const missedKeys = keys.filter((key) => {
				return !find(hasKey(key), cachedResults.data);
			});

			return batchGetObjects(api, options, scope, missedKeys).then((res) => {
				const {meta} = res;
				returnMeta = append(meta, returnMeta);

				transactionCache.batchSet(scope, res.data);

				const data = keys.map((key, i) => {
					if (cachedResults.data[i]) {
						return Record.create(scope, cachedResults.data[i]);
					}
					return find(hasKey(key), res.data);
				});

				// Remove relationship links from other objects which reference
				// this one. A list of object ids which hold references is stored
				// in the foreignKeys Set.
				return data.reduce((promise, record) => {
					return removeForeignKeyRelationships(scope, record);
				}, Promise.resolve(null)).then(() => {
					// Once we remove all references from foreign key relationships
					// it's time to remove the objects themselves.
					return batchRemoveObjects(api, options, scope, keys).then((res) => {
						const {meta, data} = res;
						returnMeta = append(meta, returnMeta);

						transactionCache.batchRemove(scope, data);

						return Response.create({
							data,
							meta: returnMeta
						});
					});
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB batchRemove()`,
					err,
					batchRemove
				));
			});
		}

		function scan(args) {
			const {scope, type, cursor, limit} = args;

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				args.options
			);

			let returnMeta = [];

			return api.dynamodbScanQuery(options.dynamodb, {cursor, limit}, scope, type).then((res) => {
				const {data, cursor, meta} = res;
				returnMeta = append(meta, returnMeta);

				transactionCache.batchSet(scope, data);

				return Response.create({
					data: data.map(ResourceObject.create),
					cursor,
					meta: returnMeta
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB scan()`,
					err,
					scan
				));
			});
		}

		// When an object is updated, we need to determine the foreignKeys added/removed from
		// the relationships Hash, find each related object, and update its foreignKeys
		// Set.
		function updateForeignKeys(scope, oldObject, newObject) {
			oldObject = Record.create(scope, oldObject);
			newObject = Record.create(scope, newObject);
			const {type, id} = newObject;

			const oldKeys = oldObject.relationships ? unnest(Object.keys(oldObject.relationships).map((rname) => {
				return oldObject.relationships[rname];
			})) : [];

			const newKeys = newObject.relationships ? unnest(Object.keys(newObject.relationships).map((rname) => {
				return newObject.relationships[rname];
			})) : [];

			// Find the set (no duplicates) of keys which are in oldKeys but not
			// in the newKeys. These are keys which must be removed.
			const toRemove = differenceByKey(oldKeys, newKeys);

			// Find the set (no duplicates) of keys which are in newKeys but not
			// in the oldKeys. These are keys which must be added.
			const newlyAdded = differenceByKey(newKeys, oldKeys);

			return Promise.all([
				removeForeignKey(scope, type, id, toRemove),
				addForeignKey(scope, type, id, newlyAdded)
			]);
		}

		function removeForeignKey(scope, type, id, keys) {
			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: true}
			);

			return batchGetObjects(api, options, scope, keys).then((res) => {
				const objects = res.data.filter((obj, i) => {
					if (!obj) {
						const key = keys[i];
						emitWarning(new InvariantError(
							`RynoDB corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
						));
					}

					return Boolean(obj);
				});

				const mutatedObjects = objects.map((obj) => {
					// Return a copy of the original object rather than mutating it.
					return assoc(
						`foreignKeys`,
						obj.foreignKeys.filter(complement(hasKey({type, id}))),
						obj
					);
				});

				return batchSetObjects(api, options, scope, mutatedObjects);
			});
		}

		function addForeignKey(scope, type, id, keys) {
			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: true}
			);

			return batchGetObjects(api, options, scope, keys).then((res) => {
				const objects = res.data.filter((obj, i) => {
					if (!obj) {
						const key = keys[i];
						emitWarning(new InvariantError(
							`RynoDB corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
						));
					}

					return Boolean(obj);
				});

				const mutatedObjects = objects.map((obj) => {
					// Return a copy of the original object rather than mutating it.
					return assoc(
						`foreignKeys`,
						uniqueByKey(append({type, id}, obj.foreignKeys)),
						obj
					);
				});

				return batchSetObjects(api, options, scope, mutatedObjects);
			});
		}

		// When an object is removed, we need to use it's foreignKeys Set to find all
		// other objects which reference it and remove the reference from those
		// objects' relationships Map.
		function removeForeignKeyRelationships(scope, obj) {
			const {type, id, foreignKeys} = obj;

			if (!foreignKeys || foreignKeys.length === 0) {
				return Promise.resolve(true);
			}

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: true}
			);

			return batchGetObjects(api, options, scope, foreignKeys).then((res) => {
				const objects = res.data.filter((obj, i) => {
					if (!obj) {
						const key = foreignKeys[i];
						emitWarning(new InvariantError(
							`RynoDB corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} foreignKeys`
						));
					}
					return Boolean(obj);
				});

				const mutatedObjects = compact(objects.map((obj) => {
					const relationships = obj.relationships;
					if (!relationships) return null;

					const newRelationships = Object.keys(relationships).reduce((r, rname) => {
						r[rname] = relationships[rname].filter(complement(hasKey({type, id})));
						return r;
					}, Object.create(null));

					// Return a copy of the object rather than mutating it.
					return assoc(`relationships`, newRelationships, obj);
				}));

				return batchSetObjects(api, options, scope, mutatedObjects);
			});
		}

		return {
			get,
			batchGet,
			set,
			batchSet,
			remove,
			batchRemove,
			scan,

			commit() {
				return Promise.resolve(true);
			},

			rollback() {
				return Promise.resolve(false);
			}
		};
	};
};

function createTransactionCache() {
	const cache = Object.create(null);

	function get(scope, key) {
		const {type, id} = key;
		const k = `${scope}:${type}:${id}`;
		const obj = cache[k] ? clone(cache[k]) : null;
		return {
			data: obj,
			meta: {transactionCacheHit: Boolean(obj)}
		};
	}

	function batchGet(scope, keys) {
		const data = keys.map((key) => {
			const {type, id} = key;
			const k = `${scope}:${type}:${id}`;
			return cache[k] ? clone(cache[k]) : null;
		});

		const transactionCacheHits = compact(data).length;
		const transactionCacheMisses = keys.length - transactionCacheHits;

		return {
			data,
			meta: {transactionCacheHits, transactionCacheMisses}
		};
	}

	function set(scope, obj) {
		const {type, id} = obj;
		const k = `${scope}:${type}:${id}`;
		cache[k] = Record.create(scope, clone(obj));
		return {data: cache[k], meta: Object.create(null)};
	}

	function batchSet(scope, objects) {
		const data = objects.map((obj) => set(scope, obj)).map((res) => res.data);
		return {data, meta: Object.create(null)};
	}

	function remove(scope, key) {
		const {type, id} = key;
		const k = `${scope}:${type}:${id}`;
		return {
			data: delete cache[k],
			meta: Object.create(null)
		};
	}

	function batchRemove(scope, keys) {
		const data = keys.map((key) => remove(scope, key)).map((res) => res.data);
		return {data, meta: Object.create(null)};
	}

	return {
		get,
		batchGet,
		set,
		batchSet,
		remove,
		batchRemove
	};
}
