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

//
// Main export
//
module.exports = function transactionFactory(options) {
	// Start with argument validation checks.
	if (!isObject(options)) {
		throw new ProgrammerError(
			`transactionFactory() options must be a plain Object`
		);
	}

	// Deconstruct some important arguments.
	const events = options.events;
	const dynamodb = options.dynamodb;
	const dynamodbTablePrefix = options.dynamodbTablePrefix;

	// Create the global options which will be used throughout the transaction API methods.
	const globalOptions = omit([
		`events`,
		`dynamodb`,
		`dynamodbTablePrefix`
	], options);

	// More validation checks.
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

	// Helper function to emit standard warning events on the Kixx event bus.
	function emitWarning(err) {
		events.broadcast({
			type: `RYNODB`,
			pattern: `warning`,
			error: err
		});
	}

	// Helper function to emit standard warning events on the Kixx event bus.
	function emitError(err) {
		events.broadcast({
			type: `RYNODB`,
			pattern: `error`,
			error: err
		});
	}

	// Construct the API. This is basically just partial application of some
	// basic CRUD functions. It will be used throught the transaction API methods.
	const api = createInternalApi(
		{
			dynamodb: {prefix: options.dynamodbTablePrefix}
		},
		dynamodb
	);

	return function createTransaction() {
		const transactionCache = createTransactionCache();

		// - `args.scope` *required*
		// - `args.key.type` *required*
		// - `args.key.id` *required*
		// - `args.include`
		// - `args.options.resetCache` (default=true)
		// - `args.options.useCache` (default=true)
		function get(args) {
			const {scope, key, include} = args;
			const {type, id} = key;

			// Construct a new options Object used specifically for this execution
			// of this this method.
			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: true},
				args.options
			);

			let returnMeta = [];

			// Returns cachedRes.data as a Record Object.
			const cachedRes = transactionCache.get(scope, key);

			// If we got a transactionCache hit, construct a resolved promise from it.
			// Otherwise fetch the object from the store using getObject().
			let promise;
			if (cachedRes.data) {
				promise = Promise.resolve(cachedRes);
				// The `returnMeta` will be appended when the promise resolves below.
			} else {
				returnMeta = append(cachedRes.meta, returnMeta);
				promise = getObject(api, options, scope, key);
			}

			return promise.then((res) => {
				const {data, meta} = res;
				// Create the ResourceObject instance expected to be returned to the caller.
				const resource = ResourceObject.create(data);

				// Update the `returnMeta`.
				returnMeta = append(meta, returnMeta);

				// If the resource was not found, just return here.
				if (!data) {
					return Response.create({
						data: resource,
						meta: returnMeta
					});
				}

				// Set the item in the transaction cache in case it's used again during
				// the transaction. The transactionCache expects objects to be sent as plain
				// database records (not transformed into a Record instance).
				transactionCache.set(scope, data);

				//
				// Handle an includes query if there is one.
				//
				if (data.relationships && include && include.length > 0) {

					// Aggregate the object keys we'll need to fetch.
					const keys = include.reduce((keys, rname) => {
						return keys.concat(data.relationships[rname] || []);
					}, []);

					// If there aren't any keys to fetch, just fall through to the bottom
					// return statement.
					if (keys.length > 0) {
						// Try to fetch the objects from the transactionCache first.
						const cachedRes = transactionCache.batchGet(scope, keys);

						// Update the `returnMeta`.
						returnMeta = append(cachedRes.meta, returnMeta);

						// Remove any null elements from the cachedRes.data Array.
						const cached = compact(cachedRes.data);

						// If we got all the required keys from the transactionCache just return it here.
						if (cached.length === keys.length) {
							return Response.create({
								data: resource,
								included: cached.map(ResourceObject.create),
								meta: returnMeta
							});
						}

						// TODO: Handle case where only some items in the set come from the cache.

						// At this point all objects have missed the transactionCache.
						// Fetch the objects from the store in batch mode.
						return batchGetObjects(api, options, scope, keys).then((res) => {
							const {data, meta} = res;

							// Update the returnMeta again.
							returnMeta = append(meta, returnMeta);

							// Check each item for referencial integrity.
							const items = data.filter((item, i) => {
								if (!item) {
									const key = keys[i];
									emitWarning(new InvariantError(
										`RynoDB corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
									));
									return false;
								}
								return true;
							});

							// Set each item in the transaction cache in case it's used
							// again during the transaction.
							transactionCache.batchSet(scope, items);

							return Response.create({
								data: resource,
								included: items.map(ResourceObject.create),
								meta: returnMeta
							});
						});
					}
				}

				// No includes query? Just return the resource object.
				return Response.create({
					data: resource,
					meta: returnMeta
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB get()`, // Error message.
					err, // The "cause" error.
					get // The context function for the stack trace.
				));
			});
		}

		// - `args.scope` *required*
		// - `args.object.type` *required*
		// - `args.object.id` *required*
		// - `args.isolated` (default=false)
		// - `args.options.resetCache` (default=true)
		// - `args.options.useCache` (default=false)
		//
		// Setting `args.isolated` to `true` will cause the operation to wait until
		// foreign key references are updated before resolving. Otherwise, foreign
		// key references will be updated asynchronously in the background.
		// More information: https://en.wikipedia.org/wiki/Isolation_(database_systems)
		function set(args) {
			const {scope, object} = args;
			const isolated = Boolean(args.isolated);
			const {type, id} = object;
			const record = Record.create(scope, object);

			// Construct a new options Object used specifically for this execution
			// of this this method.
			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: false},
				args.options
			);

			let returnMeta = [];

			// Get the original object if it exists so we can reference it for foreign key updates
			// after we set it. If we got a transactionCache hit, construct a resolved promise from it.
			// Otherwise fetch the object from the store using getObject().
			let original;
			const cachedRes = transactionCache.get(scope, record);

			// Update `returnMeta`.
			returnMeta = append(cachedRes.meta, returnMeta);

			if (cachedRes.data) {
				original = Promise.resolve(cachedRes.data);
			} else {
				original = getObject(api, options, scope, {type, id}).then((res) => {
					return res.data;
				});
			}

			// Write the object to the store.
			return setObject(api, options, scope, record).then((res) => {
				const {data, meta} = res;
				// Create the ResourceObject instance expected to be returned to the caller.
				const resource = ResourceObject.create(data);

				// Update the `returnMeta`.
				returnMeta = append(meta, returnMeta);

				// Set the item in the transaction cache in case it's used again during
				// the transaction. The transactionCache expects objects to be sent as plain
				// database records (not transformed into a Record instance).
				transactionCache.set(scope, data);

				// When an object is updated, we need to determine the foreignKeys
				// added/removed from the relationships Hash, find each related
				// object, and update its foreignKeys Set.
				const fkupdate = original.then((originalObject) => {
					if (!originalObject) return false;
					return updateForeignKeys(scope, originalObject, data).catch((err) => {
						return emitError(new StackedError(
							`Error in RynoDB set() while updating foreign keys in the background`,
							err,
							set
						));
					});
				});

				function returnResponse() {
					return Response.create({
						data: resource,
						meta: returnMeta
					});
				}

				if (isolated) {
					// Wait for the foreign key update to complete.
					return fkupdate.then(returnResponse);
				}

				// If `isolated` is not `true` then allow the foreign key update to
				// complete asynchronously in the background.
				return returnResponse();
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB set()`, // Error message.
					err, // The "cause" error.
					set // The context function for the stack trace.
				));
			});
		}

		// - `args.scope` *required*
		// - `args.key.type` *required*
		// - `args.key.id` *required*
		// - `args.options.resetCache` (default=true)
		// - `args.options.useCache` (default=false)
		function remove(args) {
			const {scope, key} = args;

			// Construct a new options Object used specifically for this execution
			// of this this method.
			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: false},
				args.options
			);

			let returnMeta = [];

			// Typically the caller will not call transaction.get() before calling
			// transaction.remove(), thus we never get a chance to cache the object in the
			// transactionCache, requiring us to fetch it here before removing it. This
			// is necessary to track down and remove all foreign key references.

			// Returns cachedRes.data as a Record Object.
			const cachedRes = transactionCache.get(scope, key);

			// If we got a transactionCache hit, construct a resolved promise from it.
			// Otherwise fetch the object from the store using getObject().
			let promise;
			if (cachedRes.data) {
				// The `returnMeta` will be appended when the promise resolves below.
				promise = Promise.resolve(cachedRes);
			} else {
				returnMeta = append(cachedRes.meta, returnMeta);
				promise = getObject(api, options, scope, key);
			}

			return promise.then((res) => {
				const {meta, data} = res;

				// Update the `returnMeta`.
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
				// in the foreignKeys Set on this object (`data`).
				return removeForeignKeyRelationships(scope, data).then(() => {
					// Once we remove all references from foreign key relationships
					// it's time to remove the object itself.
					return removeObject(api, options, scope, key).then((res) => {
						const {meta} = res;
						returnMeta = append(meta, returnMeta);

						// Remove the object from the transactionCache while we're at it.
						transactionCache.remove(scope, key);

						return Response.create({
							data: true,
							meta: returnMeta
						});
					});
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB remove()`, // Error message.
					err, // The "cause" error.
					remove // THe context function for the stack trace.
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

			if (all(identity, cachedResults.data)) {
				return Promise.resolve(Response.create({
					data: cachedResults.data.map(ResourceObject.create),
					meta: returnMeta
				}));
			}

			const missedKeys = keys.filter((key) => {
				return !find(hasKey(key), compact(cachedResults.data));
			});

			return batchGetObjects(api, options, scope, missedKeys).then((res) => {
				const {meta} = res;
				returnMeta = append(meta, returnMeta);

				transactionCache.batchSet(scope, compact(res.data));

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
					`Error in RynoDB batchGet()`, // Error message.
					err, // The "cause" error.
					batchGet // The context function for the stack trace.
				));
			});
		}

		//
		// Setting `args.isolated` to `true` will cause the operation to wait until
		// foreign key references are updated before resolving. Otherwise, foreign
		// key references will be updated asynchronously in the background.
		// More information: https://en.wikipedia.org/wiki/Isolation_(database_systems)
		function batchSet(args) {
			const {scope, objects} = args;
			const isolated = Boolean(args.isolated);

			const records = objects.map(Record.createWithScope(scope));

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				{resetCache: true, useCache: false},
				args.options
			);

			let returnMeta = [];

			// Get the original object if it exists so we can reference it for foreign key updates
			// after we set it.
			let originals;
			const cachedResults = transactionCache.batchGet(scope, records);
			returnMeta = append(cachedResults.meta, returnMeta);
			const cachedData = uniqueByKey(compact(cachedResults.data));
			// Find the set (no duplicates) of keys which are in set records but not
			// in the cached results retrieved earlier. These are keys which must be fetched.
			const toFetch = differenceByKey(objects, cachedData);
			if (toFetch.length > 0) {
				originals = batchGetObjects(api, options, scope, toFetch).then((res) => {
					return cachedData.concat(compact(res.data));
				});
			} else {
				originals = Promise.resolve(cachedData);
			}

			// Write the objects to the store.
			return batchSetObjects(api, options, scope, records).then((res) => {
				const {meta, data} = res;
				returnMeta = append(meta, returnMeta);

				transactionCache.batchSet(scope, data);

				// When an object is updated, we need to determine the foreignKeys
				// added/removed from the relationships Hash, find each related
				// object, and update its foreignKeys Set.
				const fkupdates = originals.then((originals) => {
					return uniqueByKey(data).reduce((promise, obj) => {
						const {type, id} = obj;
						return promise.then(() => {
							const original = find(hasKey({type, id}), originals);
							if (original) return updateForeignKeys(scope, original, obj);
							return false;
						});
					}, Promise.resolve(null)).catch((err) => {
						return emitError(new StackedError(
							`Error in RynoDB batchSet() while updating foreign keys in the background`,
							err,
							batchSet
						));
					});
				});

				function returnResponse() {
					return Response.create({
						data: data.map(ResourceObject.create),
						meta: returnMeta
					});
				}

				if (isolated) {
					// Wait for the foreign key update to complete.
					return fkupdates.then(returnResponse);
				}

				// Allow the foreign key update to complete in the background.
				return returnResponse();
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB batchSet()`, // Error message.
					err, // The "cause" error.
					batchSet // The context function for the stack trace.
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
			const cachedData = uniqueByKey(compact(cachedResults.data));

			const missedKeys = keys.filter((key) => {
				return !find(hasKey(key), cachedData);
			});

			return batchGetObjects(api, options, scope, missedKeys).then((res) => {
				const {meta} = res;
				returnMeta = append(meta, returnMeta);

				transactionCache.batchSet(scope, res.data);

				const data = keys.map((key) => {
					return find(hasKey(key), res.data) || find(hasKey(key), cachedData);
				});

				// Remove relationship links from other objects which reference
				// this one. A list of object ids which hold references is stored
				// in the foreignKeys Set.
				return data.reduce((promise, record) => {
					return promise.then(() => {
						return removeForeignKeyRelationships(scope, record);
					});
				}, Promise.resolve(null)).then(() => {
					// Once we remove all references from foreign key relationships
					// it's time to remove the objects themselves.
					return batchRemoveObjects(api, options, scope, keys).then((res) => {
						const {meta, data} = res;
						returnMeta = append(meta, returnMeta);

						transactionCache.batchRemove(scope, keys);

						return Response.create({
							data,
							meta: returnMeta
						});
					});
				});
			}).catch((err) => {
				return Promise.reject(new StackedError(
					`Error in RynoDB batchRemove()`, // Error message.
					err, // The "cause" error.
					batchRemove // The context function for the stack trace.
				));
			});
		}

		function scan(args) {
			const {scope, type, cursor, limit} = args;

			const options = Object.assign(
				Object.create(null),
				globalOptions,
				args.options,
				{cursor, limit}
			);

			let returnMeta = [];

			return api.dynamodbScanQuery(options, scope, type).then((res) => {
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
					`Error in RynoDB scan()`, // Error message.
					err, // The "cause" error.
					scan // The context function for the stack trace.
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

			if (toRemove.length === 0 && newlyAdded.length === 0) {
				return Promise.resolve(null);
			}

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

				return batchSetObjects(api, options, scope, mutatedObjects).then((res) => {
					transactionCache.batchSet(scope, res.data);
					return res;
				});
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

				return batchSetObjects(api, options, scope, mutatedObjects).then((res) => {
					transactionCache.batchSet(scope, res.data);
					return res;
				});
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

				return batchSetObjects(api, options, scope, mutatedObjects).then((res) => {
					transactionCache.batchSet(scope, res.data);
					return res;
				});
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
		cache[k] = obj;
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
