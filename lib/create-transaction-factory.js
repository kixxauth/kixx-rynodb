'use strict';

module.exports = function createTransactionFactory(options) {
	// return function createTransaction() {
	// 	const TXN = Object.create(null);

	// 	const transactionErrors = [];

	// 	const transactionCache = createTransactionCache();

	// 	TXN.get = function get(args) {
	// 		const {scope, type, id, include} = args;
	// 		const key = {type, id};

	// 		let returnMeta = [];

	// 		// Fetch the object, resetting the cache if there is a cache miss
	// 		// on the get.
	// 		const options = {
	// 			resetCache: true,
	// 			dynamodbGetObjectOptions
	// 		};

	// 		return getObject(api, scope, key, options).then((res) => {
	// 			const {data, meta} = res;

	// 			returnMeta = append(meta, returnMeta);

	// 			// Return null if not found.
	// 			if (!data) {
	// 				return ReturnValues.create({
	// 					data: null,
	// 					meta: returnMeta
	// 				});
	// 			}

	// 			const obj = data;

	// 			// Set the item in the transaction cache in case it's used again during
	// 			// the transaction.
	// 			transactionCache.set(obj);

	// 			// Handle an includes query.
	// 			if (obj.relationships && include && include.length > 0) {

	// 				// Aggregate the object keys we'll need to fetch.
	// 				const keys = include.reduce((keys, rname) => {
	// 					return keys.concat(obj.relationships[rname] || []);
	// 				}, []);

	// 				// If there aren't any keys to fetch, just fall through to the bottom
	// 				// return statement.
	// 				if (keys.length > 0) {

	// 					// Fetch the objects in batch mode, resetting the cache if there is
	// 					// a cache miss on the get.
	// 					const options = {
	// 						resetCache: true,
	// 						dynamodbBatchGetObjectsOptions
	// 					};

	// 					return batchGetObjects(api, scope, keys, options).then((res) => {
	// 						const {data, meta} = res;

	// 						returnMeta = append(meta, returnMeta);

	// 						const items = data.filter((item, i) => {
	// 							if (!item) {
	// 								const key = keys[i];
	// 								warn(new InvariantError(
	// 									`Rynodb corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
	// 								));
	// 							}

	// 							// Set each item in the transaction cache in case it's used
	// 							// again during the transaction.
	// 							transactionCache.set(item);
	// 							return Boolean(item);
	// 						});

	// 						return ReturnValues.create({
	// 							data: Resource.create(obj),
	// 							included: items.map(Resource.create),
	// 							meta: returnMeta
	// 						});
	// 					});
	// 				}
	// 			}

	// 			// No includes query? Just return the resource object.
	// 			return ReturnValues.create({
	// 				data: Resource.create(obj),
	// 				meta: returnMeta
	// 			});
	// 		}).catch((err) => {
	// 			// Stash errors to be picked up when the transaction is committed.
	// 			transactionErrors = append(new StackedError(
	// 				`Error in Rynodb Transaction#get()`,
	// 				err,
	// 				get
	// 			), transactionErrors);
	// 			return ReturnValues.create({
	// 				error: true,
	// 				meta: returnMeta
	// 			});
	// 		});
	// 	};

	// 	TXN.set = function set(scope, obj) {
	// 		const {type, id} = obj;

	// 		// Check the transaction cache for this object so we can get the delta
	// 		// for relationship and index updates.
	// 		const oldObject = transactionCache.get(scope, type, id);

	// 		return setObject(api, scope, obj).then((newObject) => {
	// 			// When an object is updated, we need to determine the foreignKeys
	// 			// added/removed from the relationships Hash, find each related
	// 			// object, and update its foreignKeys Set.
	// 			return updateForeignKeys(scope, oldObject, newObject).then(() => {
	// 				return {data: createReturnObject(newObject)};
	// 			});
	// 		}).catch((err) => {
	// 			// Stash errors to be picked up when the transaction is committed.
	// 			transactionErrors = append(new StackedError(
	// 				`Error in Rynodb Transaction#set()`,
	// 				err,
	// 				set
	// 			), transactionErrors);
	// 			return obj;
	// 		});
	// 	};

	// 	TXN.remove = function remove(args) {
	// 		const {scope, type, id} = args;
	// 		const key = {type, id};

	// 		const originalObject = transactionCache.get({scope, type, id});

	// 		const promise = originalObject ?
	// 			Promise.resolve(originalObject) :
	// 			getObject(api, scope, key, {skipCache: true});

	// 		// Typically the caller will not call TXN.get() before calling
	// 		// TXN.remove(), thus we never get a chance to cache the object in the
	// 		// transactionCache, requiring us to fetch it here before removing it.
	// 		return promise
	// 			.then((obj) => {
	// 				if (!obj) return null;

	// 				// Remove relationship links from other objects which reference
	// 				// this one. A list of object ids which hold references is stored
	// 				// in the foreignKeys Set.
	// 				return removeForeignKeyRelationships(scope, obj);
	// 			})
	// 			.then(() => {
	// 				// Once we remove all references from foreign key relationships and
	// 				// indexes it's time to remove the object itself.
	// 				return removeObject(api, scope, key);
	// 			})
	// 			.then(always(true))
	// 			.catch((err) => {
	// 				// Stash errors to be picked up when the transaction is committed.
	// 				transactionErrors = append(new StackedError(
	// 					`Error in Rynodb Transaction#remove()`,
	// 					err,
	// 					remove
	// 				), transactionErrors);
	// 				return false;
	// 			});
	// 	};

	// 	TXN.scan = function scan(args) {
	// 		const {scope, type, cursor, limit} = args;

	// 		const params = {
	// 			hashKey: {scope, type},
	// 			cursor,
	// 			limit
	// 		};

	// 		return api.dynamodbQueryObjects(params).then((res) => {
	// 			return {
	// 				data: items.map(createReturnObject),
	// 				cursor: {start: inclusiveStop + 1}
	// 			};
	// 		}).catch((err) => {
	// 			// Stash errors to be picked up when the transaction is committed.
	// 			transactionErrors = append(new StackedError(
	// 				`Error in Rynodb Transaction#scan()`,
	// 				err,
	// 				scan
	// 			), transactionErrors);
	// 			return {items: [], cursor: null};
	// 		});
	// 	};

	// 	TXN.commit = function commit() {
	// 		if (transactionErrors.length > 0) {
	// 			// Make a copy of transactionErrors using .slice() to avoid mutation of
	// 			// the private Array by the caller.
	// 			return Promise.reject(transactionErrors.slice());
	// 		}

	// 		return Promise.resolve(true);
	// 	};

	// 	TXN.rollback = function rollback() {
	// 		return Promise.resolve(true);
	// 	};

	// 	return TXN;
	// };

	// function createTransactionCache() {
	// 	const cache = Object.create(null);

	// 	return {
	// 		get(args) {
	// 			const {scope, type, id} = args;
	// 			const key = `${scope}:${type}:${id}`;
	// 			return cache[key] ? clone(cache[key]) : null;
	// 		},
	// 		set(obj) {
	// 			const {scope, type, id} = obj;
	// 			const key = `${scope}:${type}:${id}`;
	// 			cache[key] = clone(obj);
	// 			return obj;
	// 		}
	// 	};
	// }

	// // When an object is updated, we need to determine the foreignKeys added/removed from
	// // the relationships Hash, find each related object, and update its foreignKeys
	// // Set.
	// function updateForeignKeys(scope, oldObject, newObject) {
	// 	const {type, id} = newObject;

	// 	const a = oldObject.relationships ? unnest(Object.keys(oldObject.relationships).map((rname) => {
	// 		return oldObject.relationships[rname];
	// 	})) : [];

	// 	const b = newObject.relationships ? unnest(Object.keys(newObject.relationships).map((rname) => {
	// 		return newObject.relationships[rname];
	// 	})) : [];

	// 	const toRemove = differenceByKey(a, b);
	// 	const newlyAdded = differenceByKey(b, a);

	// 	return Promise.all([
	// 		removeForeignKeys(scope, type, id, toRemove),
	// 		addForeignKeys(scope, type, id, newlyAdded)
	// 	]);
	// }

	// function removeForeignKeys(scope, type, id, keys) {
	// 	const options = {
	// 		skipCache: true,
	// 		dynamodbBatchGetObjectsOptions
	// 	};

	// 	return batchGetObjects(api, scope, keys, options)
	// 		.then((objects) => {
	// 			return objects.filter((obj, i) => {
	// 				if (!obj) {
	// 					const key = keys[i];
	// 					warn(new InvariantError(
	// 						`Rynodb corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
	// 					));
	// 				}

	// 				return Boolean(obj);
	// 			});
	// 		})
	// 		.then((objects) => {
	// 			const mutatedObjects = objects.map((obj) => {
	// 				// Clone before mutating.
	// 				obj = clone(obj);
	// 				obj.foreignKeys = obj.foreignKeys.filter(keysEqual({type, id}));
	// 				return obj;
	// 			});

	// 			const options = {
	// 				dynamodbBatchSetObjectsOptions
	// 			};

	// 			return batchSetObjects(api, scope, mutatedObjects, options);
	// 		});
	// }

	// function addForeignKeys(scope, type, id, keys) {
	// 	const options = {
	// 		skipCache: true,
	// 		dynamodbBatchGetObjectsOptions
	// 	};

	// 	return batchGetObjects(api, scope, keys, options)
	// 		.then((objects) => {
	// 			return objects.filter((obj, i) => {
	// 				if (!obj) {
	// 					const key = keys[i];
	// 					warn(new InvariantError(
	// 						`Rynodb corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
	// 					));
	// 				}

	// 				return Boolean(obj);
	// 			});
	// 		})
	// 		.then((objects) => {
	// 			const mutatedObjects = objects.map((obj) => {
	// 				// Clone before mutating.
	// 				obj = clone(obj);
	// 				obj.foreignKeys = uniqueByKey(append({type, id}, obj.foreignKeys));
	// 				return obj;
	// 			});

	// 			const options = {
	// 				dynamodbBatchSetObjectsOptions
	// 			};

	// 			return batchSetObjects(api, scope, mutatedObjects, options);
	// 		});
	// }

	// // When an object is removed, we need to use it's foreignKeys Set to find all
	// // other objects which reference it and remove the reference from those
	// // objects' relationships Map.
	// function removeForeignKeyRelationships(scope, obj) {
	// 	const {type, id, foreignKeys} = obj;

	// 	if (!foreignKeys || foreignKeys.length === 0) {
	// 		return Promise.resolve(true);
	// 	}

	// 	const options = {
	// 		skipCache: true,
	// 		dynamodbBatchGetObjectsOptions
	// 	};

	// 	return batchGetObjects(api, scope, foreignKeys, options)
	// 		.then((objects) => {
	// 			return objects.filter((obj, i) => {
	// 				if (!obj) {
	// 					const key = foreignKeys[i];
	// 					warn(new InvariantError(
	// 						`Rynodb corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${key.type}.${key.id} foreignKeys`
	// 					));
	// 				}

	// 				return Boolean(obj);
	// 			});
	// 		})
	// 		.then((objects) => {
	// 			const mutatedObjects = compact(objects.map((obj) => {
	// 				const relationships = obj.relationships;
	// 				if (!relationships) return null;

	// 				// Make a copy before mutating.
	// 				obj = clone(obj);

	// 				obj.relationships = Object.keys(relationships).reduce((newr, rname) => {
	// 					newr[rname] = relationships[rname].filter((key) => {
	// 						return key.type !== type || key.id !== id;
	// 					});
	// 					return newr;
	// 				}, Object.create(null));

	// 				return obj;
	// 			}));

	// 			const options = {
	// 				dynamodbBatchSetObjectsOptions
	// 			};

	// 			return batchSetObjects(api, scope, mutatedObjects, options);
	// 		});
	// 	}
	// };
};
