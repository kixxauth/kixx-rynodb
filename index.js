exports.initialize = (app) => {
	const emitter = app.emitter;

	const API = Object.create(null);

	function warn(err) {
		emitter.emit(`warn`, err);
	}

	API.createTransaction = function createTransaction() {
		const TXN = Object.create(null);

		const transactionErrors = [];

		const transactionCache = createTransactionCache();

		TXN.get = function get(args) {
			const {scope, type, id, include} = args;

			// Fetch the object, resetting the cache if there is a cache miss
			// on the get.
			return getObject(scope, key, {resetCache: true}).then((obj) => {
				// Return null if not found.
				if (!obj) return null;

				// Set the item in the transaction cache in case it's used again during
				// the transaction.
				transactionCache.set(obj);

				// Handle an includes query.
				if (obj.relationships && include && include.length > 0) {

					// Aggregate the object keys we'll need to fetch.
					const keys = include.reduce((keys, rname) => {
						return keys.concat(obj.relationships[rname] || []);
					}, []);

					// If there aren't any keys to fetch, just fall through to the bottom.
					// return statement.
					if (keys.length > 0) {

						// Fetch the objects in batch mode, resetting the cache if there is
						// a cache miss on the get.
						return batchGetObjects(scope, keys, {resetCache: true}).then((items) => {
							items = items.filter((item, i) => {
								if (!item) {
									const key = keys[i];
									warn(new InvariantError(
										`Rynodb corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
									));
								}


								// Set each item in the transaction cache in case it's used
								// again during the transaction.
								transactionCache.set(item);
								return Boolean(item);
							});

							return {
								resource: createReturnObject(obj),
								included: items.map(createReturnObject)
							};
						});
					}
				}

				// No includes query? Just return the resource object.
				return {resource: createReturnObject(obj), included: []};
			}).catch((err) => {
				transactionErrors.push(new VError(err, `Error in Rynodb Transaction#get()`));
				return null;
			});
		};

		TXN.set = function set(scope, obj) {
			const {type, id} = obj;

			// Check the transaction cache for this object so we can get the delta
			// for relationship and index updates.
			const oldObject = transactionCache.get(scope, type, id);

			return setObject(scope, obj)
				.then((newObject) => {
					return Promise.all([
						// Add the object to the type index so it can be retrieved
						// using a type scan operation.
						typeIndexObject(scope, newObject),

						// When an object is updated, we need to determine the foreignKeys
						// added/removed from the relationships Hash, find each related
						// object, and update its foreignKeys Set.
						updateForeignKeys(scope, oldObject, newObject),

						// When an object is created or updated it's index entries may have
						// changed. We need to determine which entries need to be removed
						// from indexing, and which need to be added.
						indexObject(scope, newObject)
					]).then(() => createReturnObject(newObject));
				})
				.catch((err) => {
					transactionErrors.push(new VError(err, `Error in Rynodb Transaction#set()`));
					return obj;
				});
		};

		TXN.remove = function remove(args) {
			const {scope, type, id} = args;
			const key = {type, id};

			getObject(scope, key, {skipCache: true})
				.then((obj) => {
					if (!obj) return null;

					return Promise.all([
						removeForeignKeyRelationships(scope, obj),
						removeFromQueryTable(),
						removeFromTypeIndex(scope, type, id)
					]);
				})
				.then(() => {
					return removeObject(scope, type, id);
				})
				.then(() => {
					return true;
				})
				.catch((err) => {
					transactionErrors.push(new VError(err, `Error in Rynodb Transaction#remove()`));
					return null;
				});
		};

		TXN.scan = function scan(args) {
			const {scope, type, cursor, limit};

			const redisIndexKey = composeTypeScanIndexKey(scope, type);
			const inclusiveStart = cursor ? cursor.start : 0;
			const inclusiveStop = inclusiveStart + limit - 1;

			return Promise.resolve(null)
				.then(() => {
					return redisZRANGE(redisIndexKey, inclusiveStart, inclusiveStop);
				})
				.then((ids) => {
					const keys = ids.map((id) => {
						return {type, id};
					});

					return batchGetObjects(scope, keys, {resetCache: true}).then((items) => {
						items = items.filter((item, i) => {
							if (!item) {
								const {type, id} = keys[i];
								warn(new InvariantError(
									`Rynodb corrupt data: Missing resource ${type}.${id} but referenced in type scan index "${redisIndexKey}"`
								));
							}

							return Boolean(item);
						});

						return {
							items: items.map(createReturnObject),
							cursor: {start: inclusiveStop + 1}
						};
					});
				})
				.catch((err) => {
					transactionErrors.push(new VError(err, `Error in Rynodb Transaction#scan()`));
					return null;
				});
		};

		TXN.query = function query(args) {
			throw new Error(`Transaction#query() is not yet implemented`);
			// const {
			// 	scope,
			// 	type,
			// 	indexName,
			// 	operator,
			// 	parameters,
			// 	cursor,
			// 	limit
			// } = args;

			// Document:
			// {
			//  type: `${type}`,
			//  id: `${id}`,
			// 	partitionKey: `${scope}:${type}:${id}`,
			// 	rangeKey: `${indexName}:${indexKey}`,
			// 	indexPartitionKey: `${scope}:${indexName}`,
			// 	indexKey: `${indexKey}`
			// }
			//
			// Table:
			// HashKey: partitionKey, RangeKey: rangeKey
			//
			// Index:
			// HashKey: indexPartitionKey, RangeKey: indexKey

			// let KeyConditionExpression = `indexPartitionKey = :pkey `;
			// switch (operator) {
			// 	case QUERY_OPERATOR_EQUALS:
			// 		KeyConditionExpression += `indexKey = :ikey`;
			// 	case QUERY_OPERATOR_BEGINS_WITH:
			// 		KeyConditionExpression += `begins_with (indexKey, :ikey)`;
			// 	default:
			// 		return Promise.reject(new Error(
			// 			`Rynodb Transaction#query() operator "${operator}" is not yet implemented.`
			// 		));
			// }

			// return Promise.resolve(null)
			// 	.then(() => {
			// 		return dynamodbQuery({
			// 			TableName: DYNAMODB_QUERY_TABLE_NAME,
			// 			IndexName: DYNAMODB_QUERY_INDEX_NAME,
			// 			ExpressionAttributeValues: {
			// 				':pkey': marshalDDBValue(composeQueryIndexPartitionKey(scope, indexName)),
			// 				':ikey': marshalDDBValue(parameters[0])
			// 			},
			// 			KeyConditionExpression,
			// 			ExclusiveStartKey: cursor,
			// 			Limit: limit
			// 		});
			// 	})
			// 	.then((res) => {
			// 		const {Items, LastEvaluatedKey} = res;

			// 		const keys = Items.map((doc) => {
			// 			const {type, id} = unmarshalDDBDocument(doc);
			// 			return {type, id};
			// 		});

			// 		return batchGetObjects(scope, keys).then((items) => {
			// 			return {
			// 				items: items.map(createReturnObject),
			// 				cursor: LastEvaluatedKey
			// 			};
			// 		});
			// 	})
			// 	.then((res) => {
			// 		res.items = res.items.filter((item) => {
			// 			if (!item) {
			// 				warn(new InvariantError(
			// 					`Rynodb corrupt data: Missing resource ${type}.${id} but referenced in index "${indexName}"`
			// 				));
			// 			}

			// 			return Boolean(item);
			// 		});

			// 		return res;
			// 	})
			// 	.catch((err) => {
			// 		transactionErrors.push(new VError(err, `Error in Rynodb Transaction#query()`));
			// 		return null;
			// 	});
		};

		TXN.commit = function commit() {
			if (transactionErrors.length > 0) {
				// Make a copy of transactionErrors using .slice() to avoid mutation of
				// the private Array by the caller.
				return Promise.reject(transactionErrors.slice());
			}

			return Promise.resolve(true);
		};

		TXN.rollback = function rollback() {
			return Promise.resolve(true);
		};

		return TXN;
	}

	function createTransactionCache() {
		const cache = Object.create(null);

		return {
			get(args) {
				const {scope, type, id} = args;
				const key = `${scope}:${type}:${id}`;
				return cache[key] ? clone(cache[key]) : null;
			},
			set(obj) {
				const {scope, type, id} = obj;
				const key = `${scope}:${type}:${id}`;
				cache[key] = clone(obj);
				return obj;
			}
		};
	}

	function createDBObject(spec) {
		spec = clone(spec);

		return Object.defineProperties(Object.create(null), {
			type: {
				enumerable: true,
				value: spec.type
			},
			id: {
				enumerable: true,
				value: spec.id
			},
			created: {
				enumerable: true,
				value: spec.created || new Date().toISOString()
			},
			updated: {
				enumerable: true,
				value: spec.updated || new Date().toISOString()
			},
			indexEntries: {
				enumerable: true,
				value: spec.indexEntries || []
			},
			foreignKeys: {
				enumerable: true,
				value: spec.foreignKeys || []
			},
			attributes: {
				enumerable: true,
				value: spec.attributes || Object.create(null)
			},
			relationships: {
				enumerable: true,
				value: spec.relationships || Object.create(null)
			}
		});
	}

	function createIndexEntryDocument(spec) {
		// Document:
		// {
		//  type: `${type}`,
		//  id: `${id}`,
		// 	partitionKey: `${scope}:${type}:${id}`,
		// 	rangeKey: `${indexName}:${indexKey}`,
		// 	indexPartitionKey: `${scope}:${indexName}`,
		// 	indexKey: `${indexKey}`
		// }
		//
		// Table:
		// HashKey: partitionKey, RangeKey: rangeKey
		//
		// Index:
		// HashKey: indexPartitionKey, RangeKey: indexKey

		return Object.defineProperties(Object.create(null), {
			type: {
				enumerable: true,
				value: spec.type
			},
			id: {
				enumerable: true,
				value: spec.id
			},
			partitionKey: {
				enumerable: true,
				value: composeQueryTablePartitionKey(spec.scope, spec.type, spec.id)
			},
			rangeKey: {
				enumerable: true,
				value: composeQueryTableRangeKey(spec.indexName, spec.indexKey)
			},
			indexPartitionKey: {
				enumerable: true,
				value: composeQueryIndexPartitionKey(spec.scope, spec.indexName)
			},
			indexKey: {
				enumerable: true,
				value: spec.indexKey
			}
		});
	}

	function typeIndexObject(scope, obj) {
		const {type, id} = obj;
		const indexKey = composeTypeScanIndexKey(scope, type);
		return redisZADD(indexKey, 0, id);
	}

	// When an object is updated, we need to determine the foreignKeys added/removed from
	// the relationships Hash, find each related object, and update its foreignKeys
	// Set.
	function updateForeignKeys(scope, oldObject, newObject) {
		const a = oldObject.relationships ? unnest(Object.keys(oldObject.relationships).map((rname) => {
			return oldObject.relationships[rname];
		})) : [];

		const b = newObject.relationships ? unnest(Object.keys(newObject.relationships).map((rname) => {
			return newObject.relationships[rname];
		})) : [];

		const toRemove = differenceByKey(a, b);
		const newlyAdded = differenceByKey(b, a);

		return Promise.all([
			removeForeignKeys(scope, type, id, toRemove),
			addForeignKeys(scope, type, id, newlyAdded)
		]);
	}

	function removeForeignKeys(scope, type, id, keys) {
		return batchGetObjects(scope, keys, {skipCache: true})
			.then((objects) => {
				return objects.filter((obj, i) => {
					if (!obj) {
						const key = keys[i];
						warn(new InvariantError(
							`Rynodb corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
						));
					}

					return Boolean(item);
				});
			})
			.then((objects) => {
				const mutatedObjects = objects.map((obj) => {
					// Clone before mutating.
					obj = clone(obj);
					obj.foreignKeys = obj.foreignKeys.filter(keysEqual({type, id}));
					return obj;
				});

				return batchSetObjects(scope, mutatedObjects);
			});
	}

	function addForeignKeys(scope, type, id, keys) {
		return batchGetObjects(scope, keys, {skipCache: true})
			.then((objects) => {
				return objects.filter((obj, i) => {
					if (!obj) {
						const key = keys[i];
						warn(new InvariantError(
							`Rynodb corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${type}.${id} relationships`
						));
					}

					return Boolean(item);
				});
			})
			.then((objects) => {
				const mutatedObjects = objects.map((obj) => {
					// Clone before mutating.
					obj = clone(obj);
					obj.foreignKeys = uniqueByKey(append({type, id}, obj.foreignKeys));
					return obj;
				});

				return batchSetObjects(scope, mutatedObjects);
			});
	}

	// When an object is created or updated it's index entries may have changed.
	// We need to determine which entries need to be removed from indexing, and
	// which need to be added.
	function indexObject(scope, obj) {
		const {type, id} = obj;
		const indexEntries = obj.indexEntries ? obj.indexEntries : [];

		const newEntries = unnest(indexEntries.map((entry) => {
			const {indexName, keys} = entry;
			return keys.map((indexKey) => {
				return createIndexEntryDocument({
					scope,
					type,
					id,
					indexName,
					indexKey
				});
			});
		}));

		const differenceByIndex = differenceWith((a, b) => {
			return a.indexName === b.indexName && a.indexKey === b.indexKey;
		});

		return getIndexEntries(scope, type, id).then((currentEntries) => {
			const toRemove = differenceByIndex(currentEntries, newEntries);
			const newlyAdded = differenceByIndex(newEntries, currentEntries);

			return Promise.all([
				removeIndexEntries(toRemove),
				addIndexEntries(newlyAdded)
			]);
		});
	}

	function getIndexEntries(scope, type, id) {
		// Document:
		// {
		//  type: `${type}`,
		//  id: `${id}`,
		// 	partitionKey: `${scope}:${type}:${id}`,
		// 	rangeKey: `${indexName}:${indexKey}`,
		// 	indexPartitionKey: `${scope}:${indexName}`,
		// 	indexKey: `${indexKey}`
		// }
		//
		// Table:
		// HashKey: partitionKey, RangeKey: rangeKey
		//
		// Index:
		// HashKey: indexPartitionKey, RangeKey: indexKey

		const params = {
			TableName: DYNAMODB_QUERY_TABLE_NAME,
			ExpressionAttributeValues: {
				':pkey': marshalDDBValue(composeQueryTablePartitionKey(scope, type, id))
			},
			KeyConditionExpression: `partitionKey = :pkey`
		};

		return dynamodbQuery(params).then((res) => {
			const {Items, LastEvaluatedKey} = res;

			// TODO: Handle the situation if LastEvaluatedKey is present
			if (LastEvaluatedKey) {
				warn(new InvariantError(
					`Received LastEvaluatedKey while running DynamoDB index table query`
				));
			}

			return Items.map(unmarshalDDBDocument);
		});
	}

	function removeIndexEntries(entries) {
		const RequestItems = Object.create(null);
		RequestItems[DYNAMODB_QUERY_TABLE_NAME] = entries.map((entry) => {
			return {
				DeleteRequest: {
					Key: {
						partitionKey: marshalDDBValue(entry.partitionKey),
						rangeKey: marshalDDBValue(entry.rangeKey)
					}
				}
			};
		});

		const params = {RequestItems};

		return dynamodbBatchWriteItem(params);
	}

	function addIndexEntries(entries) {
		const RequestItems = Object.create(null);
		RequestItems[DYNAMODB_QUERY_TABLE_NAME] = entries.map((entry) => {
			return {
				PutRequest: {
					Item: marshalDDBDocument(entry)
				}
			};
		});

		const params = {RequestItems};

		return dynamodbBatchWriteItem(params);
	}

	// When an object is removed, we need to use it's foreignKeys Set to find all
	// other objects which reference it and remove the reference from those
	// objects' relationships Map.
	function removeForeignKeyRelationships(scope, obj) {
		const {type, id, foreignKeys} = obj;

		if (!foreignKeys || foreignKeys.length === 0) {
			return Promise.resolve(true);
		}

		return batchGetObjects(scope, foreignKeys, {skipCache: true})
			.then((objects) => {
				return objects.filter((obj, i) => {
					if (!obj) {
						const key = foreignKeys[i];
						warn(new InvariantError(
							`Rynodb corrupt data: Missing resource ${key.type}.${key.id} but referenced in ${key.type}.${key.id} foreignKeys`
						));
					}

					return Boolean(item);
				});
			})
			.then((objects) => {
				const mutatedObjects = compact(objects.map((obj) => {
					const relationships = obj.relationships;
					if (!relationships) return null;

					// Make a copy before mutating.
					obj = clone(obj);

					obj.relationships = Object.keys(relationships).reduce((newr, rname) => {
						newr[rname] = relationships[rname].filter((key) => {
							return key.type !== type || key.id !== id;
						});
						return newr;
					}, Object.create(null));

					return obj;
				}));

				return batchSetObjects(scope, mutatedItems);
			});
	}

	function removeFromQueryTable(scope, type, id) {
		return getIndexEntries(scope, type, id).then(removeIndexEntries);
	}

	function removeFromTypeIndex(scope, type, id) {
		const indexKey = composeTypeScanIndexKey(scope, type);
		return redisZREM(indexKey, id);
	}

	function batchGetObjects(scope, keys, options = {}) {
		const {skipCache, resetCache} = options;
	}

	function redisZRANGE(key, start, stop) {
		return new Promise((resolve, reject) => {
			redis.zrange(key, start, stop, (err, res) => {
				if (err) {
					return reject(new VError(
						err,
						`Redis ZRANGE query error`
					));
				}
				return resolve(res);
			});
		});
	}

	function redisZREM(key, member) {
		return new Promise((resolve, reject) => {
			redis.zrem(key, member, (err) => {
				if (err) {
					return reject(new VError(
						err,
						`Redis ZREM error`
					));
				}
				return resolve(true);
			});
		});
	}

	function redisZADD(key, index, member) {
		return new Promise((resolve, reject) => {
			redis.zadd(key, index, member, (err) => {
				if (err) {
					return reject(new VError(
						err,
						`Redis ZADD error`
					));
				}
				return resolve(true);
			});
		});
	}

	function dynamodbQuery() {
	}

	function dynamodbBatchWriteItem() {
		// TODO: Handle UnprocessedItems. See:
		//   http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#batchWriteItem-property
	}

	function marshalDDBDocument(doc) {
	}

	function unmarshalDDBDocument(doc) {
	}

	function marshalDDBValue(val) {
	}

	function unmarshalDDBValue(val) {
	}

	function composeQueryTablePartitionKey(scope, type, id) {
		return `${scope}:${type}:${id}`;
	}

	function composeQueryTableRangeKey(indexName, indexKey) {
		return `${indexName}:${indexKey}`;
	}

	function composeQueryIndexPartitionKey(scope, indexName) {
		return `${scope}:${indexName}`;
	}

	function composeTypeScanIndexKey(scope, type) {
		return `${scope}:typescan:${type}`;
	}

	const keysEqual = curry(function (a, b) {
		return a.type === b.type && a.id === b.id;
	});

	const differenceByKey = differenceWith(keysEqual);
	const uniqueByKey = uniqWith(keysEqual);

	return API;
};
