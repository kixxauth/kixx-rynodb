exports.initialize = () => {

	const API = Object.create(null);

	API.createTransaction = function createTransaction() {
		const TXN = Object.create(null);

		TXN.get = function get(args) {
			const {scope, type, id, include} = args;

			return getObject(scope, key, {resetCache: true})
				.then((obj) => {
					if (!obj) return null;

					if (obj.relationships && include && include.length > 0) {
						const keys = include.reduce((keys, rname) => {
							return keys.concat(obj.relationships[rname] || []);
						}, []);

						if (keys.length > 0) {
							return batchGetObjects(scope, keys, {resetCache: true}).then((items) => {
								items = items.filter((item, i) => {
									if (!item) {
										const {type, id} = keys[i];
										warn(new InvariantError(
											`Rynodb corrupt data: No resource "${type}.${id}" but found in relationships`
										));
									}

									return Boolean(item);
								});

								return {
									resource: createReturnObject(obj),
									included: items.map(createReturnObject)
								};
							});
						}
					}

					return {resource: createReturnObject(obj)}
				})
				.catch((err) => {
					return Promise.reject(new VError(
						err,
						`Error in Rynodb Transaction#get()`
					));
				});
		};

		TXN.set = function set() {
		};

		TXN.remove = function remove(args) {
			const {scope, type, id} = args;
			const key = {type, id};

			getObject(scope, key, {skipCache: true})
				.then((obj) => {
					if (!obj) return null;

					return Promise.all([
						removeForeignKeyRelationships(scope, obj),
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
					return Promise.reject(new VError(
						err,
						`Error in Rynodb Transaction#remove()`
					));
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
									`Rynodb corrupt data: No resource "${type}.${id}" but found in type scan`
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
					return Promise.reject(new VError(
						err,
						`Error in Rynodb Transaction#scan()`
					));
				});
		};

		TXN.query = function query(args) {
			// throw new Error(`Transaction#query() is not yet implemented`);
			const {
				scope,
				type,
				indexName,
				operator,
				parameters,
				cursor,
				limit
			} = args;

			// Index:
			// HashKey: `${scope}:${indexName}` RangeKey: `${key}`
			//
			// Table:
			// HashKey: `${scope}:${indexName}:${type}:${id}` RangeKey: `${key}`

			let KeyConditionExpression = `partitionKey = :pkey `;
			switch (operator) {
				case QUERY_OPERATOR_EQUALS:
					KeyConditionExpression += `indexKey = :ikey`;
				case QUERY_OPERATOR_BEGINS_WITH:
					KeyConditionExpression += `begins_with (indexKey, :ikey)`;
				default:
					return Promise.reject(new Error(
						`Rynodb Transaction#query() operator "${operator}" is not yet implemented.`
					));
			}

			return Promise.resolve(null)
				.then(() => {
					return dynamodbQuery({
						TableName: DYNAMODB_QUERY_TABLE_NAME,
						IndexName: DYNAMODB_QUERY_INDEX_NAME,
						ExpressionAttributeValues: {
							':pkey': marshalDDBValue(`${scope}:${indexName}`),
							':ikey': marshalDDBValue(parameters[0])
						},
						KeyConditionExpression,
						ExclusiveStartKey: cursor,
						Limit: limit
					});
				})
				.then((res) => {
					const {Items, LastEvaluatedKey} = res;

					const keys = Items.map((doc) => {
						const {type, id} = unmarshalDDBDocument(doc);
						return {type, id};
					});

					return batchGetObjects(scope, keys, {resetCache: false}).then((items) => {
						return {
							items: items.map(createReturnObject),
							cursor: LastEvaluatedKey
						};
					});
				})
				.then((res) => {
					res.items = res.items.filter((item) => {
						if (!item) {
							warn(new InvariantError(
								`Rynodb corrupt data: No resource "${type}.${id}" but found in index "${indexName}"`
							));
						}

						return Boolean(item);
					});

					return res;
				})
				.catch((err) => {
					return Promise.reject(new VError(
						err,
						`Error in Rynodb Transaction#query()`
					));
				});
		};

		TXN.commit = function commit() {
		};

		TXN.rollback = function rollback() {
		};

		return TXN;
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

	function batchGetObjects(scope, keys, options = {}) {
		const {resetCache} = options;
	}

	function removeForeignKeyRelationships(scope, obj) {
		const keys = obj.foreignKeys;

		if (!keys || keys.length === 0) {
			return Promise.resolve(true);
		}

		return batchGetObjects(scope, keys, {skipCache: true})
			.then((items) => {
				return items.filter((item, i) => {
					if (!item) {
						const {type, id} = keys[i];
						warn(new InvariantError(
							`Rynodb corrupt data: No resource "${type}.${id}" but found in foreignKeys`
						));
					}

					return Boolean(item);
				});
			})
			.then((items) => {
				const mutatedItems = compact(items.map((item) => {
					const relationships = item.relationships;
					if (!relationships) return null;

					item.relationships = Object.keys(relationships).reduce((newr, rname) => {
						newr[rname] = relationships[rname].filter((key) => {
							return key.type !== type || key.id !== id;
						});
						return newr;
					}, Object.create(null));

					return item;
				}));

				return batchSetObjects(scope, mutatedItems);
			});
	}

	function removeFromQueryTable() {
		// Index:
		// HashKey: `${scope}:${indexName}` RangeKey: `${key}`
		//
		// Table:
		// HashKey: `${scope}:${indexName}:${type}:${id}` RangeKey: `${key}`

		const params = {
			TableName: DYNAMODB_QUERY_TABLE_NAME,
			ExpressionAttributeValues: {
				':pkey': marshalDDBValue(`${scope}:${indexName}:${type}:${id}`)
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

			const keys = Items.map((doc) => {
				return unmarshalDDBDocument(doc).indexKey;
			});

			// var params = {
			//   Key: {
			//    "Artist": {
			//      S: "No One You Know"
			//     },
			//    "SongTitle": {
			//      S: "Scared of My Shadow"
			//     }
			//   },
			//   TableName: "Music"
			// };
		});
	}

	function removeFromTypeIndex(scope, type, id) {
		const indexKey = composeTypeScanIndexKey(scope, type);
		return redisZREM(indexKey, id);
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

	function marshalDDBDocument(doc) {
	}

	function unmarshalDDBDocument(doc) {
	}

	function marshalDDBValue(val) {
	}

	function unmarshalDDBValue(val) {
	}

	function composeTypeScanIndexKey(scope, type) {
		return `${scope}:typescan:${type}`;
	}

	return API;
};
