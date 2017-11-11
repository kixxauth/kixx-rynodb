'use strict';

const Promise = require(`bluebird`);
const Kixx = require(`kixx`);
const {curry} = require(`kixx/lib`);

const {StackedError} = Kixx;

exports.set = curry(function redisSet(connection, options, scope, object) {
	const {timeToLive, prefix} = options;
	const {type, id} = object;
	const key = composeRedisObjectKey(prefix, scope, type, id);
	const value = JSON.stringify(object);

	return new Promise((resolve, reject) => {
		connection.set(key, value, `EX`, timeToLive, (err) => {
			if (err) {
				return reject(new StackedError(`Error in redisSet()`, err, redisSet));
			}
			resolve({
				data: JSON.parse(value),
				cursor: null,
				meta: {}
			});
		});
	});
});

exports.batchSet = curry(function redisBatchSet(connection, options, scope, objects) {
	const {timeToLive, prefix} = options;

	const commands = objects.map((object) => {
		const {type, id} = object;
		const key = composeRedisObjectKey(prefix, scope, type, id);
		const value = JSON.stringify(object);
		return [`SET`, key, value, `EX`, timeToLive];
	});

	return new Promise((resolve, reject) => {
		connection.multi(commands).exec((err) => {
			if (err) {
				return reject(new StackedError(`Error in redisBatchSet()`, err, redisBatchSet));
			}
			resolve({
				data: JSON.parse(JSON.stringify(objects)),
				cursor: null,
				meta: {}
			});
		});
	});
});

exports.get = curry(function redisGet(connection, options, scope, key) {
	const {prefix} = options;
	const {type, id} = key;
	key = composeRedisObjectKey(prefix, scope, type, id);

	return new Promise((resolve, reject) => {
		connection.get(key, (err, resString) => {
			if (err) {
				return reject(new StackedError(`Error in redisGet()`, err, redisGet));
			}
			resolve({
				data: JSON.parse(resString),
				cursor: null,
				meta: {}
			});
		});
	});
});

exports.batchGet = curry(function redisBatchGet(connection, options, scope, keys) {
	const {prefix} = options;

	const commands = keys.map((key) => {
		const {type, id} = key;
		key = composeRedisObjectKey(prefix, scope, type, id);
		return [`GET`, key];
	});

	return new Promise((resolve, reject) => {
		connection.multi(commands).exec((err, res) => {
			if (err) {
				return reject(new StackedError(`Error in redisBatchGet()`, err, redisBatchGet));
			}
			const items = res.map((x) => x ? x : `null`);
			resolve({
				data: JSON.parse(`[${items.join(`,`)}]`),
				cursor: null,
				meta: {}
			});
		});
	});
});

exports.remove = curry(function redisRemove(connection, options, scope, key) {
	const {prefix} = options;
	const {type, id} = key;
	key = composeRedisObjectKey(prefix, scope, type, id);

	return new Promise((resolve, reject) => {
		connection.get(key, (err) => {
			if (err) {
				return reject(new StackedError(`Error in redisRemove()`, err, redisRemove));
			}
			resolve({
				data: true,
				cursor: null,
				meta: {}
			});
		});
	});
});

function composeRedisObjectKey(prefix, scope, type, id) {
	return `${prefix}:${scope}:${type}:${id}`;
}
