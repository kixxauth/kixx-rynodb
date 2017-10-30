const {curry} = require(`ramda`);
const Promise = require(`bluebird`);
const Kixx = require(`kixx`);

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
			resolve(JSON.parse(value));
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
			resolve(JSON.parse(JSON.stringify(objects)))
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
				return reject(new StackedError(`Error in redisGet()`, err, redisSet));
			}
			resolve(JSON.parse(resString));
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
			resolve(JSON.parse(`[${items.join(`,`)}]`))
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
				return reject(new StackedError(`Error in redisRemove()`, err, redisSet));
			}
			resolve(true);
		});
	});
});

function composeRedisObjectKey(prefix, scope, type, id) {
	return `${prefix}:${scope}:${type}:${id}`;
}
