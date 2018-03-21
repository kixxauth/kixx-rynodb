'use strict';

const {helpers} = require('kixx-assert');

const protoToString = Object.prototype.toString;

function clone(obj) {
	const type = typeof obj;

	if (obj === null || type !== 'object') return obj;

	if (Array.isArray(obj)) return obj.map(clone);

	if (type === 'object') {
		if (protoToString.call(obj) === '[object Date]') {
			return new Date(obj.toString());
		}
		return Object.getOwnPropertyNames(obj).reduce((newObj, key) => {
			newObj[key] = clone(obj[key]);
			return newObj;
		}, {});
	}
}
exports.clone = clone;

function omitKeys(keys) {
	return function (obj) {
		return Object.keys(obj).reduce(function (target, key) {
			if (!keys.includes(key)) target[key] = obj[key];
			return target;
		}, {});
	};
}
exports.omitKeys = omitKeys;

const assertIsObject = helpers.assertion1(helpers.isObject, function (actual) {
	return helpers.printf('expected %x to be a plain Object', actual);
});
exports.assertIsObject = assertIsObject;

function compact(list) {
	return list.filter((item) => Boolean(item));
}
exports.compact = compact;

function partition(size) {
	return function (list) {
		const chunks = [];
		let currentChunkIndex = 0;

		for (let i = 0; i < list.length; i++) {
			let currentChunk = chunks[currentChunkIndex];
			if (!currentChunk) {
				currentChunk = [];
				chunks[currentChunkIndex] = currentChunk;
			}
			currentChunk.push(list[i]);
			if (currentChunk.length === size) currentChunkIndex += 1;
		}

		return chunks;
	};
}
exports.partition = partition;
