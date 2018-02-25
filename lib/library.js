'use strict';

const {isArray, curry, printf, assertion1} = require(`kixx/library`);

const assertIsArray = assertion1(isArray, (actual) => {
	return printf(`expected %x to be an Array`, actual);
});
exports.assertIsArray = assertIsArray;

const hasKey = curry((a, b) => {
	return a.id === b.id && a.type === b.type;
});
exports.hasKey = hasKey;
