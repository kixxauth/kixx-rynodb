'use strict';

const {isArray, curry, printf, assertion1} = require(`kixx/library`);

exports.assertIsArray = assertion1(isArray, (actual) => {
	return printf(`expected %x to be an Array`, actual);
});

exports.hasKey = curry(function hasKey(a, b) {
	return a.id === b.id && a.type === b.type;
});
