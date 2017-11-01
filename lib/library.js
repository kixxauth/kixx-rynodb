'use strict';

const {isArray, printf, assertion1} = require(`kixx/library`);

exports.assertIsArray = assertion1(isArray, (actual) => {
	return printf(`expected %x to be an Array`, actual);
});
