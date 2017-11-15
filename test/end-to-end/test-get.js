'use strict';

const {assert} = require(`kixx/library`);

module.exports = function (t, params) {
	t.describe(`get`, (t) => {
		t.it(`is not smoking`, () => {
			assert.isOk(true, `smoking`);
		});
	});
};
