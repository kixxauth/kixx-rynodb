'use strict';

const {assert} = require('kixx/library');

module.exports = function (t) {
	t.describe('nominal case', (t) => {
		t.before(function (done) {
			done();
		});

		t.it('is not smoking', function () {
			assert.isOk(false, 'is smoking');
		});
	});
};
