'use strict';

const {StackedError} = require(`kixx`);

exports.reportFullStackTrace = function reportFullStackTrace(done) {
	return function (err) {
		console.error(`Error detected. Full stack trace:`); // eslint-disable-line no-console
		console.error(StackedError.getFullStack(err)); // eslint-disable-line no-console
		done(err);
	};
};
