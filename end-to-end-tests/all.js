/* eslint-disable no-console */
'use strict';

const Promise = require('bluebird');

const tests = [
	require('./missing-dynamodb-credentials').main,
	require('./invalid-dynamodb-credentials').main,
	require('./check-tables').main,
	require('./missing-dynamodb-table').main,
	require('./setup-schema').main,
	require('./attempt-throttled-requests').main,
	require('./batch-requests-with-too-many-keys').main
];

const promise = tests.reduce((promise, test) => {
	return promise.then(() => test());
}, Promise.resolve(null));

promise.then(() => {
	console.log('Done :-)');
	return null;
}).catch((err) => {
	console.error('Runtime Error:');
	console.error(err.stack);
	return null;
});
