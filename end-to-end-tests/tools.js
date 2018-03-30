/* eslint-disable no-process-env */
'use strict';

const {isNonEmptyString} = require('kixx/library');
const debug = require('debug');

exports.TABLE_PREFIX = 'ttt';

exports.debug = function (name) {
	return debug(`end-to-end:${name}`);
};

exports.getAwsCredentials = () => {
	const awsAccessKey = process.env.AWS_ACCESS_KEY_ID;
	const awsSecretKey = process.env.AWS_SECRET_ACCESS_KEY;
	const awsRegion = process.env.AWS_REGION;

	if (!isNonEmptyString(awsAccessKey)) {
		throw new Error('process.env.AWS_ACCESS_KEY_ID is required');
	}
	if (!isNonEmptyString(awsSecretKey)) {
		throw new Error('process.env.AWS_SECRET_ACCESS_KEY is required');
	}
	if (!isNonEmptyString(awsRegion)) {
		throw new Error('process.env.AWS_REGION is required');
	}

	return {
		awsAccessKey,
		awsSecretKey,
		awsRegion
	};
};
