'use strict';

const Promise = require(`bluebird`);
const {ProgrammerError} = require(`kixx`);
const {isObject, isNonEmptyString} = require(`kixx/library`);
const ddb = require(`./dynamodb`);

module.exports = function setupSchema(options) {
	if (!isObject(options)) {
		throw new ProgrammerError(
			`setupSchema() options must be a plain Object`
		);
	}

	const dynamodb = options.dynamodb;
	const dynamodbTablePrefix = options.dynamodbTablePrefix;

	if (!isObject(dynamodb)) {
		throw new ProgrammerError(
			`setupSchema() options must include a valid AWS DynamoDB instance as .dynamodb`
		);
	}
	if (!isNonEmptyString(dynamodbTablePrefix) || !/^[a-z_]+$/.test(dynamodbTablePrefix)) {
		throw new ProgrammerError(
			`setupSchema() options must include a valid options.dynamodbTablePrefix String`
		);
	}

	function createTable() {
		return ddb.createTable(dynamodb, {prefix: dynamodbTablePrefix}, {});
	}

	return ddb.describeTable(dynamodb, {prefix: dynamodbTablePrefix}).then((res) => {
		if (res.TableStatus !== `ACTIVE`) {
			return createTable();
		}
		return res;
	}, (err) => {
		if (err.code === `ResourceNotFoundException`) {
			return createTable();
		}
		return Promise.reject(err);
	});
};