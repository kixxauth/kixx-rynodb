'use strict';

class DynamoDB {
	batchGetItem(params, callback) {
		const TableName = Object.keys(params.RequestItems)[0];
		const Responses = {};

		Responses[TableName] = params.RequestItems[TableName].Keys.map((key) => {
			key.type = {S: key.scope_type_key.S.split(`:`)[1]};
			key.attributes = {M: {title: {S: `Foo Bar`}}};
			return key;
		});

		let res = {
			Responses,
			UnprocessedItems: {},
			foo: `bar`
		};

		res = DynamoDB.setConsumedCapacity(params, res);

		process.nextTick(() => {
			callback(null, res);
		});
	}

	batchWriteItem(params, callback) {
		let res = {
			UnprocessedItems: {},
			foo: `bar`
		};

		res = DynamoDB.setConsumedCapacity(params, res);

		process.nextTick(() => {
			callback(null, res);
		});
	}

	getItem(params, callback) {
		let res = {
			Item: JSON.parse(JSON.stringify(params.Key)),
			foo: `bar`
		};

		res.Item.type = {S: params.Key.scope_type_key.S.split(`:`)[1]};

		res.Item.attributes = {M: {title: {S: `Foo Bar`}}};

		res = DynamoDB.setConsumedCapacity(params, res);

		process.nextTick(() => {
			callback(null, res);
		});
	}

	putItem(params, callback) {
		let res = {
			Attributes: `XXX`,
			foo: `bar`
		};

		res = DynamoDB.setConsumedCapacity(params, res);

		process.nextTick(() => {
			callback(null, res);
		});
	}

	static setConsumedCapacity(params, res) {
		if (params.ReturnConsumedCapacity === `TOTAL`) {
			res.ConsumedCapacity = {
				TableName: params.TableName,
				CapacityUnits: 1.5
			};
		}

		if (params.ReturnConsumedCapacity === `INDEXES`) {
			if (params.IndexName) {
				const GlobalSecondaryIndexes = {};
				GlobalSecondaryIndexes[params.IndexName] = {CapacityUnits: 0.5};
				res.ConsumedCapacity = {
					TableName: params.TableName,
					CapacityUnits: 0.5,
					Table: {
						CapacityUnits: 0
					},
					GlobalSecondaryIndexes
				};
			} else {
				res.ConsumedCapacity = {
					TableName: params.TableName,
					CapacityUnits: 1.5,
					Table: {
						CapacityUnits: 1.5
					}
				};
			}
		}

		return res;
	}
}

module.exports = DynamoDB;
