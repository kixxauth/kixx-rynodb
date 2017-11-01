'use strict';

class DynamoDB {
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
