/* eslint-disable no-process-env, no-console */
'use strict';

const AWS = require(`aws-sdk`);

const ENDPOINT = process.env.ENDPOINT;
const AWS_REGION = process.env.AWS_REGION;
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;

const ARGS = process.argv[2];

const CONFIG = {
	apiVersion: `2012-08-10`,
	region: AWS_REGION,
	accessKeyId: AWS_ACCESS_KEY_ID,
	secretAccessKey: AWS_SECRET_ACCESS_KEY
};

if (ENDPOINT) {
	CONFIG.endpoint = ENDPOINT;
}

const dynamodb = new AWS.DynamoDB(CONFIG);

const args = JSON.parse(ARGS);

const params = {
	TableName: args.TableName,
	IndexName: args.IndexName,
	ExpressionAttributeValues: {
		':p1': {S: args.channel},
		':v1': {S: args.q}
	},
	KeyConditionExpression: `channel = :p1 AND begins_with (title, :v1)`,
	Limit: 5,
	ExclusiveStartKey: null,
	ReturnConsumedCapacity: `TOTAL`
};

dynamodb.query(params, (err, res) => {
	if (err) {
		console.log(`Error:`);
		console.log(`Error.name: ${err.name}, Error.code: ${err.code}`);
		console.log(err.stack);
		return;
	}

	console.log(`Result:`);
	console.log(JSON.stringify(res, null, 2));

	// Empty result:
	// {
	//   "Items": [],
	//   "Count": 0,
	//   "ScannedCount": 0
	// }

	// Results:
	// {
	//   "Items": [
	//     {
	//       "type": {
	//         "S": "collection"
	//       },
	//       "title": {
	//         "S": "Pioneer Panthers"
	//       }
	//     }
	//   ],
	//   "Count": 5,
	//   "ScannedCount": 5,
	//   "LastEvaluatedKey": {
	//     "id": {
	//       "S": "1be28184c2164ee991ca772d89332093"
	//     },
	//     "channel": {
	//       "S": "isc"
	//     },
	//     "title": {
	//       "S": "Pioneer Panthers"
	//     }
	//   },
	//   // ReturnConsumedCapacity: "TOTAL"
	//   "ConsumedCapacity": {
	//     "TableName": "odd_store_collection_entities",
	//     "CapacityUnits": 0.5
	//   }
	//   // ReturnConsumedCapacity: "INDEXES"
	//   "ConsumedCapacity": {
	//     "TableName": "odd_store_collection_entities",
	//     "CapacityUnits": 0.5,
	//     "Table": {
	//       "CapacityUnits": 0
	//     },
	//     "GlobalSecondaryIndexes": {
	//       "odd_store_collection_by_channel": {
	//         "CapacityUnits": 0.5
	//       }
	//     }
	//   }
	// }
});

// Error.name: ValidationException, Error.code: ValidationException
// ValidationException: ExpressionAttributeValues contains invalid value: Supplied AttributeValue is empty, must contain exactly one of the supported datatypes for key :v1

// Error.name: ValidationException, Error.code: ValidationException
// ValidationException: The table does not have the specified index: odd_store_collection_by_channel
