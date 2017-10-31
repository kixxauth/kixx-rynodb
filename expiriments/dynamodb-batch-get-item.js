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

const params = Object.assign({
	RequestItems: {
		odd_store_collection_entities: {
			Keys: [
				{id: {S: `res-ooyala-label-247526c9dd644f8ea25cf728971b9e2b`}},
				{id: {S: `res-ooyala-label-f28af28ab1fb4d26b0e4becaa715c25b`}},
				{id: {S: `res-ooyala-label-a3ffc439be5c4cc4bc904a9bef55b21d`}}
			]
		}
	}
}, args);

dynamodb.batchGetItem(params, (err, res) => {
	if (err) {
		console.log(`Error:`);
		console.log(`Error.name: ${err.name}, Error.code: ${err.code}`);
		console.log(err.stack);
		return;
	}

	console.log(`Result:`);
	console.log(JSON.stringify(res, null, 2));
});

// Result:
// {
//   "Responses": {
//     "odd_store_collection_entities": [
//       {
//         "type": {
//           "S": "collection"
//         },
//         "id": {
//           "S": "res-ooyala-label-f28af28ab1fb4d26b0e4becaa715c25b"
//         },
//         "description": {
//           "S": " "
//         },
//         "tags": {
//           "L": []
//         },
//         "title": {
//           "S": "2017 Races"
//         }
//       }
//     ]
//   },
//   "UnprocessedKeys": {},
//   // ReturnConsumedCapacity: "TOTAL"
//   "ConsumedCapacity": [
//     {
//       "TableName": "odd_store_collection_entities",
//       "CapacityUnits": 1.5
//     }
//   ]
//   // ReturnConsumedCapacity: "INDEXES"
//   "ConsumedCapacity": [
//     {
//       "TableName": "odd_store_collection_entities",
//       "CapacityUnits": 1.5,
//       "Table": {
//         "CapacityUnits": 1.5
//       }
//     }
//   ]
// }
