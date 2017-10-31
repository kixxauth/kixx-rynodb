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
	Key: {
		id: {
			S: args.id
		}
	},
	ReturnConsumedCapacity: `INDEXES`
};

dynamodb.getItem(params, (err, res) => {
	if (err) {
		console.log(`Error:`);
		console.log(`Error.name: ${err.name}, Error.code: ${err.code}`);
		console.log(err.stack);
		return;
	}

	console.log(`Result:`);
	console.log(JSON.stringify(res, null, 2));

	// When item not found: res is an empty Object.

	// res:
	// {
	//   "Item": {
	//     "relationships": {
	//       "M": {
	//         "channel": {
	//           "M": {
	//             "id": {
	//               "S": "abc-123-188e"
	//             },
	//             "type": {
	//               "S": "channel"
	//             }
	//           }
	//         }
	//       }
	//     },
	//     "source": {
	//       "S": "vimeo-album"
	//     },
	//     "specTitleIndex": {
	//       "S": "van"
	//     },
	//     "id": {
	//       "S": "abc-123-188e"
	//     },
	//     "specType": {
	//       "S": "collectionSpec"
	//     },
	//     "specTitle": {
	//       "S": "Van"
	//     },
	//     "type": {
	//       "S": "contentSpec"
	//     }
	//   },
	//   // ReturnConsumedCapacity: "TOTAL"
	//   "ConsumedCapacity": {
	//     "TableName": "odd_store_collection_entities",
	//     "CapacityUnits": 0.5
	//   },
	//   // ReturnConsumedCapacity: "INDEXES"
	//   "ConsumedCapacity": {
	//     "TableName": "odd_store_collection_entities",
	//     "CapacityUnits": 0.5,
	//     "Table": {
	//       "CapacityUnits": 0.5
	//     }
	//   }
	// }
});

//
// Error.name: ConfigError, Error.code: ConfigError
// ConfigError: Missing region in config
//

// Got both of these for the same error:
//
// Error.name: CredentialsError, Error.code: CredentialsError
// Error: connect EHOSTUNREACH 169.254.169.254:80 - Local (192.168.0.90:55065)
//
// Error.name: CredentialsError, Error.code: CredentialsError
// CredentialsError: Missing credentials in config

// Missing a Key attribute:
//
// Error.name: ValidationException, Error.code: ValidationException
// ValidationException: Supplied AttributeValue is empty, must contain exactly one of the supported datatypes

// Table does not exist:
//
// Error.name: ResourceNotFoundException, Error.code: ResourceNotFoundException
// ResourceNotFoundException: Requested resource not found
