```js
var params = {
  RequestItems: { /* required */
    '<TableName>': {
      Keys: [ /* required */
        {
          '<AttributeName>': { /* AttributeValue */
            B: new Buffer('...') || 'STRING_VALUE' /* Strings will be Base-64 encoded on your behalf */,
            BOOL: true || false,
            BS: [
              new Buffer('...') || 'STRING_VALUE' /* Strings will be Base-64 encoded on your behalf */,
              /* more items */
            ],
            L: [
              /* recursive AttributeValue */,
              /* more items */
            ],
            M: {
              '<AttributeName>': /* recursive AttributeValue */,
              /* '<AttributeName>': ... */
            },
            N: 'STRING_VALUE',
            NS: [
              'STRING_VALUE',
              /* more items */
            ],
            NULL: true || false,
            S: 'STRING_VALUE',
            SS: [
              'STRING_VALUE',
              /* more items */
            ]
          },
          /* '<AttributeName>': ... */
        },
        /* more items */
      ],
      AttributesToGet: [
        'STRING_VALUE',
        /* more items */
      ],
      ConsistentRead: true || false,
      ExpressionAttributeNames: {
        '<ExpressionAttributeNameVariable>': 'STRING_VALUE',
        /* '<ExpressionAttributeNameVariable>': ... */
      },
      ProjectionExpression: 'STRING_VALUE'
    },
    /* '<TableName>': ... */
  },
  ReturnConsumedCapacity: INDEXES | TOTAL | NONE
};
```
