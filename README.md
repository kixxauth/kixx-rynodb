Kixx
====
A enhanced Dynamodb store for [Kixx](https://github.com/kixxauth/kixx) applications.

## DynamoDB Schema

### Entities Table
Holds all database records as JSON serialized objects.

__Record__

```js
{
    scope: STRING,
    type: STRING,
    id: STRING,
    scope_type_key: STRING,
    created: ISO_DATE_STRING,
    updated: ISO_DATE_STRING,
    attributes: HASH_OBJECT,
    meta: HASH_OBJECT
}
```

__Name:__ PREFIX_entities_master

Key                   | Name           | Value
--------------------- | -------------- | -----
Primary partition key | id             | The subject ID String
Primary sort key      | scope_type_key | Compound SCOPE:TYPE String

__Index name:__ PREFIX_entities_by_type

Key           | Name           | Value
------------- | -------------- | -----
Partition key | scope_type_key | Compound SCOPE:TYPE String
Sort key      | updated        | The subject updated ISO Date String

### Relationships Table
Holds all relationship entries as a subject -> predicat -> object tuple.

__Record__

```js
{
    object_scope: STRING,
    object_type: STRING,
    object_id: STRING,
    predicate: STRING,
    index: INTEGER,
    subject_key: `${subject.scope}:${subject.type}:${subject.id}`,
    object_key: `${object_scope}:${object_type}:${object_id}`,
    predicate_key: `${predicate}:${object_id}:${index}`
}
```

__Name:__ PREFIX_relationship_entries

Key                   | Name          | Value
--------------------- | ------------- | -----
Primary partition key | subject_key   | Compound SCOPE:TYPE:ID String
Primary sort key      | predicate_key | Compound PREDICATE:OBJECT:INDEX String

*The compound PREDICATE:OBJECT:INDEX sort key allows us to store a object ID multiple times on the same subject->predicate*

__Index name:__ PREFIX_reverse_relationships

Key           | Name           | Value
------------- | -------------- | -----
Partition key | object_key     | Compound SCOPE:TYPE:ID String
Sort key      | index          | The object relationship index Integer

### Index Lookup Table
Holds all index entries emittied from mapping functions.

__Record__

```js
{
    scope: STRING,
    type: STRING,
    id: STRING,
    index_name: STRING, // Also the name of the map function.
    compound_key: STRING, // The index value created by the map function.
    subject_key: `${scope}:${type}:${id}`,
    unique_key: `${index_name}:${compound_key}`
}
```

__Name:__ PREFIX_index_entries

Key                   | Name        | Value
--------------------- | ----------- | -----
Primary partition key | subject_key | Compound SCOPE:TYPE:ID String
Primary sort key      | unique_key  | Compound INDEX_NAME:COMPOUND_KEY String

__Index name:__ PREFIX_index_lookup

Key           | Name         | Value
------------- | ------------ | -----
Partition key | scope_name   | Compound SCOPE:INDEX_NAME String
Sort key      | compound_key | The index value created by the map function.

### Schema Use Cases

__Get subject by ID:__ Use subject scope, type, and id to getItem() from `entities_master` table.

__Page all subjects by type:__ Use subject scope and type to query(scope_type_key) from `entities_by_type` index.

__Get relationship keys for subject:__ Use subject scope, type, and id to query() from `relationship_entries` table.

__Get relationship objects for subject:__ Use subject scope, type, and id to query() from `relationship_entries` table. Then, use batchGet() to get the objects from the `entities_master` table.

__Get relationship keys by predicate for subject:__ Use subject scope, type, id, and predicate to query(scope_type_key).begins_with(predicate) from `relationship_entries` table.

__Get relationship objects by predicate for subject:__ Use subject scope, type, id, and predicate to query(scope_type_key).begins_with(predicate) from `relationship_entries` table. Then, use batchGet() to get the objects from the `entities_master` table.

__Query objects from index:__ Use index scope and name to query(scope_name).anyOtherRangeQuery() from index_lookup. Use returned scope, type, and id to batchGet() items from `entities_master`.

__Delete a record:__ Use object scope, type, and id to query(object_key) `reverse_relationships` index. Then, use batchWriteItem() to delete items from `relationship_entries` using returned subject_key and predicate_key. In parallel, Use object scope, type, and id to query(subject_key) `index_entries`. Then, use batchWriteItem() to delete items from `index_entries` using returned subject_key and unique_key. Finally deleteItem() from the `entities_master` table using the scope, type, and id.

__Set a record:__ First, use putItem() to set the record in `entities_master`. If there are any relationships on the record, replace them (see *Replace relationships on subject* below).

__Remove some relationships from subject:__ Use subject scope, type, id and predicate key to query(subject_key).begins_with(predicate) `relationship_entries` table. Then, use batchWriteItem() to delete records from `relationship_entries` using the returned subject_key and predicate_key.

__Add some relationships on subject:__ Use subject scope, type, id and predicate key to query(subject_key).begins_with(predicate) `relationship_entries` table. Concat new entries, then dedupe them using the returned subject_key and predicate_key. Then, use batchWriteItem() to add new records to `relationship_entries`.

__Replace relationships on subject:__ Use subject scope, type, id and predicate key to query(subject_key).begins_with(predicate) `relationship_entries` table. Then, use batchWriteItem() to delete records from `relationship_entries` using the returned subject_key and predicate_key. Finally, use batchWriteItem() to add all the new records to `relationship_entries`.

Copyright and License
---------------------
Copyright: (c) 2017 by Kris Walker (www.kixx.name)

Unless otherwise indicated, all source code is licensed under the MIT license. See MIT-LICENSE for details.

