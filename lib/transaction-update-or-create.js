'use strict';

module.exports = function transaction_updateOrCreate(self, obj, indexEntries, options) {
	const {scope, type, id} = obj;
	const {merge} = options;

	const dynamodb = self._dynamodb;

	let existingEntity;
	let existingIndexEntries;

	function getEntity() {
		const Key = DatabaseRecord.createKey(scope, type, id);

		return dynamodb.getEntity({Key}).then(({item}) => {
			existingEntity = Entity.fromDatabaseRecord(item);
			return null;
		}).catch((err) => {
			return Promise.reject(new StackedError(
				`Error attempting to get ${scope}:${type}:${id} from DynamoDB during updateOrCreate()`,
				err
			));
		});
	}

	function checkForRelationships() {
		if (existingEntity && obj.relationships) {
			return Promise.reject(new Error(
				`Cannot modify relationships during an update operation`
			));
		}

		return null;
	}

	function getIndexEntries() {
		if (!existingEntity) {
			indexEntries = [];
			return null;
		}

		const subjectKey = IndexEntry.createSubjectKey(scope, type, id);

		return dynamodb.getIndexEntriesBySubject(subjectKey).then(({items}) => {
			existingIndexEntries = items.map(IndexEntry.fromDatabaseRecordToTableKey);
			return null;
		}).catch((err) => {
			return Promise.reject(new StackedError(
				`Error attempting to get index entries from DynamoDB for ${scope}:${type}:${id} during updateOrCreate()`,
				err
			));
		});
	}

	function removeIndexEntries() {
		if (existingIndexEntries.length === 0) return null;

		return dynamodb.batchRemoveIndexEntries(existingIndexEntries).catch((err) => {
			return Promise.reject(new StackedError(
				`Error attempting to remove index entries from DynamoDB for ${scope}:${type}:${id} during updateOrCreate()`,
				err
			));
		});
	}

	function mergeAndSet() {
		let newEntity;
		let attributes;

		if (typeof merge === 'function') {
			try {
				attributes = merge(existingEntity.attributes, obj.attributes);
			} catch (err) {
				err.message = `Error in merge function supplied to updateOrCreate()`
			}
		} else {
			attributes = mergeDeep(existingEntity.attributes, obj.attributes);
		}

		if (existingEntity) {
			newEntity = {
				scope,
				type,
				id,
				attributes,
				relationships: obj.relationships
			};
		} else {
			newEntity = obj;
		}

		return self._set(newEntity, indexEntries, options).catch((err) => {
			return Promise.reject(new StackedError(
				`Error attempting to set ${scope}:${type}:${id} during updateOrCreate()`,
				err
			));
		});
	};

	return getEntity()
		.then(checkForRelationships)
		.then(getIndexEntries)
		.then(removeIndexEntries)
		.then(mergeAndSet);
};
