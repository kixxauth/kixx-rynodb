'use strict';

module.exports = function transaction_create(self, obj, options) {
	const {scope, type, id} = obj;

	const dynamodb = self._dynamodb;

	function checkConflict() {
		const Key = DatabaseRecord.createKey(scope, type, id);
		const ProjectionExpression = Object.keys(Key).join(', ');
		const params = {Key, ProjectionExpression};

		return dynamodb.getEntity(params).then(({item}) => {
			if (item) {
				return Promise.reject(new ConflictError(
					`Item ${scope}:${type}:${id} already exists and cannot be created`
				));
			}
			return null;
		}, (err) => {
			return Promise.reject(new StackedError(
				`Error attempting to check if ${scope}:${type}:${id} exists in DynamoDB`,
				err
			));
		});
	}

	function save() {
		return self._set(obj, options).catch(function (err) {
			return Promise.reject(new StackedError(
				`Error attempting to set ${scope}:${type}:${id} during the create operation`
			));
		});
	}

	return checkConflict().then(save);
};
