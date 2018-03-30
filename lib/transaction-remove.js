'use strict';

module.exports = function transaction_remove(self, obj, options) {
	const {scope, type, id} = obj;

	function removeEntity() {
		const Key = DatabaseRecord.createKey(scope, type, id);

		return dynamodb.removeEntity({Key}).then(() => {
			return true;
		}).catch((err) => {
			return Promise.reject(new StackedError(
				`Error attempting to remove ${scope}:${type}:${id} from DynamoDB during remove()`,
				err
			));
		});
	}

	return removeEntity();
};

