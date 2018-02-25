'use strict';

function create(options) {
	// Run the item through the index mapper.
}

module.exports = function transactionFactory() {
	return function createTransaction() {
		return {
			create,
			get,
			batchGet,
			update,
			batchCreateOrUpdate,
			remove,
			batchRemove,
			scan,
			query,
			commit,
			rollback
		};
	};
};

