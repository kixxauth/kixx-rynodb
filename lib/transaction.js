'use strict';

class Transaction {
	deleteItem(key, options = {}) {
		const {scope, type, id} = key;

		function removeIndexEntries() {
			const subjectKey = IndexEntry.createSubjectKey(scope, type, id);
		}

		function removeRelationshipEntries() {
			const subjectKey = RelationshipEntry.createSubjectKey(scope, type, id);
		}
	}
}
