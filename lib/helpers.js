'use strict';

const KixxAssert = require('kixx-assert');

const {isNonEmptyString, isObject} = KixxAssert.helpers;

function createIndexEntries(indexes, record) {
	return indexes.reduce((entries, [indexName, mapper]) => {
		function emit(key, rec) {
			if (!isNonEmptyString(key)) {
				throw new Error('The key string passed as the first argument to emit() must be a non empty String.');
			}
			if (!isObject(rec)) {
				throw new Error('The record object passed as second argument to emit() must be a plain Object.');
			}
			const attributes = rec.attributes || rec;
			entries.push(record.createIndexEntry(indexName, key, attributes));
		}
		mapper(record.toPublic(), emit);
		return entries;
	}, []);
}
exports.createIndexEntries = createIndexEntries;
