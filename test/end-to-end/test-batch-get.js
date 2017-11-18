'use strict';

const {assert, find} = require(`kixx/library`);
const {hasKey} = require(`../../lib/library`);

const {reportFullStackTrace} =require(`../test-support/library`);

module.exports = function (t, params) {
	const {scope, documents, createTransaction} = params;

	t.describe(`batchGet`, (t) => {
		t.describe(`simple use case`, (t) => {
			const batchKeys1 = documents.slice(700, 800).map((doc) => {
				return {type: doc.type, id: doc.id};
			});

			const batchKeys2 = documents.slice(703, 803).map((doc) => {
				return {type: doc.type, id: doc.id};
			});

			let batch1;
			let batch2;
			let batch1Elapsed;
			let batch2Elapsed;

			t.before((done) => {
				const start = Date.now();
				const txn = createTransaction();

				return txn.batchGet({scope, keys: batchKeys1})
					.then((res) => {
						batch1 = res;
						batch1Elapsed = Date.now() - start;
						return null;
					})
					.then(() => {
						const start = Date.now();
						return txn.batchGet({scope, keys: batchKeys2}).then((res) => {
							batch2 = res;
							batch2Elapsed = Date.now() - start;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			});

			t.it(`retrieved the correct non cached objects`, () => {
				const objects = batch1.data;
				assert.isEqual(batchKeys1.length, objects.length, `length`);
				objects.forEach((obj) => {
					const doc = find(hasKey(obj), documents);
					assert.isEqual(doc.attributes.title, obj.attributes.title, `attributes.title`);
				});
			});

			t.it(`retrieved the correct cached objects`, () => {
				const objects = batch2.data;
				assert.isEqual(batchKeys2.length, objects.length, `length`);
				objects.forEach((obj) => {
					const doc = find(hasKey(obj), documents);
					assert.isEqual(doc.attributes.title, obj.attributes.title, `attributes.title`);
				});
			});

			t.it(`appropriately used the transaction cache`, () => {
				const nonCachedMeta = batch1.meta[0];
				const cachedMeta = batch2.meta[0];
				assert.isEqual(0, nonCachedMeta.transactionCacheHits, `transactionCacheHits`);
				assert.isEqual(100, nonCachedMeta.transactionCacheMisses, `transactionCacheMisses`);
				assert.isEqual(97, cachedMeta.transactionCacheHits, `transactionCacheHits`);
				assert.isEqual(3, cachedMeta.transactionCacheMisses, `transactionCacheMisses`);
				assert.isGreaterThan(50, batch1Elapsed, `non cached elapsed`);
				assert.isLessThan(50, batch2Elapsed, `cached elapsed`);
			});
		});
	});
};
