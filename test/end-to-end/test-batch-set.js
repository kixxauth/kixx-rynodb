'use strict';

const {assert, find, assoc, range} = require(`kixx/library`);
const Chance = require(`chance`);
const {hasKey} = require(`../../lib/library`);

const createDocument = require(`../test-support/create-document`);
const {reportFullStackTrace} = require(`../test-support/library`);

const chance = new Chance();

module.exports = function (t, params) {
	const {scope, documents, createTransaction} = params;

	t.describe(`batchSet`, (t) => {
		t.describe(`simple use case`, (t) => {
			const objects = Object.freeze(range(0, 100).map(() => {
				return createDocument({type: `testBatchSetType`});
			}));

			const keys = Object.freeze(objects.map((obj) => {
				return Object.freeze({type: obj.type, id: obj.id});
			}));

			const relatedObjects = Object.freeze(documents.slice(400, 500).map((doc) => {
				return Object.freeze(assoc(
					`relationships`,
					{testBatchSet: Object.freeze(chance.pickset(keys, 10))},
					doc
				));
			}));

			let response;
			let fetched;
			let fetchRelated;

			t.before((done) => {
				createTransaction().batchSet({scope, objects})
					.then((res) => {
						response = res;
						return null;
					})
					.then(() => {
						return createTransaction().batchSet({
							scope,
							objects: relatedObjects,
							isolated: true
						});
					})
					.then(() => {
						return createTransaction().batchGet({scope, keys}).then((res) => {
							fetched = res;
							return null;
						});
					})
					.then(() => {
						return createTransaction().batchRemove({scope, keys});
					})
					.then(() => {
						const keys = relatedObjects.map((x) => {
							return {type: x.type, id: x.id};
						});
						return createTransaction().batchGet({scope, keys}).then((res) => {
							fetchRelated = res;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			}, {timeout: 7000});

			t.it(`returns a copy of each document`, () => {
				const items = response.data;
				assert.isEqual(objects.length, items.length, `length`);
				items.forEach((data) => {
					const doc = find(hasKey(data), objects);
					assert.isNotEqual(doc, data, `is !==`);
					assert.isEqual(doc.type, data.type, `type`);
					assert.isEqual(doc.id, data.id, `id`);
					assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				});
			});

			t.it(`fetches a copy of each document`, () => {
				const items = fetched.data;
				assert.isEqual(objects.length, items.length, `length`);
				items.forEach((data) => {
					const doc = find(hasKey(data), objects);
					assert.isNotEqual(doc, data, `is !==`);
					assert.isEqual(doc.type, data.type, `type`);
					assert.isEqual(doc.id, data.id, `id`);
					assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				});
			});

			t.it(`cannot use the cache on a separate transaction`, () => {
				const responseMeta = response.meta[0];
				const fetchedMeta = fetched.meta[0];
				assert.isEqual(0, responseMeta.transactionCacheHits, `response transactionCacheHits`);
				assert.isEqual(objects.length, responseMeta.transactionCacheMisses, `response transactionCacheMisses`);
				assert.isEqual(0, fetchedMeta.transactionCacheHits, `fetched transactionCacheHits`);
				assert.isEqual(objects.length, fetchedMeta.transactionCacheMisses, `fetched transactionCacheMisses`);
			});

			t.it(`linked relationship references for removal`, () => {
				const items = fetchRelated.data;
				assert.isEqual(relatedObjects.length, items.length, `length`);
				items.forEach((data) => {
					assert.isEqual(0, data.relationships.testBatchSet.length, `relationships.testBatchSet`);
				});
			});
		});

		t.describe(`same transaction`, (t) => {
			const txn = createTransaction();

			const objects = Object.freeze(range(0, 100).map(() => {
				return createDocument({type: `testBatchSetType`});
			}));

			const keys = Object.freeze(objects.map((obj) => {
				return Object.freeze({type: obj.type, id: obj.id});
			}));

			const relatedObjects = Object.freeze(documents.slice(400, 500).map((doc) => {
				return Object.freeze(assoc(
					`relationships`,
					{testBatchSet: Object.freeze(chance.pickset(keys, 10))},
					doc
				));
			}));

			let response;
			let fetched;
			let fetchRelated;

			t.before((done) => {
				txn.batchSet({scope, objects})
					.then((res) => {
						response = res;
						return null;
					})
					.then(() => {
						return txn.batchSet({
							scope,
							objects: relatedObjects,
							isolated: true
						});
					})
					.then(() => {
						return txn.batchGet({scope, keys}).then((res) => {
							fetched = res;
							return null;
						});
					})
					.then(() => {
						return txn.batchRemove({scope, keys});
					})
					.then(() => {
						const keys = relatedObjects.map((x) => {
							return {type: x.type, id: x.id};
						});
						return txn.batchGet({scope, keys}).then((res) => {
							fetchRelated = res;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			}, {timeout: 7000});

			t.it(`returns a copy of each document`, () => {
				const items = response.data;
				assert.isEqual(objects.length, items.length, `length`);
				items.forEach((data) => {
					const doc = find(hasKey(data), objects);
					assert.isNotEqual(doc, data, `is !==`);
					assert.isEqual(doc.type, data.type, `type`);
					assert.isEqual(doc.id, data.id, `id`);
					assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				});
			});

			t.it(`uses the cache on the same transaction`, () => {
				const responseMeta = response.meta[0];
				const fetchedMeta = fetched.meta[0];
				assert.isEqual(0, responseMeta.transactionCacheHits, `response transactionCacheHits`);
				assert.isEqual(objects.length, responseMeta.transactionCacheMisses, `response transactionCacheMisses`);
				assert.isEqual(objects.length, fetchedMeta.transactionCacheHits, `fetched transactionCacheHits`);
				assert.isEqual(0, fetchedMeta.transactionCacheMisses, `fetched transactionCacheMisses`);
			});

			t.it(`linked relationship references for removal`, () => {
				const items = fetchRelated.data;
				assert.isEqual(relatedObjects.length, items.length, `length`);
				items.forEach((data) => {
					assert.isEqual(0, data.relationships.testBatchSet.length, `relationships.testBatchSet`);
				});
			});
		});

		t.describe(`update in the same transaction`, (t) => {
			const txn = createTransaction();

			const keys = chance.pickset(documents, 100).map((obj) => {
				return Object.freeze({type: obj.type, id: obj.id});
			});

			let response;
			let fetched;

			t.before((done) => {
				txn.batchGet({scope, keys})
					.then((res) => {
						const objects = res.data.map((obj) => {
							obj.attributes.title = `Test batchSet() Title`;
							return obj;
						});

						return txn.batchSet({scope, objects}).then((res) => {
							response = res;
							return null;
						});
					})
					.then((res) => {
						return txn.batchGet({scope, keys}).then((res) => {
							fetched = res;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			}, {timeout: 7000});

			t.it(`returns a copy of each document`, () => {
				const items = response.data;
				assert.isEqual(keys.length, items.length, `length`);
				items.forEach((data) => {
					const doc = find(hasKey(data), documents);
					assert.isNotEqual(doc, data, `is !==`);
					assert.isEqual(doc.type, data.type, `type`);
					assert.isEqual(doc.id, data.id, `id`);
					assert.isEqual(`Test batchSet() Title`, data.attributes.title, `attributes.title`);
				});
			});

			t.it(`fetches a copy of each document`, () => {
				const items = fetched.data;
				assert.isEqual(keys.length, items.length, `length`);
				items.forEach((data) => {
					const doc = find(hasKey(data), documents);
					assert.isNotEqual(doc, data, `is !==`);
					assert.isEqual(doc.type, data.type, `type`);
					assert.isEqual(doc.id, data.id, `id`);
					assert.isEqual(`Test batchSet() Title`, data.attributes.title, `attributes.title`);
				});
			});

			t.it(`uses the cache on the same transaction`, () => {
				const responseMeta = response.meta[0];
				const fetchedMeta = fetched.meta[0];
				assert.isEqual(keys.length, responseMeta.transactionCacheHits, `response transactionCacheHits`);
				assert.isEqual(0, responseMeta.transactionCacheMisses, `response transactionCacheMisses`);
				assert.isEqual(keys.length, fetchedMeta.transactionCacheHits, `fetched transactionCacheHits`);
				assert.isEqual(0, fetchedMeta.transactionCacheMisses, `fetched transactionCacheMisses`);
			});
		});
	});
};
