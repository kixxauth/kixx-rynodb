'use strict';

const {assert, assoc} = require(`kixx/library`);

const createDocument = require(`../test-support/create-document`);
const {reportFullStackTrace} = require(`../test-support/library`);

module.exports = function (t, params) {
	const {scope, documents, createTransaction} = params;

	t.describe(`remove`, (t) => {
		t.describe(`with related objects`, (t) => {
			const doc = createDocument({
				type: `testRemoveType`
			});

			const relatedObjects = documents.slice(100, 103).map((x) => {
				const relationships = {foos: [{type: doc.type, id: doc.id}]};
				return assoc(`relationships`, relationships, x);
			});

			const key = {type: doc.type, id: doc.id};

			let initialFetch;
			let response;
			let fetchAfterRemove;
			let fetchRelated;

			t.before((done) => {
				createTransaction().set({scope, object: doc})
					.then(() => {
						return createTransaction().batchSet({
							scope,
							objects: relatedObjects,
							isolated: true
						});
					})
					.then(() => {
						return createTransaction().get({scope, key}).then((res) => {
							initialFetch = res;
							return null;
						});
					})
					.then(() => {
						return createTransaction().remove({scope, key}).then((res) => {
							response = res;
							return null;
						});
					})
					.then(() => {
						return createTransaction().get({scope, key}).then((res) => {
							fetchAfterRemove = res;
							return null;
						});
					})
					.then(() => {
						const keys = relatedObjects.map((obj) => {
							return {type: obj.type, id: obj.id};
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
			});

			t.describe(`initialFetch`, (t) => {
				t.it(`returns a copy of the document`, () => {
					const data = initialFetch.data;
					assert.isNotEqual(doc, data, `is !==`);
					assert.isEqual(doc.type, data.type, `type`);
					assert.isEqual(doc.id, data.id, `id`);
					assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				});
			});

			t.it(`returns a boolean true`, () => {
				assert.isEqual(true, response.data, `response.data`);
			});

			t.it(`cannot be fetched`, () => {
				assert.isEqual(null, fetchAfterRemove.data, `fetchAfterRemove.data`);
			});

			t.it(`removed relationship references`, () => {
				const data = fetchRelated.data;
				assert.isEqual(relatedObjects.length, data.length, `length`);
				data.forEach((item) => {
					assert.isEqual(0, item.relationships.foos.length, `relationships.foos`);
				});
			});

			t.it(`cannot use cache on a separate transaction`, () => {
				const responseMeta = response.meta[0];
				const fetchedMeta = fetchAfterRemove.meta[0];
				assert.isEqual(false, responseMeta.transactionCacheHit, `response transactionCacheHit`);
				assert.isEqual(false, fetchedMeta.transactionCacheHit, `fetched transactionCacheHit`);
			});
		});

		t.describe(`in same transaction`, (t) => {
			const txn = createTransaction();

			const doc = createDocument({
				type: `testRemoveType`
			});

			const relatedObjects = documents.slice(100, 103).map((x) => {
				const relationships = {foos: [{type: doc.type, id: doc.id}]};
				return assoc(`relationships`, relationships, x);
			});

			const key = {type: doc.type, id: doc.id};

			let initialFetch;
			let response;
			let fetchAfterRemove;
			let fetchRelated;

			t.before((done) => {
				txn.set({scope, object: doc})
					.then(() => {
						return txn.get({scope, key}).then((res) => {
							initialFetch = res;
							return null;
						});
					})
					.then(() => {
						return txn.batchSet({
							scope,
							objects: relatedObjects,
							isolated: true
						});
					})
					.then(() => {
						return txn.remove({scope, key}).then((res) => {
							response = res;
							return null;
						});
					})
					.then(() => {
						return txn.get({scope, key}).then((res) => {
							fetchAfterRemove = res;
							return null;
						});
					})
					.then(() => {
						const keys = relatedObjects.map((obj) => {
							return {type: obj.type, id: obj.id};
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
			});

			t.describe(`initialFetch`, (t) => {
				t.it(`returns a copy of the document`, () => {
					const data = initialFetch.data;
					assert.isNotEqual(doc, data, `is !==`);
					assert.isEqual(doc.type, data.type, `type`);
					assert.isEqual(doc.id, data.id, `id`);
					assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				});
			});

			t.it(`returns a boolean true`, () => {
				assert.isEqual(true, response.data, `response.data`);
			});

			t.it(`cannot be fetched`, () => {
				assert.isEqual(null, fetchAfterRemove.data, `fetchAfterRemove.data`);
			});

			t.it(`removed relationship references`, () => {
				const data = fetchRelated.data;
				assert.isEqual(relatedObjects.length, data.length, `length`);
				data.forEach((item) => {
					assert.isEqual(0, item.relationships.foos.length, `relationships.foos`);
				});
			});

			t.it(`uses the cache`, () => {
				const responseMeta = response.meta[0];
				const fetchedMeta = fetchAfterRemove.meta[0];
				assert.isEqual(true, responseMeta.transactionCacheHit, `response transactionCacheHit`);
				assert.isEqual(false, fetchedMeta.transactionCacheHit, `fetched transactionCacheHit`);
			});
		});
	});
};
