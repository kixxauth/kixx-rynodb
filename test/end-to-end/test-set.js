'use strict';

const {assert, assoc} = require(`kixx/library`);
const Chance = require(`chance`);

const createDocument = require(`../test-support/create-document`);
const {reportFullStackTrace} = require(`../test-support/library`);

const chance = new Chance();

module.exports = function (t, params) {
	const {scope, documents, createTransaction} = params;

	t.describe(`set`, (t) => {
		t.describe(`simple use case`, (t) => {
			const doc = createDocument({type: `testSetType`});
			const key = {type: doc.type, id: doc.id};

			const relatedObject = documents.slice(202, 203).map((x) => {
				return Object.freeze(assoc(`relationships`, {testSet: [key]}, x));
			})[0];

			let response;
			let fetched;
			let fetchRelated;

			t.before((done) => {
				createTransaction().set({scope, object: doc})
					.then((res) => {
						response = res;
						return null;
					})
					.then(() => {
						return createTransaction().set({
							scope,
							object: relatedObject,
							isolated: true
						});
					})
					.then(() => {
						return createTransaction().get({scope, key}).then((res) => {
							fetched = res;
							return null;
						});
					})
					.then(() => {
						return createTransaction().remove({scope, key});
					})
					.then(() => {
						const key = {type: relatedObject.type, id: relatedObject.id};
						return createTransaction().get({scope, key}).then((res) => {
							fetchRelated = res;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			});

			t.it(`returns a copy of the document`, () => {
				const data = response.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				assert.isEqual(doc.meta.foo, data.meta.foo, `meta.foo`);
				assert.isNonEmptyString(data.meta.created, `meta.created`);
				assert.isNonEmptyString(data.meta.updated, `meta.updated`);
			});

			t.it(`fetches a copy of the document`, () => {
				const data = fetched.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				assert.isEqual(response.data.meta.foo, data.meta.foo, `meta.foo`);
				assert.isEqual(response.data.meta.created, data.meta.created, `meta.created`);
				assert.isEqual(response.data.meta.updated, data.meta.updated, `meta.updated`);
			});

			t.it(`cannot use the cache on a separate transaction`, () => {
				const responseMeta = response.meta[0];
				const fetchedMeta = fetched.meta[0];
				assert.isEqual(false, responseMeta.transactionCacheHit, `response transactionCacheHit`);
				assert.isEqual(false, fetchedMeta.transactionCacheHit, `fetched transactionCacheHit`);
			});

			t.it(`linked relationship references for removal`, () => {
				const data = fetchRelated.data;
				assert.isEqual(0, data.relationships.testSet.length, `relationships.testSet`);
			});
		});

		t.describe(`same transaction`, (t) => {
			const doc = createDocument({type: `testSetType`});
			const txn = createTransaction();

			let response;
			let fetched;

			t.before((done) => {
				txn.set({scope, object: doc})
					.then((res) => {
						response = res;
						return null;
					})
					.then(() => {
						const key = {type: doc.type, id: doc.id};
						return txn.get({scope, key}).then((res) => {
							fetched = res;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			});

			t.it(`returns a copy of the document`, () => {
				const data = response.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				assert.isNonEmptyString(data.meta.created, `meta.created`);
				assert.isNonEmptyString(data.meta.updated, `meta.updated`);
			});

			t.it(`fetches a copy of the document`, () => {
				const data = fetched.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
				assert.isEqual(response.data.meta.created, data.meta.created, `meta.created`);
				assert.isEqual(response.data.meta.updated, data.meta.updated, `meta.updated`);
			});

			t.it(`uses the cache on the same transaction`, () => {
				const meta = fetched.meta[0];
				assert.isEqual(true, meta.transactionCacheHit, `transactionCacheHit`);
			});
		});

		t.describe(`update in same transaction`, (t) => {
			const doc = chance.pickone(documents);
			const key = {type: doc.type, id: doc.id};
			const txn = createTransaction();

			let original;
			let response;
			let fetched;

			t.before((done) => {
				txn.get({scope, key})
					.then((res) => {
						original = res.data;
						const object = res.data;

						// Make some changes to the object.
						object.attributes.title = `Test Set Title`;
						object.relationships.foos = documents.slice(0, 10).map((d) => {
							return {type: d.type, id: d.id};
						});

						// Save the object.
						return txn.set({scope, object}).then((res) => {
							response = res;
							return null;
						});
					})
					.then(() => {
						// Fetch the object after updating it.
						return txn.get({scope, key}).then((res) => {
							fetched = res;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			});

			t.it(`returns a copy of the document`, () => {
				const data = response.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(`Test Set Title`, data.attributes.title, `attributes.title`);
			});

			t.it(`fetches a copy of the document`, () => {
				const data = fetched.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(`Test Set Title`, data.attributes.title, `attributes.title`);
			});

			t.it(`correctly updates meta data`, () => {
				const data = fetched.data;
				assert.isEqual(original.meta.created, data.meta.created, `created`);
				assert.isGreaterThan(original.meta.created, data.meta.updated, `updated`);
			});

			t.it(`uses the cache on the same transaction`, () => {
				const meta = fetched.meta[0];
				assert.isEqual(true, meta.transactionCacheHit, `transactionCacheHit`);
			});
		});
	});
};
