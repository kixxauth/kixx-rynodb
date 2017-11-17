'use strict';

const {assert} = require(`kixx/library`);
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

			let response;
			let fetched;

			t.before((done) => {
				createTransaction().set({scope, object: doc})
					.then((res) => {
						response = res;
						return null;
					})
					.then(() => {
						return createTransaction().get({scope, key}).then((res) => {
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
			});

			t.it(`fetches a copy of the document`, () => {
				const data = fetched.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
			});

			t.it(`cannot use the cache on a separate transaction`, () => {
				const meta = fetched.meta[0];
				assert.isEqual(false, meta.transactionCacheHit, `no transactionCacheHit`);
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
			});

			t.it(`fetches a copy of the document`, () => {
				const data = fetched.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(doc.attributes.title, data.attributes.title, `attributes.title`);
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

			let response;
			let fetched;

			t.before((done) => {
				txn.get({scope, key})
					.then((res) => {
						const object = res.data;

						// Make some changes to the object.
						object.attributes.title = `Foo Bar Baz`;
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
				assert.isEqual(`Foo Bar Baz`, data.attributes.title, `attributes.title`);
			});

			t.it(`fetches a copy of the document`, () => {
				const data = fetched.data;
				assert.isNotEqual(doc, data, `is !==`);
				assert.isEqual(doc.type, data.type, `type`);
				assert.isEqual(doc.id, data.id, `id`);
				assert.isEqual(`Foo Bar Baz`, data.attributes.title, `attributes.title`);
			});

			t.it(`uses the cache on the same transaction`, () => {
				const meta = fetched.meta[0];
				assert.isEqual(true, meta.transactionCacheHit, `transactionCacheHit`);
			});
		});
	});
};
