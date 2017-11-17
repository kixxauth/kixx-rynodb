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
						return createTransaction().get({scope, key}).then((res) => {
							initialFetch = res;
							return null;
						});
					})
					.then(() => {
						return createTransaction().batchSet({scope, objects: relatedObjects});
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
						console.log(` ---- Fetching related ----`);
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

			t.it(`is not smoking`, () => {
				console.log(`*** fetchRelated`);
				console.log(fetchRelated.data.map((x) => x.relationships.foos));
				assert.isOk(false, `smoking`);
			});
		});
	});
};
