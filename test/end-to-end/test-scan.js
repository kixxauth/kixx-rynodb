'use strict';

const {assert, find, append, init, head, last} = require(`kixx/library`);
const {hasKey} = require(`../../lib/library`);

const {reportFullStackTrace} = require(`../test-support/library`);

module.exports = function (t, params) {
	const {scope, documents, collections, createTransaction} = params;

	t.describe(`scan`, (t) => {
		t.describe(`fixed subset of documents`, (t) => {
			const type = `testCollection`;

			let response;

			t.before((done) => {
				return createTransaction().scan({scope, type})
					.then((res) => {
						response = res;
						return null;
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			});

			t.it(`returns all the collections`, () => {
				const items = response.data;
				assert.isEqual(collections.length, items.length, `length`);
				items.forEach((data) => {
					const doc = find(hasKey(data), collections);
					assert.isOk(doc, `found document`);
				});
			});

			t.it(`returns a null cursor`, () => {
				assert.isEqual(null, response.cursor, `res.cursor`);
			});
		});

		t.describe(`paged set of documents`, (t) => {
			const txn = createTransaction();
			const type = `fooType`;

			let page1;
			let page2;
			let page3;

			t.before((done) => {
				return txn.scan({scope, type})
					.then((res) => {
						page1 = res;
						return res;
					})
					.then((res) => {
						const {cursor} = res;
						const limit = 100;
						return txn.scan({scope, type, cursor, limit}).then((res) => {
							page2 = res;
							return res;
						});
					})
					.then((res) => {
						const {cursor} = res;
						return txn.scan({scope, type, cursor}).then((res) => {
							page3 = res;
							return res;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			});

			t.describe(`page1`, (t) => {
				t.it(`returns a default page of documents`, () => {
					const items = page1.data;
					// Default page length is 10
					assert.isEqual(10, items.length, `length`);
					items.forEach((data) => {
						const doc = find(hasKey(data), documents);
						assert.isOk(doc, `found document`);
					});
				});

				t.it(`returns a cursor`, () => {
					const {id, scope_type_key, updated} = page1.cursor;
					assert.isNonEmptyString(id.S, `id`);
					assert.isNonEmptyString(scope_type_key.S, `scope_type_key`);
					assert.isNonEmptyString(updated.S, `updated`);
				});
			});

			t.describe(`page2`, (t) => {
				t.it(`returns a specified page of documents`, () => {
					const items = page2.data;
					assert.isEqual(100, items.length, `length`);
					items.forEach((data) => {
						const doc = find(hasKey(data), documents);
						assert.isOk(doc, `found document`);
					});
				});

				t.it(`returns no duplicates`, () => {
					const items = page2.data;
					const others = page1.data;
					items.forEach((data) => {
						const doc = find(hasKey(data), others);
						assert.isNotOk(doc, `found document`);
					});
				});

				t.it(`returns a cursor`, () => {
					const {id, scope_type_key, updated} = page2.cursor;
					assert.isNonEmptyString(id.S, `id`);
					assert.isNonEmptyString(scope_type_key.S, `scope_type_key`);
					assert.isNonEmptyString(updated.S, `updated`);
				});
			});

			t.describe(`page3`, (t) => {
				t.it(`returns a specified page of documents`, () => {
					const items = page3.data;
					// Default page length is 10
					assert.isEqual(10, items.length, `length`);
					items.forEach((data) => {
						const doc = find(hasKey(data), documents);
						assert.isOk(doc, `found document`);
					});
				});

				t.it(`returns no duplicates`, () => {
					const items = page3.data;
					const others = page1.data.concat(page2.data);
					items.forEach((data) => {
						const doc = find(hasKey(data), others);
						assert.isNotOk(doc, `found document`);
					});
				});

				t.it(`returns a cursor`, () => {
					const {id, scope_type_key, updated} = page3.cursor;
					assert.isNonEmptyString(id.S, `id`);
					assert.isNonEmptyString(scope_type_key.S, `scope_type_key`);
					assert.isNonEmptyString(updated.S, `updated`);
				});
			});
		});

		t.describe(`get all documents of a certain type`, (t) => {
			const txn = createTransaction();
			const type = `barType`;
			const controlSet = documents.filter((doc) => {
				return doc.type === type;
			});
			const limit = 50;
			const params = {scope, type, limit};

			function getPage(pages, cursor) {
				const args = Object.assign({}, params, {cursor});
				return txn.scan(args).then((res) => {
					pages = append(res, pages);
					if (res.cursor) {
						return getPage(pages, res.cursor);
					}
					return pages;
				});
			}

			let response;
			let fetchedDocs;

			t.before((done) => {
				return getPage([]).then((res) => {
					response = res;
					fetchedDocs = res.reduce((docs, r) => {
						return docs.concat(r.data);
					}, []);
					return done();
				}).catch(reportFullStackTrace(done));
			});

			t.it(`returns all documents of expected type`, () => {
				assert.isEqual(controlSet.length, fetchedDocs.length, `length`);
				fetchedDocs.forEach((doc) => {
					const original = find(hasKey(doc), controlSet);
					assert.isOk(original, `got original`);
				});
			});

			t.it(`aggregates docs into pages of expected length`, () => {
				init(response).forEach((r) => {
					assert.isEqual(limit, r.data.length, `page length`);
				});
				assert.isOk(last(response).data.length <= limit, `last page`);
			});

			t.it(`orders by updated date`, () => {
				const firstResult = head(fetchedDocs).meta;
				const lastResult = last(fetchedDocs).meta;
				assert.isGreaterThan(firstResult.updated, lastResult.updated, `updated`);
			});
		});
	});
};
