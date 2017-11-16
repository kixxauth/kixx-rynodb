'use strict';

const {assert, find} = require(`kixx/library`);
const Chance = require(`chance`);
const {hasKey} = require(`../../lib/library`);

const {reportFullStackTrace} =require(`../test-support/library`);

const chance = new Chance();

module.exports = function (t, params) {
	const {scope, documents, collections, createTransaction} = params;

	t.describe(`get`, (t) => {
		t.describe(`simple use case`, (t) => {
			const doc = chance.pickone(documents);
			const key = {type: doc.type, id: doc.id};

			let noCacheResponse;
			let noCacheElapsed;
			let cachedResponse;
			let withCacheElapsed;

			t.before((done) => {
				const start = Date.now();
				const txn = createTransaction();

				return txn.get({scope, key})
					.then((res) => {
						noCacheResponse = res;
						noCacheElapsed = Date.now() - start;
						return null;
					})
					.then(() => {
						const start = Date.now();
						return txn.get({scope, key}).then((res) => {
							cachedResponse = res;
							withCacheElapsed = Date.now() - start;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			});

			t.it(`retrieved the correct non cached object`, () => {
				const obj = noCacheResponse.data;
				assert.isEqual(doc.type, obj.type, `type`);
				assert.isEqual(doc.id, obj.id, `id`);
				assert.isEqual(doc.attributes.title, obj.attributes.title, `attributes.title`);
			});

			t.it(`retrieved the correct cached object`, () => {
				const obj = cachedResponse.data;
				assert.isEqual(doc.type, obj.type, `type`);
				assert.isEqual(doc.id, obj.id, `id`);
				assert.isEqual(doc.attributes.title, obj.attributes.title, `attributes.title`);
			});

			t.it(`appropriately used the transaction cache`, () => {
				const cachedMeta = cachedResponse.meta[0];
				const noCachedMeta = noCacheResponse.meta[0];
				assert.isOk(cachedMeta.transactionCacheHit, `meta.transactionCacheHit`);
				assert.isNotOk(noCachedMeta.transactionCacheHit, `meta.transactionCacheHit`);
				assert.isGreaterThan(5, noCacheElapsed, `noCacheElapsed`);
				assert.isLessThan(5, withCacheElapsed, `withCacheElapsed`);
			});
		});

		t.describe(`with include 2:1`, (t) => {
			const collection = collections[0];
			const key = {type: collection.type, id: collection.id};
			const include = [`foos`, `bars`];

			let noCacheResponse;
			let noCacheElapsed;
			let cachedResponse;
			let withCacheElapsed;

			t.before((done) => {
				const start = Date.now();
				const txn = createTransaction();

				return txn.get({scope, key, include})
					.then((res) => {
						noCacheResponse = res;
						noCacheElapsed = Date.now() - start;
						return null;
					})
					.then(() => {
						const start = Date.now();
						return txn.get({scope, key, include}).then((res) => {
							cachedResponse = res;
							withCacheElapsed = Date.now() - start;
							return null;
						});
					})
					.then(() => {
						return done();
					})
					.catch(reportFullStackTrace(done));
			});

			t.it(`retrieved the correct non cached object`, () => {
				const obj = noCacheResponse.data;
				assert.isEqual(collection.type, obj.type, `type`);
				assert.isEqual(collection.id, obj.id, `id`);
				assert.isEqual(collection.attributes.title, obj.attributes.title, `attributes.title`);
			});

			t.it(`retrieved the correct cached object`, () => {
				const obj = cachedResponse.data;
				assert.isEqual(collection.type, obj.type, `type`);
				assert.isEqual(collection.id, obj.id, `id`);
				assert.isEqual(collection.attributes.title, obj.attributes.title, `attributes.title`);
			});

			t.it(`retrieved the correct non cached relationships`, () => {
				const included = noCacheResponse.included;
				const rel = (collection.relationships.foos || []).concat(collection.relationships.bars || []);

				assert.isEqual(rel.length, included.length, `length`);
				assert.isGreaterThan(100, rel.length, `more than 100`);

				rel.forEach((key) => {
					assert.isOk(find(hasKey(key), included, `has key`));
				});
			});

			t.it(`retrieved the correct cached relationships`, () => {
				const included = cachedResponse.included;
				const rel = (collection.relationships.foos || []).concat(collection.relationships.bars || []);

				assert.isEqual(rel.length, included.length, `length`);
				assert.isGreaterThan(100, rel.length, `more than 100`);

				rel.forEach((key) => {
					assert.isOk(find(hasKey(key), included, `has key`));
				});
			});

			t.it(`appropriately used the transaction cache`, () => {
				const cachedMeta = cachedResponse.meta[0];
				const noCachedMeta = noCacheResponse.meta[0];
				assert.isOk(cachedMeta.transactionCacheHit, `meta.transactionCacheHit`);
				assert.isNotOk(noCachedMeta.transactionCacheHit, `meta.transactionCacheHit`);
				assert.isGreaterThan(100, noCacheElapsed, `noCacheElapsed`);
				assert.isLessThan(30, withCacheElapsed, `withCacheElapsed`);
			});
		});
	});
};
