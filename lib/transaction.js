'use strict';

const {StackedError} = require('kixx');
const Entity = require('./entity');
const {assert, mergeDeep} = require('kixx/library');

class Transaction {
	constructor(options) {
		Object.defineProperties(this, {
			dynamodb: {
				value: options.dynamodb
			}
		});
	}

	updateOrCreateItem(object, options = {}) {
		const {scope, type, id, attributes} = object;

		assert.isNonEmptyString(scope, 'updateOrCreateItem() object.scope');
		assert.isNonEmptyString(type, 'updateOrCreateItem() object.type');
		assert.isNonEmptyString(id, 'updateOrCreateItem() object.id');

		const getEntity = () => {
			const key = Entity.createKey(scope, type, id);

			return this.dynamodb.getEntity(key, options).then((res) => {
				return Entity.fromDatabaseRecord(res.entity).toPublicItem();
			}, (err) => {
				throw new StackedError(
					`DynamoDB error during Transaction#updateOrCreateItem()`,
					err
				);
			});
		};

		const saveEntity = (object) => {
			const entity = Entity.fromPublicObject(object);

			return this.dynamodb.setEntity(entity, options).then((res) => {
				const item = Entity.fromDatabaseRecord(res.entity).toPublicItem();
				return {item};
			}, (err) => {
				throw new StackedError(
					`DynamoDB error during Transaction#updateOrCreateItem()`,
					err
				);
			});
		};

		return getEntity().then((entity) => {
			return saveEntity({
				scope,
				type,
				id,
				attributes: mergeDeep(entity.attributes, attributes)
			});
		});
	}

	createItem(object, options = {}) {
		const {scope, type, id} = object;

		assert.isNonEmptyString(scope, 'createItem() object.scope');
		assert.isNonEmptyString(type, 'createItem() object.type');
		assert.isNonEmptyString(id, 'createItem() object.id');

		const entity = Entity.fromPublicObject(object);

		return this.dynamodb.createEntity(entity, options).then((res) => {
			const item = Entity.fromDatabaseRecord(res.entity).toPublicItem();
			return {item};
		}, (err) => {
			throw new StackedError(
				`DynamoDB error during Transaction#createItem()`,
				err
			);
		});
	}

	getItem(object, options = {}) {
		const {scope, type, id} = object;

		assert.isNonEmptyString(scope, 'getItem() object.scope');
		assert.isNonEmptyString(type, 'getItem() object.type');
		assert.isNonEmptyString(id, 'getItem() object.id');

		const key = Entity.createKey(scope, type, id);

		return this.dynamodb.getEntity(key, options).then((res) => {
			const item = Entity.fromDatabaseRecord(res.entity).toPublicItem();
			return {item};
		}, (err) => {
			throw new StackedError(
				`DynamoDB error during Transaction#getItem()`,
				err
			);
		});
	}
}

module.exports = Transaction;
