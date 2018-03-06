'use strict';

const {clone, deepFreeze, omit, pick} = require(`kixx/library`);

const KEY_DELIMITER = `:`;

const omitMetaKeys = omit([`created`, `updated`]);
const keyFromRootRecord = pick([`_id`, `_scope_type_key`]);
const keyFromRelationshipRecord = pick([`_subject_key`, `_predicate_key`]);

function rootScopeTypeKey(scope, type) {
	return scope + KEY_DELIMITER + type;
}

function relationshipSubjectKey(subject) {
	const {scope, type, id} = subject;
	return scope + KEY_DELIMITER + type + KEY_DELIMITER + id;
}

function relationshipObjectKey(object) {
	const {scope, type, id} = object;
	return scope + KEY_DELIMITER + type + KEY_DELIMITER + id;
}

function relationshipPredicateKey(predicate, index, object) {
	const {type, id} = object;
	return predicate + KEY_DELIMITER + index + KEY_DELIMITER + type + KEY_DELIMITER + id;
}

class DynamoDB {
	static newRootRecord(spec) {
		const {scope, type, id, created, updated} = spec;
		const attributes = spec.attributes ? clone(spec.attributes) : {};
		const meta = spec.meta ? omitMetaKeys(spec.meta) : {};

		const record = {
			_scope: scope,
			_type: type,
			_id: id,
			_scope_type_key: rootScopeTypeKey(scope, type),
			_created: created,
			_updated: updated,
			_meta: meta
		};

		return deepFreeze(Object.assign(record, attributes));
	}

	static newRelationshipRecord(subject, predicate, index, object) {
		return Object.freeze({
			_subject_scope: subject.sope,
			_subject_type: subject.type,
			_subject_id: subject.id,
			_predicate: predicate,
			_index: index,
			_object_scope: object.sope,
			_object_type: object.type,
			_object_id: object.id,
			_subject_key: relationshipSubjectKey(subject),
			_object_key: relationshipObjectKey(object),
			_predicate_key: relationshipPredicateKey(predicate, index, object)
		});
	}

	static keyFromRootRecord(record) {
		return keyFromRootRecord(record);
	}

	static keyFromRelationshipRecord(record) {
		return keyFromRelationshipRecord(record);
	}
}

Object.defineProperties(DynamoDB, {
	KEY_DELIMITER: {
		enumerable: true,
		value: KEY_DELIMITER
	}
});

module.exports = DynamoDB;
