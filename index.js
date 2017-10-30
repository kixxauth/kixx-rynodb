'use strict';

const Kixx = require(`kixx`);

const {Error, InvariantError} = Kixx.Errors;
const {clone, unnest, append, compact, differenceWith, curry, uniqWith} = Kixx.Library;

exports.initialize = (app) => {
	const emitter = app.emitter;

	const API = Object.create(null);

	return API;
};
