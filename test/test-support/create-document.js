'use strict';

const {range, random} = require(`kixx/library`);
const Chance = require(`chance`);

const chance = new Chance();

module.exports = function createDocument(spec) {
	spec = spec || {};
	return Object.assign({
		type: chance.pickone([`fooType`, `barType`]),
		id: chance.guid(),
		attributes: {
			title: range(0, random(2, 7)).map(() => chance.word()).join(` `),
			description: chance.sentence(),
			images: range(0, random(0, 5)).map(() => {
				return {
					label: chance.pickone([`original`, `thumbnail`]),
					url: chance.url({extensions: [`jpg`, `png`]})
				};
			})
		}
	}, spec);
};
