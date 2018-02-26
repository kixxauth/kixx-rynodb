'use strict';

const {isArray, curry, printf, assertion1} = require(`kixx/library`);

exports.assertIsArray = assertion1(isArray, (actual) => {
	return printf(`expected %x to be an Array`, actual);
});

exports.hasKey = curry(function hasKey(a, b) {
	return a.id === b.id && a.type === b.type;
});

function waitForAllPromises(operations) {
	const length = operations.length;

	return new Promise((resolve, reject) => {
		const results = [];
		const errors = [];
		let count = 0;

		function maybeComplete() {
			if (count === length) {
				if (errors.length > 0) {
					reject(errors[0]);
				} else {
					resolve(results);
				}
			}
			return null;
		}

		function maybeCompleteResult(index) {
			return function (result) {
				count += 1;
				results[index] = result;
				return maybeComplete();
			};
		}

		function maybeCompleteError(err) {
			count += 1;
			errors.push(err);
			return maybeComplete();
		}

		for (let i = 0; i < length; i++) {
			operations[i].then(maybeCompleteResult(i), maybeCompleteError);
		}
	});
}
exports.waitForAllPromises = waitForAllPromises;
