module.exports = {
	"env": {
		"es6": true,
		"node": true
	},
	"extends": "eslint:recommended",
	"parserOptions": {
		"ecmaVersion": 2017
	},
	"rules": {
		"array-bracket-spacing": [
			"error",
			"always",
			{ objectsInArrays: false }
		],
		"array-callback-return": [
			"error"
		],
		"arrow-parens": [
			"error",
			"always"
		],
		"comma-dangle": [
			"error",
			{
				arrays: "always-multiline",
				objects: "always-multiline",
				functions: "never",
				imports: "never",
				exports: "never"
			}
		],
		"curly": [
			"error"
		],
		"eol-last": [
			"error"
		],
		"indent": [
			"error",
			"tab"
		],
		"linebreak-style": [
			"error",
			"unix"
		],
		"no-buffer-constructor": [
			"error"
		],
		"no-caller": [
			"error"
		],
		"no-console": [
			"error"
		],
		"no-floating-decimal": [
			"error"
		],
		"no-multi-spaces": [
			"error"
		],
		"no-path-concat": [
			"error"
		],
		"no-process-env": [
			"error"
		],
		"no-shadow-restricted-names": [
			"error"
		],
		"no-template-curly-in-string": [
			"error"
		],
		"no-use-before-define": [
			"error",
			{ functions: false, classes: false }
		],
		"no-var": [
			"error"
		],
		"no-warning-comments": [
			"warn",
			{ location: "anywhere" }
		],
		"object-curly-spacing": [
			"error",
			"always"
		],
		"prefer-const": [
			"error"
		],
		"quotes": [
			"error",
			"single"
		],
		"radix": [
			"error"
		],
		"semi": [
			"error",
			"always"
		],
		"strict": [
			"error"
		]
	}
};