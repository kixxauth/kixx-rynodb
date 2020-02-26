module.exports = {
	"env": {
		"es6": true,
		"node": true
	},
	"extends": "eslint:recommended",
	"parserOptions": {
		"ecmaVersion": 2015
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
		"arrow-body-style": [
			"error",
			"always"
		],
		"arrow-parens": [
			"error",
			"always"
		],
		"arrow-spacing": [
			"error"
		],
		"block-spacing": [
			"error"
		],
		"brace-style": [
			"error",
			"stroustrup"
		],
		"callback-return": [
			"error"
		],
		"camelcase": [
			"error"
		],
		"capitalized-comments": [
			"error",
			"always",
			{ ignoreConsecutiveComments: true }
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
		"comma-spacing": [
			"error"
		],
		"comma-style": [
			"error"
		],
		"curly": [
			"error"
		],
		"eol-last": [
			"error"
		],
		"eqeqeq": [
			"error"
		],
		"indent": [
			"error",
			"tab",
			{ SwitchCase: 1 }
		],
		"keyword-spacing": [
			"error"
		],
		"linebreak-style": [
			"error",
			"unix"
		],
		"max-len": [
			"error",
			{ tabWidth: 2, code: 120, comments: 100 }
		],
		"multiline-ternary": [
			"error",
			"always-multiline"
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
		"no-else-return": [
			"error"
		],
		"no-eval": [
			"error"
		],
		"no-extend-native": [
			"error"
		],
		"no-floating-decimal": [
			"error"
		],
		"no-implicit-coercion": [
			"error"
		],
		"no-implied-eval": [
			"error"
		],
		"no-multi-assign": [
			"error"
		],
		"no-multi-spaces": [
			"error"
		],
		"no-nested-ternary": [
			"error"
		],
		"no-new-wrappers": [
			"error"
		],
		"no-path-concat": [
			"error"
		],
		"no-plusplus": [
			"error"
		],
		"no-process-env": [
			"error"
		],
		"no-return-assign": [
			"error"
		],
		// "no-restricted-syntax": [
		// 	"error",
		// 	""
		// ],
		"no-sequences": [
			"error"
		],
		"no-shadow": [
			"error",
			{ builtinGlobals: true, hoist: "all" }
		],
		"no-shadow-restricted-names": [
			"error"
		],
		"no-template-curly-in-string": [
			"error"
		],
		"no-throw-literal": [
			"error"
		],
		"no-trailing-spaces": [
			"error"
		],
		"no-underscore-dangle": [
			"error"
		],
		"no-unmodified-loop-condition": [
			"error"
		],
		"no-unused-expressions": [
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
		"no-whitespace-before-property": [
			"error"
		],
		"object-curly-spacing": [
			"error",
			"always"
		],
		"operator-assignment": [
			"error",
			"never"
		],
		"operator-linebreak": [
			"error",
			"before"
		],
		"prefer-const": [
			"error"
		],
		"prefer-destructuring": [
			"error"
		],
		"prefer-promise-reject-errors": [
			"error"
		],
		"quotes": [
			"error",
			"single",
			{ avoidEscape: true }
		],
		"radix": [
			"error"
		],
		"semi": [
			"error",
			"always"
		],
		"spaced-comment": [
			"error"
		],
		"space-before-blocks": [
			"error"
		],
		"space-before-function-paren": [
			"error",
			{ anonymous: "always", named: "never", asyncArrow: "never" }
		],
		"space-in-parens": [
			"error"
		],
		"space-infix-ops": [
			"error"
		],
		"strict": [
			"error"
		]
	}
};