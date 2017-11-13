'use strict';

const Promise = require(`bluebird`);
const {omit} = require(`kixx/library`);
const dynalite = require(`dynalite`);

// A DynamoDB http server, optionally backed by LevelDB
//
// Options:
// --port <port>         The port to listen on (default: 4567)
// --path <path>         The path to use for the LevelDB store (in-memory by default)
// --ssl                 Enable SSL for the web server (default: false)
// --createTableMs <ms>  Amount of time tables stay in CREATING state (default: 500)
// --deleteTableMs <ms>  Amount of time tables stay in DELETING state (default: 500)
// --updateTableMs <ms>  Amount of time tables stay in UPDATING state (default: 500)
// --maxItemSizeKb <kb>  Maximum item size (default: 400)
module.exports = function dynaliteServer(options) {
	const port = options.port;
	options = omit([`port`], options);
	const server = dynalite(options);

	return new Promise((resolve, reject) => {
		server.listen(port, (err) => {
			if (err) return reject(err);
			return resolve(server);
		});
	});
};
