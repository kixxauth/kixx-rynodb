'use strict';

const url = require(`url`);
const crypto = require(`crypto`);

function hmac(key, data) {
	const hmac = crypto.createHmac(`sha256`, key);
	hmac.update(data);
	return hmac.digest(`hex`);
}

function hash(data) {
	const hash = crypto.createHash(`sha256`);
	hash.update(data);
	return hash.digest(`hex`);
}

function amzSignatureKey(key, datestamp, region, service) {
	const kDate = exports.sign(`AWS4` + key, datestamp);
	const kRegion = exports.sign(kDate, region);
	const kService = exports.sign(kRegion, service);
	const kSigning = exports.sign(kService, `aws4_request`);
	return kSigning;
}

// - options.region - String
// - options.secretKey - String
// - options.accessKey - String
// - options.endpoint - URL String with protocol, hostname, and path
// - target - String
// - payload - String (JSON)
function amzRequestOptions(options, target, payload) {
	const {region, secretKey, accessKey} = options;
	const endpoint = url.parse(options.endpoint);

	const t = new Date();
	const parts = t.toISOString().split(`.`);
	const amzdate = parts[0].replace(/-|:/g, ``) + `Z`; // '20170630T060649Z'
	const datestamp = parts[0].split(`T`)[0].replace(/-/g, ``);

	const payloadHash = hash(payload);
	const signedHeaders = `host;x-amz-content-sha256;x-amz-date;x-amz-target`;
	const scope = `${datestamp}/${region}/dynamodb/aws4_request`;

	const headers = [
		`host:${endpoint.hostname}`,
		`x-amz-content-sha256:${payloadHash}`,
		`x-amz-date:${amzdate}`,
		`x-amz-target:${target}`
	].join(`\n`);

	const CanonicalString = [
		`POST`,
		`${endpoint.path}\n`,
		`${headers}\n`,
		signedHeaders,
		payloadHash
	].join(`\n`);

	const StringToSign = [
		`AWS4-HMAC-SHA256`,
		amzdate,
		scope,
		hash(CanonicalString)
	].join(`\n`);

	const key = amzSignatureKey(secretKey, datestamp, region, `dynamodb`);
	const signature = hmac(key, StringToSign);

	const Authorization = [
		`AWS4-HMAC-SHA256 Credential=${accessKey}/${scope}`,
		`SignedHeaders=${signedHeaders}`,
		`Signature=${signature}`
	].join(`,`);

	return Object.freeze({
		protocol: endpoint.protocol,
		hostname: endpoint.hostname,
		method: `POST`,
		path: endpoint.path,
		headers: Object.freeze({
			'x-amz-date': amzdate,
			'Content-Type': `application/x-amz-json-1.0`,
			'Content-Length': Buffer.byteLength(payload),
			'Authorization': Authorization,
			'x-amz-target': target,
			'x-amz-content-sha256': payloadHash
		})
	});
}
