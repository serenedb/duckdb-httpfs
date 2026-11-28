#include "s3/aws_credential_chain.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

#include <curl/curl.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <string>

namespace duckdb {

namespace {

constexpr const char *CONTAINER_HOST = "http://169.254.170.2";
constexpr const char *INSTANCE_HOST = "http://169.254.169.254";
constexpr long REQUEST_TIMEOUT_MS = 10000;
constexpr long INSTANCE_PROBE_TIMEOUT_MS = 1000;

string Env(const char *name) {
	const char *value = std::getenv(name);
	return value ? string(value) : string();
}

string ReadToken(const string &path) {
	std::ifstream in(path, std::ios::binary);
	if (!in) {
		throw IOException("Could not read the AWS token file \"%s\"", path);
	}
	std::stringstream content;
	content << in.rdbuf();
	auto token = content.str();
	StringUtil::Trim(token);
	return token;
}

struct Response {
	long status = 0;
	string body;
};

size_t AppendBody(char *data, size_t size, size_t count, void *out) {
	static_cast<string *>(out)->append(data, size * count);
	return size * count;
}

Response Call(const char *method, const string &url, const vector<string> &headers, long timeout_ms) {
	unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), &curl_easy_cleanup);
	if (!curl) {
		throw IOException("Could not start a request to %s", url);
	}
	Response response;
	curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT_MS, timeout_ms);
	curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT_MS, timeout_ms);
	curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, &AppendBody);
	curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response.body);
	if (std::strcmp(method, "PUT") == 0) {
		curl_easy_setopt(curl.get(), CURLOPT_CUSTOMREQUEST, method);
		curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, "");
		curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE, 0L);
	}
	curl_slist *list = nullptr;
	for (auto &header : headers) {
		list = curl_slist_append(list, header.c_str());
	}
	unique_ptr<curl_slist, decltype(&curl_slist_free_all)> header_list(list, &curl_slist_free_all);
	if (list) {
		curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, list);
	}
	auto code = curl_easy_perform(curl.get());
	if (code != CURLE_OK) {
		throw IOException("%s %s: %s", method, url, curl_easy_strerror(code));
	}
	curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &response.status);
	if (response.status != 200) {
		throw IOException("%s %s: HTTP %d %s", method, url, response.status, response.body.substr(0, 200));
	}
	return response;
}

std::optional<string> JSONString(std::string_view json, std::string_view key) {
	auto quoted = "\"" + string(key) + "\"";
	auto at = json.find(quoted);
	if (at == std::string_view::npos) {
		return std::nullopt;
	}
	at += quoted.size();
	while (at < json.size() && (json[at] == ' ' || json[at] == '\t' || json[at] == '\n' || json[at] == '\r')) {
		at++;
	}
	if (at >= json.size() || json[at] != ':') {
		return std::nullopt;
	}
	at++;
	while (at < json.size() && (json[at] == ' ' || json[at] == '\t' || json[at] == '\n' || json[at] == '\r')) {
		at++;
	}
	if (at >= json.size() || json[at] != '"') {
		return std::nullopt;
	}
	string value;
	for (at++; at < json.size(); at++) {
		auto c = json[at];
		if (c == '"') {
			return value;
		}
		if (c != '\\') {
			value += c;
			continue;
		}
		if (++at >= json.size()) {
			return std::nullopt;
		}
		switch (json[at]) {
		case 'n':
			value += '\n';
			break;
		case 't':
			value += '\t';
			break;
		case 'r':
			value += '\r';
			break;
		case 'b':
			value += '\b';
			break;
		case 'f':
			value += '\f';
			break;
		case 'u': {
			if (at + 4 >= json.size()) {
				return std::nullopt;
			}
			auto code = std::stoul(string(json.substr(at + 1, 4)), nullptr, 16);
			if (code > 0x7F) {
				return std::nullopt;
			}
			value += static_cast<char>(code);
			at += 4;
			break;
		}
		default:
			value += json[at];
		}
	}
	return std::nullopt;
}

std::optional<string> XMLText(std::string_view xml, std::string_view tag) {
	auto open = "<" + string(tag) + ">";
	auto close = "</" + string(tag) + ">";
	auto begin = xml.find(open);
	if (begin == std::string_view::npos) {
		return std::nullopt;
	}
	begin += open.size();
	auto end = xml.find(close, begin);
	if (end == std::string_view::npos) {
		return std::nullopt;
	}
	auto text = string(xml.substr(begin, end - begin));
	text = StringUtil::Replace(text, "&lt;", "<");
	text = StringUtil::Replace(text, "&gt;", ">");
	text = StringUtil::Replace(text, "&quot;", "\"");
	text = StringUtil::Replace(text, "&apos;", "'");
	return StringUtil::Replace(text, "&amp;", "&");
}

string PercentEncode(std::string_view value) {
	static constexpr const char *HEX = "0123456789ABCDEF";
	string encoded;
	for (unsigned char c : value) {
		if (StringUtil::CharacterIsAlphaNumeric(static_cast<char>(c)) || c == '-' || c == '_' || c == '.' || c == '~') {
			encoded += static_cast<char>(c);
		} else {
			encoded += '%';
			encoded += HEX[c >> 4];
			encoded += HEX[c & 15];
		}
	}
	return encoded;
}

int64_t DaysFromCivil(int64_t y, int64_t m, int64_t d) {
	y -= m <= 2;
	const int64_t era = (y >= 0 ? y : y - 399) / 400;
	const int64_t yoe = y - era * 400;
	const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
	const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	return era * 146097 + doe - 719468;
}

int64_t NowSeconds() {
	return Timestamp::GetEpochSeconds(Timestamp::GetCurrentTimestamp());
}

AWSCredentials Require(std::optional<AWSCredentials> credentials, const string &source) {
	if (!credentials) {
		throw IOException("The AWS credentials from %s lack AccessKeyId or SecretAccessKey", source);
	}
	return std::move(*credentials);
}

} // namespace

AWSCredentialChain &AWSCredentialChain::Get() {
	static AWSCredentialChain chain;
	return chain;
}

optional_idx AWSCredentialChain::ParseExpiration(std::string_view time) {
	int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
	string text(time);
	if (std::sscanf(text.c_str(), "%4d-%2d-%2dT%2d:%2d:%2d", &year, &month, &day, &hour, &minute, &second) != 6) {
		return optional_idx();
	}
	int64_t seconds = DaysFromCivil(year, month, day) * 86400 + hour * 3600 + minute * 60 + second;
	auto zone = text.find_first_of("Z+-", 19);
	if (zone != string::npos && text[zone] != 'Z') {
		int offset_hours = 0, offset_minutes = 0;
		if (std::sscanf(text.c_str() + zone + 1, "%2d:%2d", &offset_hours, &offset_minutes) != 2) {
			return optional_idx();
		}
		auto offset = offset_hours * 3600 + offset_minutes * 60;
		seconds += text[zone] == '+' ? -offset : offset;
	}
	if (seconds < 0) {
		return optional_idx();
	}
	return optional_idx(static_cast<idx_t>(seconds));
}

std::optional<AWSCredentials> AWSCredentialChain::ParseJSON(std::string_view json) {
	auto key = JSONString(json, "AccessKeyId");
	auto secret = JSONString(json, "SecretAccessKey");
	if (!key || !secret || key->empty() || secret->empty()) {
		return std::nullopt;
	}
	AWSCredentials credentials {std::move(*key), std::move(*secret), JSONString(json, "Token").value_or(""), {}};
	if (auto expiration = JSONString(json, "Expiration")) {
		credentials.expiration = ParseExpiration(*expiration);
	}
	return credentials;
}

std::optional<AWSCredentials> AWSCredentialChain::ParseAssumeRoleXML(std::string_view xml) {
	auto key = XMLText(xml, "AccessKeyId");
	auto secret = XMLText(xml, "SecretAccessKey");
	if (!key || !secret || key->empty() || secret->empty()) {
		return std::nullopt;
	}
	AWSCredentials credentials {std::move(*key), std::move(*secret), XMLText(xml, "SessionToken").value_or(""), {}};
	if (auto expiration = XMLText(xml, "Expiration")) {
		credentials.expiration = ParseExpiration(*expiration);
	}
	return credentials;
}

std::optional<AWSCredentials> AWSCredentialChain::Environment() {
	auto key = Env("AWS_ACCESS_KEY_ID");
	if (key.empty()) {
		return std::nullopt;
	}
	return AWSCredentials {std::move(key), Env("AWS_SECRET_ACCESS_KEY"), Env("AWS_SESSION_TOKEN"), {}};
}

AWSCredentialChain::Source AWSCredentialChain::Detect() {
	if (Environment()) {
		return Source::STATIC;
	}
	if (!Env("AWS_WEB_IDENTITY_TOKEN_FILE").empty() && !Env("AWS_ROLE_ARN").empty()) {
		return Source::WEB_IDENTITY;
	}
	if (!Env("AWS_CONTAINER_CREDENTIALS_FULL_URI").empty() || !Env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").empty()) {
		return Source::CONTAINER;
	}
	if (StringUtil::Lower(Env("AWS_EC2_METADATA_DISABLED")) == "true") {
		return Source::NONE;
	}
	try {
		current = make_shared_ptr<const AWSCredentials>(Fetch(Source::INSTANCE));
		return Source::INSTANCE;
	} catch (std::exception &) {
		return Source::NONE;
	}
}

AWSCredentials AWSCredentialChain::Fetch(Source source) {
	switch (source) {
	case Source::STATIC:
		return Environment().value_or(AWSCredentials {});
	case Source::CONTAINER: {
		auto url = Env("AWS_CONTAINER_CREDENTIALS_FULL_URI");
		if (url.empty()) {
			url = CONTAINER_HOST + Env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
		}
		auto authorization = Env("AWS_CONTAINER_AUTHORIZATION_TOKEN");
		if (auto file = Env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"); !file.empty()) {
			authorization = ReadToken(file);
		}
		vector<string> headers;
		if (!authorization.empty()) {
			headers.push_back("Authorization: " + authorization);
		}
		return Require(ParseJSON(Call("GET", url, headers, REQUEST_TIMEOUT_MS).body), url);
	}
	case Source::WEB_IDENTITY: {
		auto region = Env("AWS_REGION");
		if (region.empty()) {
			region = Env("AWS_DEFAULT_REGION");
		}
		auto session = Env("AWS_ROLE_SESSION_NAME");
		if (session.empty()) {
			std::random_device random;
			session = StringUtil::Format("serenedb-%08x%08x", random(), random());
		}
		auto host = region.empty() ? string("https://sts.amazonaws.com") : "https://sts." + region + ".amazonaws.com";
		auto url = host + "/?Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn=" +
		           PercentEncode(Env("AWS_ROLE_ARN")) + "&RoleSessionName=" + PercentEncode(session) +
		           "&WebIdentityToken=" + PercentEncode(ReadToken(Env("AWS_WEB_IDENTITY_TOKEN_FILE")));
		return Require(ParseAssumeRoleXML(Call("GET", url, {}, REQUEST_TIMEOUT_MS).body), host);
	}
	case Source::INSTANCE: {
		auto token = Call("PUT", string(INSTANCE_HOST) + "/latest/api/token",
		                  {"X-aws-ec2-metadata-token-ttl-seconds: 21600"}, INSTANCE_PROBE_TIMEOUT_MS)
		                 .body;
		vector<string> headers {"X-aws-ec2-metadata-token: " + token};
		auto roles_url = string(INSTANCE_HOST) + "/latest/meta-data/iam/security-credentials/";
		auto roles = Call("GET", roles_url, headers, REQUEST_TIMEOUT_MS).body;
		auto role = roles.substr(0, roles.find('\n'));
		StringUtil::Trim(role);
		if (role.empty()) {
			throw IOException("The EC2 instance has no IAM role");
		}
		return Require(ParseJSON(Call("GET", roles_url + role, headers, REQUEST_TIMEOUT_MS).body), roles_url);
	}
	default:
		throw InternalException("No AWS credential source to fetch from");
	}
}

shared_ptr<const AWSCredentials> AWSCredentialChain::Current() {
	lock_guard<mutex> guard(lock);
	if (source == Source::UNKNOWN) {
		source = Detect();
	}
	if (source == Source::NONE) {
		return nullptr;
	}
	auto now = NowSeconds();
	if (current && (!current->expiration.IsValid() ||
	                now + REFRESH_BEFORE_SECONDS < static_cast<int64_t>(current->expiration.GetIndex()))) {
		return current;
	}
	try {
		current = make_shared_ptr<const AWSCredentials>(Fetch(source));
	} catch (std::exception &) {
		if (current && current->expiration.IsValid() && now < static_cast<int64_t>(current->expiration.GetIndex())) {
			return current;
		}
		throw;
	}
	return current;
}

} // namespace duckdb
