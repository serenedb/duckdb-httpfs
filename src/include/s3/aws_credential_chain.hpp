#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"

#include <cstdint>
#include <optional>
#include <string_view>

namespace duckdb {

struct AWSCredentials {
	string access_key_id;
	string secret_access_key;
	string session_token;
	optional_idx expiration;
};

class AWSCredentialChain {
public:
	static constexpr int64_t REFRESH_BEFORE_SECONDS = 5 * 60;

	static AWSCredentialChain &Get();

	shared_ptr<const AWSCredentials> Current();

	static std::optional<AWSCredentials> Environment();
	static std::optional<AWSCredentials> ParseJSON(std::string_view json);
	static std::optional<AWSCredentials> ParseAssumeRoleXML(std::string_view xml);
	static optional_idx ParseExpiration(std::string_view time);

private:
	enum class Source : uint8_t { UNKNOWN, NONE, STATIC, WEB_IDENTITY, CONTAINER, INSTANCE };

	Source Detect();
	AWSCredentials Fetch(Source source);

	mutex lock;
	Source source = Source::UNKNOWN;
	shared_ptr<const AWSCredentials> current;
};

} // namespace duckdb
