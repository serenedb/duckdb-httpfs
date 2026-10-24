#include "s3/s3fs.hpp"

#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"
#include "s3/s3_xml_response.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

void S3FileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	if (source == target) {
		OpenFile(source, FileFlags::FILE_FLAGS_READ, opener);
		return;
	}

	FileOpenerInfo info = {target};
	auto auth_params = S3AuthResolver::Resolve(opener, info);
	auto parsed_source = S3Url::Parse(source, auth_params);
	const auto &key = parsed_source.GetKey();
	HTTPHeaders headers;
	headers["x-amz-copy-source"] = "/" + S3Url::Encode(parsed_source.GetBucket(), S3URLEncodeMode::PATH) +
	                               (StringUtil::StartsWith(key, "/") ? "" : "/") +
	                               S3Url::Encode(key, S3URLEncodeMode::PATH);
	auto session = S3RequestExecutor::CreateSession(opener, target, auth_params);
	auto request_result =
	    PutRequest(*session, S3RequestOperation::COPY_OBJECT, target, nullptr, 0, S3RequestQuery(), std::move(headers));
	auto &response = request_result.response;
	if (response->HasRequestError()) {
		throw IOException("S3 copy of \"%s\" to \"%s\" could not be completed: %s", source,
		                  request_result.context.display_url, response->GetRequestError());
	}
	if (response->status != HTTPStatusCode::OK_200) {
		throw S3RequestUtil::GetRequestError(request_result.context, *response);
	}
	EraseGlobalCacheEntry(target);
	S3XMLResponse copy_result;
	if (!S3XMLResponseParser::TryParse(response->body, copy_result)) {
		throw IOException("S3 copy of \"%s\" to \"%s\" returned malformed XML, so it is not known to have completed; "
		                  "the source is kept",
		                  source, request_result.context.display_url);
	}
	if (copy_result.type == S3XMLResponseType::ERROR) {
		throw HTTPException(*response, "S3 copy of \"%s\" to \"%s\" failed: %s%s%s; the source is kept", source,
		                    request_result.context.display_url,
		                    copy_result.error_code.empty() ? "S3 returned an embedded error" : copy_result.error_code,
		                    copy_result.error_message.empty() ? "" : ": ", copy_result.error_message);
	}
	RemoveFile(source, opener);
}

} // namespace duckdb
