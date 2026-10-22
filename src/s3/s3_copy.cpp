#include "s3/s3fs.hpp"

#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

void S3FileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	const auto source_size = OpenFile(source, FileFlags::FILE_FLAGS_READ, opener)->GetFileSize();
	if (source == target) {
		return;
	}

	FileOpenerInfo info = {target};
	auto auth_params = S3AuthResolver::Resolve(opener, info);
	auto parsed_source = S3Url::Parse(source, auth_params);
	const auto &key = parsed_source.GetKey();
	auto copy_source = "/" + S3Url::Encode(parsed_source.GetBucket(), S3URLEncodeMode::PATH) +
	                   (StringUtil::StartsWith(key, "/") ? "" : "/") + S3Url::Encode(key, S3URLEncodeMode::PATH);

	S3RequestSpec spec {target, S3RequestOperation::COPY_OBJECT, {}, "", "", ""};
	spec.extra_headers["x-amz-copy-source"] = copy_source;
	auto session = S3RequestExecutor::CreateSession(opener, target, auth_params);
	auto request_result =
	    S3RequestExecutor::RunSession(GetEncryptionUtil(), *session, spec, [&](S3RequestData &request_data) {
		    auto &params = request_data.http_params->Cast<HTTPFSParams>();
		    return RunPutRequest(request_data.http_url, request_data.headers, params, nullptr, 0, "",
		                         [&](BaseRequest &request) {
			                         request.try_request = true;
			                         return S3RequestExecutor::SendSessionRequest(*session, request_data, request);
		                         });
	    });
	auto &response = request_result.response;
	if (response->HasRequestError()) {
		throw IOException("S3 copy of \"%s\" to \"%s\" could not be completed: %s", source,
		                  request_result.context.display_url, response->GetRequestError());
	}
	if (response->status != HTTPStatusCode::OK_200) {
		throw S3RequestUtil::GetRequestError(request_result.context, *response);
	}
	auto copied = OpenFile(target, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS, opener);
	if (!copied || copied->GetFileSize() != source_size) {
		throw IOException("S3 copy of \"%s\" to \"%s\" did not produce the object; the source is kept", source,
		                  request_result.context.display_url);
	}
	RemoveFile(source, opener);
}

} // namespace duckdb
