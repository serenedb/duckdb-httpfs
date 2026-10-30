#include "s3/s3_upload_session.hpp"

#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"
#include "s3/s3_xml_response.hpp"
#include "s3/s3fs.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <cstring>

namespace duckdb {

namespace {

//! The queue of the parts sent on the async threads: an object of the database, which goes before its scheduler
class S3UploadProducer : public ObjectCacheEntry {
public:
	explicit S3UploadProducer(TaskScheduler &scheduler) : token(scheduler.CreateProducer()) {
	}

	static string ObjectType() {
		return "s3_upload_producer";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

	const unique_ptr<ProducerToken> token;
};

ProducerToken &UploadProducer(DatabaseInstance &db) {
	auto producer = db.GetObjectCache().GetOrCreate<S3UploadProducer>(S3UploadProducer::ObjectType(),
	                                                                  TaskScheduler::GetScheduler(db));
	return *producer->token;
}

class S3AmbiguousUploadException : public IOException {
public:
	explicit S3AmbiguousUploadException(const string &message) : IOException(message) {
	}
};

static bool IsSuccessfulStatus(HTTPStatusCode status) {
	auto status_code = static_cast<int>(status);
	return status_code >= 200 && status_code < 300;
}

static string EncodeChecksum(absl::crc32c_t checksum) {
	const auto value = BSwapIfLE(static_cast<uint32_t>(checksum));
	return Blob::ToBase64(string_t(const_char_ptr_cast(&value), sizeof(value)));
}

static void VerifyCreated(const S3UploadConfig &config, const HTTPResponse &response, const string &display_url) {
	if (config.exclusive && response.status == HTTPStatusCode::PreconditionFailed_412) {
		throw IOException("Cannot create file \"%s\": File exists", display_url);
	}
}

} // namespace

class S3UploadSession::PartUploadTask : public Task {
public:
	PartUploadTask(S3UploadSession &session_p, PreparedPart part_p) : session(session_p), part(std::move(part_p)) {
	}

	~PartUploadTask() override {
		if (ran) {
			return;
		}
		try {
			session.LatchFailure(ErrorData(IOException("S3 upload of part %llu of \"%s\" was dropped before it ran",
			                                           part.part_number, session.GetDisplayPath())),
			                     FailureDisposition::DEFINITIVE);
		} catch (...) { // NOLINT
		}
		session.FinishBackgroundUpload();
	}

	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		ran = true;
		session.UploadInBackground(part);
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "S3PartUploadTask";
	}

private:
	S3UploadSession &session;
	PreparedPart part;
	bool ran = false;
};

S3UploadSession::WriteClaim::WriteClaim(S3UploadSession &session_p) : session(session_p) {
}

S3UploadSession::WriteClaim::WriteClaim(WriteClaim &&other) noexcept
    : session(other.session), finished(other.finished) {
	other.finished = true;
}

S3UploadSession::WriteClaim::~WriteClaim() {
	if (!finished) {
		session.get().ReleaseWriteNoThrow();
	}
}

void S3UploadSession::WriteClaim::Finish() {
	if (finished) {
		return;
	}
	finished = true;
	session.get().ReleaseWrite();
}

void S3UploadSession::WriteClaim::Fail(ErrorData error, FailureDisposition disposition) {
	D_ASSERT(!finished);
	finished = true;
	session.get().FailOperation(std::move(error), disposition);
}

S3UploadSession::S3UploadSession(S3FileSystem &s3fs_p, shared_ptr<HTTPRequestSession> request_session_p, string path_p,
                                 S3UploadConfig config_p)
    : s3fs(s3fs_p), request_session(std::move(request_session_p)), path(std::move(path_p)), config(config_p) {
}

S3UploadSession::~S3UploadSession() noexcept {
	AwaitBackgroundUploads();
}

S3UploadSession::FailureSnapshot S3UploadSession::CaptureFailure() const DUCKDB_REQUIRES(state_lock) {
	D_ASSERT(primary_failure.primary_error);
	return primary_failure;
}

void S3UploadSession::ThrowFailure(const FailureSnapshot &failure) {
	D_ASSERT(failure.primary_error);
	if (!failure.abort_error) {
		failure.primary_error->Throw();
	}
	throw Exception(failure.primary_error->ExtraInfo(), failure.primary_error->Type(),
	                failure.primary_error->RawMessage() + "\n\nAdditionally, " + failure.abort_error->RawMessage());
}

void S3UploadSession::BeginWriteOperation() DUCKDB_EXCLUDES(state_lock) {
	FailureSnapshot failure;
	const char *error_message = nullptr;
	{
		annotated_unique_lock<annotated_mutex> guard(state_lock);
		auto settled = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return !primary_failure.primary_error || cleanup_state == CleanupState::COMPLETE;
		};
		state_lock.Await(absl::Condition(&settled));
		if (primary_failure.primary_error) {
			failure = CaptureFailure();
		} else if (lifecycle_state == LifecycleState::ABORTING || lifecycle_state == LifecycleState::ABORTED) {
			error_message = "Cannot write to an aborted S3 upload";
		} else if (lifecycle_state == LifecycleState::FINALIZING) {
			error_message = "Concurrent S3 upload operations are not supported";
		} else if (lifecycle_state == LifecycleState::FINALIZED) {
			error_message = "Cannot write to a finalized S3 upload";
		} else {
			D_ASSERT(lifecycle_state == LifecycleState::ACTIVE);
			active_operations++;
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
	if (error_message) {
		throw IOException(error_message);
	}
}

unique_ptr<S3UploadSession::BufferedPart> S3UploadSession::BeginFinalize(bool &already_finalized)
    DUCKDB_EXCLUDES(state_lock) {
	const char *error_message = nullptr;
	FailureSnapshot failure;
	unique_ptr<BufferedPart> result;
	already_finalized = false;
	{
		annotated_unique_lock<annotated_mutex> guard(state_lock);
		auto settled = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return !primary_failure.primary_error || cleanup_state == CleanupState::COMPLETE;
		};
		state_lock.Await(absl::Condition(&settled));
		if (primary_failure.primary_error) {
			failure = CaptureFailure();
		} else if (lifecycle_state == LifecycleState::ABORTING || lifecycle_state == LifecycleState::ABORTED) {
			error_message = "Cannot finalize an aborted S3 upload";
		} else if (lifecycle_state == LifecycleState::FINALIZING || active_operations > 0) {
			error_message = "Concurrent S3 upload operations are not supported";
		} else if (lifecycle_state == LifecycleState::FINALIZED) {
			already_finalized = true;
		} else {
			D_ASSERT(lifecycle_state == LifecycleState::ACTIVE);
			lifecycle_state = LifecycleState::FINALIZING;
			active_operations++;
			result = std::move(buffered_part);
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
	if (error_message) {
		throw IOException(error_message);
	}
	return result;
}

void S3UploadSession::ReleaseWrite() DUCKDB_EXCLUDES(state_lock) {
	ReleaseOperation();
}

void S3UploadSession::ReleaseWriteNoThrow() noexcept DUCKDB_EXCLUDES(state_lock) {
	try {
		ReleaseOperation();
	} catch (...) {
	}
}

void S3UploadSession::FinishFinalize() DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(lifecycle_state == LifecycleState::FINALIZING);
	D_ASSERT(active_operations == 1);
	active_operations--;
	lifecycle_state = LifecycleState::FINALIZED;
}

void S3UploadSession::LatchFailureLocked(shared_ptr<const ErrorData> error, FailureDisposition disposition)
    DUCKDB_REQUIRES(state_lock) {
	if (!primary_failure.primary_error) {
		primary_failure.primary_error = std::move(error);
		failure_disposition = disposition;
	}
	if (disposition == FailureDisposition::AMBIGUOUS) {
		abort_suppressed = true;
	}
	if (lifecycle_state == LifecycleState::FINALIZING) {
		lifecycle_state = LifecycleState::ACTIVE;
	}
}

void S3UploadSession::LatchFailure(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock) {
	auto stored_error = make_shared_ptr<const ErrorData>(std::move(error));
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		LatchFailureLocked(std::move(stored_error), disposition);
	}
}

void S3UploadSession::ReleaseOperation() DUCKDB_EXCLUDES(state_lock) {
	shared_ptr<const string> upload_id;
	FailureSnapshot failure;
	{
		annotated_unique_lock<annotated_mutex> guard(state_lock);
		D_ASSERT(active_operations > 0);
		active_operations--;
		if (!primary_failure.primary_error) {
			return;
		}

		upload_id = ClaimCleanup();
		auto cleaned_up = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return upload_id || cleanup_state == CleanupState::COMPLETE;
		};
		if (!cleaned_up()) {
			TaskScheduler::BlockingScope blocking;
			state_lock.Await(absl::Condition(&cleaned_up));
		}
		if (!upload_id) {
			failure = CaptureFailure();
		}
	}

	if (upload_id) {
		RunCleanup(*upload_id);
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		failure = CaptureFailure();
	}
	ThrowFailure(failure);
}

shared_ptr<const string> S3UploadSession::ClaimCleanup() DUCKDB_REQUIRES(state_lock) {
	if (!primary_failure.primary_error || active_operations != 0 || parts_in_flight != 0 ||
	    cleanup_state != CleanupState::NONE) {
		return nullptr;
	}
	if (failure_disposition != FailureDisposition::DEFINITIVE || !multipart_upload_id || abort_suppressed) {
		cleanup_state = CleanupState::COMPLETE;
		return nullptr;
	}
	cleanup_state = CleanupState::IN_PROGRESS;
	return multipart_upload_id;
}

void S3UploadSession::RunCleanup(const string &upload_id) noexcept DUCKDB_EXCLUDES(state_lock) {
	shared_ptr<const ErrorData> abort_error;
	try {
		abort_error = AbortMultipartUpload(upload_id);
	} catch (...) { // NOLINT
	}
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	if (abort_error && !primary_failure.abort_error) {
		primary_failure.abort_error = std::move(abort_error);
	}
	cleanup_state = CleanupState::COMPLETE;
}

void S3UploadSession::FailOperation(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock) {
	LatchFailure(std::move(error), disposition);
	ReleaseOperation();
	throw InternalException("Failed S3 operation did not rethrow its error");
}

shared_ptr<const ErrorData> S3UploadSession::AbortMultipartUpload(const string &upload_id) {
	S3RequestQuery query {{"uploadId", upload_id}};
	try {
		auto request_result =
		    s3fs.get().DeleteRequest(*request_session, S3RequestOperation::ABORT_MULTIPART_UPLOAD, path, query);
		auto &response = request_result.response;
		if (response->status == HTTPStatusCode::NoContent_204) {
			return nullptr;
		}
		auto status_error = ErrorData(GetStatusError(*response, request_result.context));
		auto contextual_error = Exception(status_error.ExtraInfo(), status_error.Type(),
		                                  "Failed to abort S3 multipart upload: " + status_error.RawMessage());
		return make_shared_ptr<const ErrorData>(contextual_error);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		auto message =
		    StringUtil::Format("Failed to abort S3 multipart upload for \"%s\": the cleanup request could not "
		                       "be completed",
		                       GetDisplayPath());
		return make_shared_ptr<const ErrorData>(error.Type(), std::move(message));
	}
}

unique_ptr<S3UploadSession::BufferedPart> S3UploadSession::AllocateBufferedPart(idx_t capacity) {
	auto buffer = s3fs.get().GetBufferManager().Allocate(MemoryTag::EXTENSION, capacity);
	return make_uniq<BufferedPart>(std::move(buffer), capacity);
}

idx_t S3UploadSession::AppendToBufferedPart(BufferedPart &buffered_part_p, const_data_ptr_t data, idx_t size) {
	D_ASSERT(buffered_part_p.size < buffered_part_p.capacity);
	auto copy_size = MinValue<idx_t>(size, buffered_part_p.capacity - buffered_part_p.size);
	memcpy(buffered_part_p.Ptr() + buffered_part_p.size, data, copy_size);
	buffered_part_p.size += copy_size;
	return copy_size;
}

void S3UploadSession::ReservePart(PreparedWrite &write, const_data_ptr_t data, idx_t size) DUCKDB_REQUIRES(state_lock) {
	if (!config.HasPartCapacity(upload_parts.size())) {
		throw IOException("S3 upload exceeds the configured maximum of %llu multipart parts", config.max_parts);
	}
	auto part_number = upload_parts.size() + 1;
	upload_parts.emplace_back();
	write.parts.emplace_back(part_number, data, size);
}

void S3UploadSession::ReservePart(PreparedWrite &write, unique_ptr<BufferedPart> buffered_part_p)
    DUCKDB_REQUIRES(state_lock) {
	if (!config.HasPartCapacity(upload_parts.size())) {
		throw IOException("S3 upload exceeds the configured maximum of %llu multipart parts", config.max_parts);
	}
	auto part_number = upload_parts.size() + 1;
	upload_parts.emplace_back();
	write.parts.emplace_back(part_number, std::move(buffered_part_p));
}

S3UploadSession::PreparedWrite S3UploadSession::PrepareWrite(const_data_ptr_t data, idx_t size, idx_t location)
    DUCKDB_EXCLUDES(state_lock) {
	absl::crc32c_t write_checksum {0};
	if (config.checksum) {
		write_checksum = absl::ComputeCrc32c(absl::string_view(const_char_ptr_cast(data), size));
	}
	FailureSnapshot failure;
	PreparedWrite result;
	{
		annotated_unique_lock<annotated_mutex> guard(state_lock);
		auto ready = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return location <= next_offset || primary_failure.primary_error;
		};
		state_lock.Await(absl::Condition(&ready));
		if (primary_failure.primary_error) {
			failure = CaptureFailure();
		} else if (location != next_offset) {
			throw IOException("S3 writes must be sequential: expected offset %llu, got %llu", next_offset, location);
		} else if (size > config.max_file_size - next_offset) {
			throw IOException("S3 upload exceeds the configured maximum file size of %llu bytes", config.max_file_size);
		} else {
			if (config.checksum) {
				checksum = absl::ConcatCrc32c(checksum, write_checksum, size);
			}
			result.buffered_part = std::move(buffered_part);
			idx_t input_offset = 0;
			if (result.buffered_part && size > 0) {
				if (result.buffered_part->size == result.buffered_part->capacity) {
					ReservePart(result, std::move(result.buffered_part));
				} else {
					input_offset = AppendToBufferedPart(*result.buffered_part, data, size);
					if (result.buffered_part->size == result.buffered_part->capacity && input_offset < size) {
						ReservePart(result, std::move(result.buffered_part));
					}
				}
			}

			while (input_offset < size) {
				auto remaining = size - input_offset;
				auto part_size = config.TargetPartSize(upload_parts.size());
				auto should_buffer = upload_parts.empty() ? remaining <= part_size : remaining < part_size;
				if (should_buffer) {
					result.buffered_part = AllocateBufferedPart(part_size);
					auto copied = AppendToBufferedPart(*result.buffered_part, data + input_offset, remaining);
					D_ASSERT(copied == remaining);
					input_offset += copied;
					break;
				}
				auto direct_size = config.DirectPartSize(upload_parts.size(), remaining);
				ReservePart(result, data + input_offset, direct_size);
				input_offset += direct_size;
			}

			buffered_part = std::move(result.buffered_part);
			next_offset += size;
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
	return result;
}

string S3UploadSession::InitializeMultipartUpload() {
	string result;
	HTTPHeaders headers;
	if (config.checksum) {
		headers["x-amz-checksum-algorithm"] = "CRC32C";
		headers["x-amz-checksum-type"] = "FULL_OBJECT";
	}
	auto request_result =
	    s3fs.get().PostRequest(*request_session, S3RequestOperation::CREATE_MULTIPART_UPLOAD, path, result, nullptr, 0,
	                           S3RequestQuery({{"uploads", ""}}), std::move(headers));
	auto &response = request_result.response;
	auto &request_context = request_result.context;
	if (response->HasRequestError()) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload initialization for \"%s\" has an unknown outcome because the response was not "
		    "received; the request was not retried and cannot be aborted without an upload ID",
		    request_context.display_url));
	}
	if (!IsSuccessfulStatus(response->status)) {
		throw GetStatusError(*response, request_context);
	}

	S3XMLResponse parsed_response;
	if (!S3XMLResponseParser::TryParse(result, parsed_response)) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload initialization for \"%s\" returned malformed XML; the request was not retried and "
		    "cannot be aborted without an upload ID",
		    request_context.display_url));
	}
	if (parsed_response.type != S3XMLResponseType::MULTIPART_INITIALIZATION) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload initialization for \"%s\" returned an unrecognized response; the request was not "
		    "retried and cannot be aborted without an upload ID",
		    request_context.display_url));
	}
	return parsed_response.upload_id;
}

void S3UploadSession::PublishInitializationFailure(ErrorData error, FailureDisposition disposition)
    DUCKDB_EXCLUDES(state_lock) {
	auto stored_error = make_shared_ptr<const ErrorData>(std::move(error));
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(initialization_state == InitializationState::IN_PROGRESS);
	initialization_state = InitializationState::FAILED;
	LatchFailureLocked(std::move(stored_error), disposition);
}

shared_ptr<const string> S3UploadSession::EnsureMultipartUpload() {
	bool initialize = false;
	FailureSnapshot failure;
	{
		annotated_unique_lock<annotated_mutex> guard(state_lock);
		auto initialized = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return initialization_state != InitializationState::IN_PROGRESS || primary_failure.primary_error;
		};
		state_lock.Await(absl::Condition(&initialized));
		if (primary_failure.primary_error) {
			failure = CaptureFailure();
		} else if (initialization_state == InitializationState::SUCCEEDED) {
			return multipart_upload_id;
		} else if (initialization_state == InitializationState::FAILED) {
			failure = CaptureFailure();
		} else {
			D_ASSERT(initialization_state == InitializationState::NOT_STARTED);
			initialization_state = InitializationState::IN_PROGRESS;
			initialize = true;
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
	D_ASSERT(initialize);

	shared_ptr<const string> upload_id;
	try {
		upload_id = make_shared_ptr<const string>(InitializeMultipartUpload());
	} catch (S3AmbiguousUploadException &ex) {
		PublishInitializationFailure(ErrorData(ex), FailureDisposition::AMBIGUOUS);
		throw;
	} catch (std::exception &ex) {
		PublishInitializationFailure(ErrorData(ex), FailureDisposition::DEFINITIVE);
		throw;
	}

	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		D_ASSERT(initialization_state == InitializationState::IN_PROGRESS);
		D_ASSERT(!multipart_upload_id);
		multipart_upload_id = upload_id;
		initialization_state = InitializationState::SUCCEEDED;
		if (primary_failure.primary_error) {
			failure = CaptureFailure();
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
	return upload_id;
}

void S3UploadSession::ThrowIfFailed() DUCKDB_EXCLUDES(state_lock) {
	FailureSnapshot failure;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (primary_failure.primary_error) {
			failure = CaptureFailure();
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
}

S3RequestResult S3UploadSession::RunUploadRequest(S3RequestOperation operation, const_data_ptr_t data, idx_t size,
                                                  const S3RequestQuery &query, HTTPHeaders headers) {
	auto result = s3fs.get().PutRequest(*request_session, operation, path, data, size, query, std::move(headers));
	if (result.response->HasRequestError()) {
		throw IOException("S3 upload request for \"%s\" could not be completed: %s", result.context.display_url,
		                  result.response->GetRequestError());
	}
	return result;
}

string S3UploadSession::UploadObject(const_data_ptr_t data, idx_t size) {
	HTTPHeaders headers;
	if (config.checksum) {
		headers["x-amz-checksum-crc32c"] = EncodeChecksum(Checksum());
	}
	if (config.exclusive) {
		headers["If-None-Match"] = "*";
	}
	auto request_result =
	    RunUploadRequest(S3RequestOperation::PUT_OBJECT, data, size, S3RequestQuery(), std::move(headers));
	auto &response = request_result.response;
	VerifyCreated(config, *response, request_result.context.display_url);
	if (response->status != HTTPStatusCode::OK_200 && response->status != HTTPStatusCode::Created_201) {
		throw GetStatusError(*response, request_result.context);
	}
	if (!response->headers.HasHeader("ETag")) {
		return {};
	}
	return response->headers.GetHeaderValue("ETag");
}

void S3UploadSession::UploadPart(PreparedPart &part) {
	auto upload_id = EnsureMultipartUpload();
	ThrowIfFailed();
	D_ASSERT(upload_id);
	S3RequestQuery query {{"partNumber", to_string(part.part_number)}, {"uploadId", *upload_id}};
	S3CompletedPart completed;
	HTTPHeaders headers;
	if (config.checksum) {
		completed.checksum_crc32c =
		    EncodeChecksum(absl::ComputeCrc32c(absl::string_view(const_char_ptr_cast(part.data), part.size)));
		headers["x-amz-checksum-crc32c"] = completed.checksum_crc32c;
	}
	auto request_result =
	    RunUploadRequest(S3RequestOperation::UPLOAD_PART, part.data, part.size, query, std::move(headers));
	auto &response = request_result.response;
	if (response->status != HTTPStatusCode::OK_200) {
		throw GetStatusError(*response, request_result.context);
	}
	if (!response->headers.HasHeader("ETag")) {
		throw IOException("Unexpected response when uploading to S3");
	}
	completed.etag = response->headers.GetHeaderValue("ETag");
	if (completed.etag.empty()) {
		throw IOException("Unexpected response when uploading to S3");
	}
	StorePart(part.part_number, std::move(completed));
}

void S3UploadSession::StorePart(idx_t part_number, S3CompletedPart part) DUCKDB_EXCLUDES(state_lock) {
	FailureSnapshot failure;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (primary_failure.primary_error) {
			failure = CaptureFailure();
		} else {
			D_ASSERT(part_number > 0);
			D_ASSERT(part_number <= upload_parts.size());
			D_ASSERT(upload_parts[part_number - 1].etag.empty());
			upload_parts[part_number - 1] = std::move(part);
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
}

S3UploadSession::MultipartSnapshot S3UploadSession::GetMultipartSnapshot() DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(initialization_state == InitializationState::SUCCEEDED);
	for (auto &part : upload_parts) {
		if (part.etag.empty()) {
			throw IOException("S3 multipart upload is missing a completed part");
		}
	}
	return {multipart_upload_id, upload_parts, checksum};
}

absl::crc32c_t S3UploadSession::Checksum() const DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	return checksum;
}

string S3UploadSession::CompleteMultipartUpload() {
	auto snapshot = GetMultipartSnapshot();
	D_ASSERT(snapshot.upload_id);
	HTTPHeaders headers;
	if (config.checksum) {
		headers["x-amz-checksum-crc32c"] = EncodeChecksum(snapshot.checksum);
		headers["x-amz-checksum-type"] = "FULL_OBJECT";
	}
	if (config.exclusive) {
		headers["If-None-Match"] = "*";
	}
	auto completion_body = S3XMLWriter::WriteCompleteMultipartUploadRequest(snapshot.parts);

	S3RequestQuery query {{"uploadId", *snapshot.upload_id}};
	string result;
	auto request_result = s3fs.get().PostRequest(*request_session, S3RequestOperation::COMPLETE_MULTIPART_UPLOAD, path,
	                                             result, const_data_ptr_cast(completion_body.data()),
	                                             completion_body.size(), query, std::move(headers));
	auto &response = request_result.response;
	auto &request_context = request_result.context;
	if (response->HasRequestError()) {
		throw S3AmbiguousUploadException(
		    StringUtil::Format("S3 multipart upload completion for \"%s\" has an unknown outcome because the "
		                       "response was not received; "
		                       "the request was not retried or aborted",
		                       request_context.display_url));
	}
	VerifyCreated(config, *response, request_context.display_url);
	if (!IsSuccessfulStatus(response->status)) {
		throw GetStatusError(*response, request_context);
	}

	S3XMLResponse parsed_response;
	if (!S3XMLResponseParser::TryParse(result, parsed_response)) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload completion for \"%s\" returned malformed XML; the request was not retried or aborted",
		    request_context.display_url));
	}
	if (parsed_response.type == S3XMLResponseType::ERROR) {
		throw HTTPException(
		    *response, "S3 multipart upload completion for \"%s\" failed: %s%s%s", request_context.display_url,
		    parsed_response.error_code.empty() ? "S3 returned an embedded error" : parsed_response.error_code,
		    parsed_response.error_message.empty() ? "" : ": ", parsed_response.error_message);
	}
	if (parsed_response.type != S3XMLResponseType::MULTIPART_COMPLETION) {
		throw S3AmbiguousUploadException(StringUtil::Format("S3 multipart upload completion for \"%s\" returned an "
		                                                    "unrecognized response; the request was not retried or "
		                                                    "aborted",
		                                                    request_context.display_url));
	}
	return std::move(parsed_response.etag);
}

string S3UploadSession::GetDisplayPath() const {
	auto captured = request_session->Capture();
	auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
	return S3Url::GetDisplayUrl(path, snapshot.auth_params);
}

HTTPException S3UploadSession::GetStatusError(const HTTPResponse &response, const S3RequestContext &request_context) {
	return S3RequestUtil::GetRequestError(request_context, response);
}

S3UploadSession::WriteClaim S3UploadSession::Write(const_data_ptr_t data, idx_t size, idx_t location) {
	BeginWriteOperation();
	WriteClaim claim(*this);
	try {
		auto write = PrepareWrite(data, size, location);
		for (auto &part : write.parts) {
			ThrowIfFailed();
			if (part.buffered_part && UploadsInBackground()) {
				ScheduleUpload(std::move(part));
			} else {
				UploadPart(part);
			}
		}
		return claim;
	} catch (S3AmbiguousUploadException &ex) {
		claim.Fail(ErrorData(ex), FailureDisposition::AMBIGUOUS);
	} catch (std::exception &ex) {
		claim.Fail(ErrorData(ex), FailureDisposition::DEFINITIVE);
	}
}

string S3UploadSession::Finalize() {
	bool already_finalized;
	auto local_buffered_part = BeginFinalize(already_finalized);
	if (already_finalized) {
		return {};
	}

	try {
		bool multipart_upload;
		{
			annotated_lock_guard<annotated_mutex> guard(state_lock);
			multipart_upload = !upload_parts.empty();
		}
		string etag;
		if (!multipart_upload) {
			if (!local_buffered_part) {
				etag = UploadObject(nullptr, 0);
			} else {
				etag = UploadObject(local_buffered_part->Ptr(), local_buffered_part->size);
			}
		} else {
			if (local_buffered_part) {
				PreparedWrite final_write;
				{
					annotated_lock_guard<annotated_mutex> guard(state_lock);
					ReservePart(final_write, std::move(local_buffered_part));
				}
				D_ASSERT(final_write.parts.size() == 1);
				UploadPart(final_write.parts.front());
			}
			AwaitBackgroundUploads();
			ThrowIfFailed();
			etag = CompleteMultipartUpload();
		}
		FinishFinalize();
		return etag;
	} catch (S3AmbiguousUploadException &ex) {
		FailOperation(ErrorData(ex), FailureDisposition::AMBIGUOUS);
	} catch (std::exception &ex) {
		FailOperation(ErrorData(ex), FailureDisposition::DEFINITIVE);
	}
}

bool S3UploadSession::Abort() {
	shared_ptr<const string> upload_id;
	unique_ptr<BufferedPart> discarded_buffer;
	FailureSnapshot failure;
	bool cleanup_owner = false;
	{
		annotated_unique_lock<annotated_mutex> guard(state_lock);
		auto settled = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return lifecycle_state != LifecycleState::FINALIZING && lifecycle_state != LifecycleState::ABORTING;
		};
		state_lock.Await(absl::Condition(&settled));
		if (lifecycle_state == LifecycleState::FINALIZED) {
			return false;
		}
		if (lifecycle_state == LifecycleState::ABORTED) {
			return true;
		}

		D_ASSERT(lifecycle_state == LifecycleState::ACTIVE);
		lifecycle_state = LifecycleState::ABORTING;
		auto drained = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return active_operations == 0 && parts_in_flight == 0;
		};
		if (!drained()) {
			TaskScheduler::BlockingScope blocking;
			state_lock.Await(absl::Condition(&drained));
		}
		discarded_buffer = std::move(buffered_part);

		if (primary_failure.primary_error) {
			auto cleaned_up = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
				return cleanup_state == CleanupState::COMPLETE;
			};
			state_lock.Await(absl::Condition(&cleaned_up));
			failure = CaptureFailure();
		} else if (multipart_upload_id && !abort_suppressed) {
			D_ASSERT(cleanup_state == CleanupState::NONE);
			cleanup_state = CleanupState::IN_PROGRESS;
			upload_id = multipart_upload_id;
			cleanup_owner = true;
		} else {
			cleanup_state = CleanupState::COMPLETE;
		}

		if (!cleanup_owner) {
			lifecycle_state = LifecycleState::ABORTED;
		}
	}
	discarded_buffer.reset();

	shared_ptr<const ErrorData> abort_error;
	if (cleanup_owner) {
		D_ASSERT(upload_id);
		abort_error = AbortMultipartUpload(*upload_id);
		{
			annotated_lock_guard<annotated_mutex> guard(state_lock);
			cleanup_state = CleanupState::COMPLETE;
			lifecycle_state = LifecycleState::ABORTED;
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
	if (abort_error) {
		abort_error->Throw();
	}
	return true;
}

bool S3UploadSession::UploadsInBackground() const {
	return config.parts_in_flight > 1 &&
	       TaskScheduler::GetScheduler(s3fs.get().GetBufferManager().GetDatabase()).NumberOfAsyncThreads() != 0;
}

void S3UploadSession::ScheduleUpload(PreparedPart part) {
	EnsureMultipartUpload();
	FailureSnapshot failure;
	{
		annotated_unique_lock<annotated_mutex> guard(state_lock);
		auto ready = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return parts_in_flight < config.parts_in_flight || primary_failure.primary_error;
		};
		if (!ready()) {
			TaskScheduler::BlockingScope blocking;
			state_lock.Await(absl::Condition(&ready));
		}
		if (primary_failure.primary_error) {
			failure = CaptureFailure();
		} else {
			parts_in_flight++;
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
	auto task = make_shared_ptr<PartUploadTask>(*this, std::move(part));
	auto &db = s3fs.get().GetBufferManager().GetDatabase();
	TaskScheduler::GetScheduler(db).ScheduleTask(UploadProducer(db), std::move(task), TaskSchedulerType::ASYNC);
}

void S3UploadSession::UploadInBackground(PreparedPart &part) noexcept {
	try {
		try {
			UploadPart(part);
		} catch (S3AmbiguousUploadException &ex) {
			LatchFailure(ErrorData(ex), FailureDisposition::AMBIGUOUS);
		} catch (std::exception &ex) {
			LatchFailure(ErrorData(ex), FailureDisposition::DEFINITIVE);
		}
	} catch (...) { // NOLINT
	}
	part.buffered_part.reset();
	FinishBackgroundUpload();
}

void S3UploadSession::FinishBackgroundUpload() noexcept {
	shared_ptr<const string> upload_id;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		D_ASSERT(parts_in_flight > 0);
		parts_in_flight--;
		upload_id = ClaimCleanup();
	}
	if (upload_id) {
		RunCleanup(*upload_id);
	}
}

void S3UploadSession::AwaitBackgroundUploads() noexcept {
	annotated_unique_lock<annotated_mutex> guard(state_lock);
	auto idle = [&]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		return parts_in_flight == 0;
	};
	if (!idle()) {
		TaskScheduler::BlockingScope blocking;
		state_lock.Await(absl::Condition(&idle));
	}
}

} // namespace duckdb
