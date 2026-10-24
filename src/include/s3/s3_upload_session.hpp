#pragma once

#include "s3/s3_settings.hpp"
#include "s3/s3_xml_response.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/http/http_util.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

#include <absl/crc/crc32c.h>

namespace duckdb {

class HTTPRequestSession;
class HTTPException;
class S3FileSystem;
struct HTTPResponse;
struct S3RequestContext;
struct S3RequestQuery;
struct S3RequestResult;
enum class S3RequestOperation : uint8_t;
enum class RequestType : uint8_t;

class S3UploadSession {
private:
	//! Session, initialization, cleanup, and failure states.
	enum class LifecycleState : uint8_t { ACTIVE, FINALIZING, FINALIZED, ABORTING, ABORTED };
	enum class InitializationState : uint8_t { NOT_STARTED, IN_PROGRESS, SUCCEEDED, FAILED };
	enum class CleanupState : uint8_t { NONE, IN_PROGRESS, COMPLETE };
	enum class FailureDisposition : uint8_t { DEFINITIVE, AMBIGUOUS };

	//! Latched upload failures.
	struct FailureSnapshot {
		shared_ptr<const ErrorData> primary_error;
		shared_ptr<const ErrorData> abort_error;
	};

	//! Buffered write preparation.
	struct BufferedPart {
		BufferedPart(BufferHandle buffer_p, idx_t capacity_p) : buffer(std::move(buffer_p)), capacity(capacity_p) {
		}

		data_ptr_t Ptr() {
			return buffer.GetDataMutable();
		}

		BufferHandle buffer;
		const idx_t capacity;
		idx_t size = 0;
	};

	struct PreparedPart {
		PreparedPart(idx_t part_number_p, const_data_ptr_t data_p, idx_t size_p)
		    : part_number(part_number_p), data(data_p), size(size_p) {
		}
		PreparedPart(idx_t part_number_p, unique_ptr<BufferedPart> buffered_part_p)
		    : part_number(part_number_p), buffered_part(std::move(buffered_part_p)), data(buffered_part->Ptr()),
		      size(buffered_part->size) {
		}

		idx_t part_number;
		unique_ptr<BufferedPart> buffered_part;
		const_data_ptr_t data;
		idx_t size;
	};

	struct PreparedWrite {
		vector<PreparedPart> parts;
		unique_ptr<BufferedPart> buffered_part;
	};

	//! Multipart completion state.
	struct MultipartSnapshot {
		shared_ptr<const string> upload_id;
		vector<S3CompletedPart> parts;
		absl::crc32c_t checksum {0};
	};

	//! Sends one part on the async threads.
	class PartUploadTask;

public:
	//! Owns one admitted write until it finishes or fails.
	class WriteClaim {
		friend class S3UploadSession;

	private:
		explicit WriteClaim(S3UploadSession &session_p);

	public:
		//! Claim lifecycle.
		WriteClaim(WriteClaim &&other) noexcept;
		WriteClaim(const WriteClaim &) = delete;
		WriteClaim &operator=(const WriteClaim &) = delete;
		~WriteClaim();
		void Finish();

	private:
		//! Failure publication.
		[[noreturn]] void Fail(ErrorData error, FailureDisposition disposition);

		//! Claimed session state.
		reference<S3UploadSession> session;
		bool finished = false;
	};

public:
	//! Session lifecycle.
	S3UploadSession(S3FileSystem &s3fs, shared_ptr<HTTPRequestSession> request_session, string path,
	                S3UploadConfig config);
	~S3UploadSession() noexcept;

	//! Upload operations.
	WriteClaim Write(const_data_ptr_t data, idx_t size, idx_t location);
	void Finalize();
	bool Abort();
	string UploadedETag() const DUCKDB_EXCLUDES(state_lock);

private:
	//! Operation admission and lifecycle transitions.
	void BeginWriteOperation() DUCKDB_EXCLUDES(state_lock);
	unique_ptr<BufferedPart> BeginFinalize(bool &already_finalized) DUCKDB_EXCLUDES(state_lock);
	void ReleaseOperation() DUCKDB_EXCLUDES(state_lock);
	void ReleaseWrite() DUCKDB_EXCLUDES(state_lock);
	void ReleaseWriteNoThrow() noexcept DUCKDB_EXCLUDES(state_lock);
	void FinishFinalize() DUCKDB_EXCLUDES(state_lock);

	//! Failure publication and cleanup.
	void LatchFailure(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock);
	void LatchFailureLocked(shared_ptr<const ErrorData> error, FailureDisposition disposition)
	    DUCKDB_REQUIRES(state_lock);
	[[noreturn]] void FailOperation(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock);
	//! Claims the cleanup of a failure once no operation or part is in flight: the multipart upload to abort, or null
	//! when another caller owns the cleanup, it has to wait, or there is nothing to abort
	shared_ptr<const string> ClaimCleanup() DUCKDB_REQUIRES(state_lock);
	//! Aborts the multipart upload a cleanup claimed and completes the cleanup
	void RunCleanup(const string &upload_id) noexcept DUCKDB_EXCLUDES(state_lock);
	FailureSnapshot CaptureFailure() const DUCKDB_REQUIRES(state_lock);
	[[noreturn]] static void ThrowFailure(const FailureSnapshot &failure);
	void ThrowIfFailed() DUCKDB_EXCLUDES(state_lock);

	//! Buffered write construction.
	PreparedWrite PrepareWrite(const_data_ptr_t data, idx_t size, idx_t location) DUCKDB_EXCLUDES(state_lock);
	unique_ptr<BufferedPart> AllocateBufferedPart(idx_t capacity);
	static idx_t AppendToBufferedPart(BufferedPart &buffered_part, const_data_ptr_t data, idx_t size);
	void ReservePart(PreparedWrite &write, const_data_ptr_t data, idx_t size) DUCKDB_REQUIRES(state_lock);
	void ReservePart(PreparedWrite &write, unique_ptr<BufferedPart> buffered_part) DUCKDB_REQUIRES(state_lock);

	//! Multipart initialization and completion.
	shared_ptr<const string> EnsureMultipartUpload();
	void PublishInitializationFailure(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock);
	string InitializeMultipartUpload();
	void StorePart(idx_t part_number, S3CompletedPart part) DUCKDB_EXCLUDES(state_lock);
	MultipartSnapshot GetMultipartSnapshot() DUCKDB_EXCLUDES(state_lock);
	void CompleteMultipartUpload();
	absl::crc32c_t Checksum() const DUCKDB_EXCLUDES(state_lock);
	void SetUploadedETag(string etag) DUCKDB_EXCLUDES(state_lock);

	//! S3 upload and cleanup requests.
	S3RequestResult RunUploadRequest(S3RequestOperation operation, const_data_ptr_t data, idx_t size,
	                                 const S3RequestQuery &query, HTTPHeaders headers);
	void UploadObject(const_data_ptr_t data, idx_t size);
	void UploadPart(PreparedPart &part);
	shared_ptr<const ErrorData> AbortMultipartUpload(const string &upload_id);

	//! Parts sent on the async threads while the writer goes on.
	bool UploadsInBackground() const;
	void ScheduleUpload(PreparedPart part);
	void UploadInBackground(PreparedPart &part) noexcept;
	void FinishBackgroundUpload() noexcept DUCKDB_EXCLUDES(state_lock);
	void AwaitBackgroundUploads() noexcept DUCKDB_EXCLUDES(state_lock);

	//! Request diagnostics.
	string GetDisplayPath() const;
	static HTTPException GetStatusError(const HTTPResponse &response, const S3RequestContext &request_context);

private:
	//! Immutable upload context.
	reference<S3FileSystem> s3fs;
	shared_ptr<HTTPRequestSession> request_session;
	const string path;
	const S3UploadConfig config;

	//! Operation and lifecycle synchronization.
	mutable annotated_mutex state_lock;
	LifecycleState lifecycle_state DUCKDB_GUARDED_BY(state_lock) = LifecycleState::ACTIVE;
	idx_t active_operations DUCKDB_GUARDED_BY(state_lock) = 0;
	//! Parts being sent on the async threads; the last one to finish after a failure cleans up
	idx_t parts_in_flight DUCKDB_GUARDED_BY(state_lock) = 0;

	//! Buffered and multipart upload state.
	idx_t next_offset DUCKDB_GUARDED_BY(state_lock) = 0;
	unique_ptr<BufferedPart> buffered_part DUCKDB_GUARDED_BY(state_lock);
	InitializationState initialization_state DUCKDB_GUARDED_BY(state_lock) = InitializationState::NOT_STARTED;
	shared_ptr<const string> multipart_upload_id DUCKDB_GUARDED_BY(state_lock);
	vector<S3CompletedPart> upload_parts DUCKDB_GUARDED_BY(state_lock);
	absl::crc32c_t checksum DUCKDB_GUARDED_BY(state_lock) {0};
	string uploaded_etag DUCKDB_GUARDED_BY(state_lock);

	//! Latched failure and cleanup state.
	FailureSnapshot primary_failure DUCKDB_GUARDED_BY(state_lock);
	FailureDisposition failure_disposition DUCKDB_GUARDED_BY(state_lock) = FailureDisposition::DEFINITIVE;
	bool abort_suppressed DUCKDB_GUARDED_BY(state_lock) = false;
	CleanupState cleanup_state DUCKDB_GUARDED_BY(state_lock) = CleanupState::NONE;
};

} // namespace duckdb
