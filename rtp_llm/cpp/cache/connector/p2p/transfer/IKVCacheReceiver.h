#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/Types.h"

#include <string>

namespace rtp_llm {
namespace transfer {

class IKVCacheRecvTask {
public:
    enum class ErrorCode {
        NONE_ERROR      = 0,
        TIMEOUT         = 1,
        CANCELLED       = 2,
        RECV_FAILED     = 3,
        BUFFER_MISMATCH = 4,
    };

public:
    virtual ~IKVCacheRecvTask() = default;

public:
    struct Result {
        bool        success;
        ErrorCode   error_code;
        std::string error_message;
    };

public:
    /// Wait task completion until deadline_ms (absolute unix ms).
    ///
    /// Return:
    /// - true: task finished before/equal deadline.
    /// - false: wait timeout; task may still be running.
    ///
    /// Thread-safety:
    /// - Can be called concurrently by multiple threads.
    virtual bool waitUntil(int64_t deadline_ms) = 0;

    /// Best-effort cancel, idempotent.
    ///
    /// Contract:
    /// 1) Multiple cancel() calls are allowed.
    /// 2) Once task reaches terminal state, later cancel() has no effect.
    /// 3) Successful cancellation should be reflected by Result.error_code = CANCELLED.
    virtual void cancel(const std::string& reason) = 0;

    /// Get terminal result snapshot.
    ///
    /// Contract:
    /// 1) Valid terminal result is guaranteed after waitUntil(...) returns true.
    /// 2) Returned result is a snapshot copy for thread-safe reads.
    virtual Result getResult() const = 0;
};
using IKVCacheRecvTaskPtr = std::shared_ptr<IKVCacheRecvTask>;

/// Receive request issued by sender peer.
///
/// Contract:
/// 1) deadline_ms is absolute unix timestamp in milliseconds.
/// 2) layer_block_info block ordering contract is same as SendRequest.
struct RecvRequest {
    std::string      unique_key;
    KeyBlockInfosPtr block_info;
    int64_t          deadline_ms;
};
using RecvRequestPtr = std::shared_ptr<RecvRequest>;

class IKVCacheReceiver {
public:
    virtual ~IKVCacheReceiver() = default;

    /// Same contract as IKVCacheSender::regMem.
    virtual bool regMem(const BlockInfo& block_info, uint64_t aligned_size = 0) = 0;

    /// Start receiving data asynchronously.
    ///
    /// Contract:
    /// 1) Returns non-null task on accepted request.
    /// 2) nullptr means request rejected before task creation.
    virtual IKVCacheRecvTaskPtr recv(const RecvRequestPtr& request) = 0;
};
using IKVCacheReceiverPtr = std::shared_ptr<IKVCacheReceiver>;

}  // namespace transfer
}  // namespace rtp_llm
