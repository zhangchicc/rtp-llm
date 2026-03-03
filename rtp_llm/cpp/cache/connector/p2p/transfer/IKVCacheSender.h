#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/Types.h"

#include <functional>
#include <string>

namespace rtp_llm {
namespace transfer {

/// Send request from producer to remote receiver endpoint.
///
/// Contract:
/// 1) ip/port points to receiver service endpoint.
/// 2) deadline_ms is absolute unix timestamp in milliseconds.
/// 3) request and its LayerBlockInfo should be treated immutable after send().
struct SendRequest {
    std::string      ip;
    uint32_t         port;
    std::string      unique_key;
    KeyBlockInfosPtr block_info;
    int64_t          deadline_ms;
};
using SendRequestPtr = std::shared_ptr<SendRequest>;

class IKVCacheSender {
public:
    enum class ErrorCode {
        NONE_ERROR      = 0,
        FAILED          = 1,
        TIMEOUT         = 2,
        RESPONSE_FAILED = 3,
    };

public:
    virtual ~IKVCacheSender() = default;

    /// Register local memory for RDMA transport.
    ///
    /// Contract:
    /// 1) RDMA implementation: perform real MR registration and return success.
    /// 2) TCP implementation: no-op and return true.
    /// 3) Caller must keep buffer valid until transfer that uses this memory finishes.
    virtual bool regMem(const BlockInfo& block_info, uint64_t aligned_size = 0) = 0;

    /// Asynchronously send data described by request.
    ///
    /// Contract:
    /// 1) callback is invoked exactly once.
    /// 2) callback may run on implementation worker threads.
    /// 3) callback(ErrorCode::TIMEOUT, ...) should be used for deadline exceed.
    /// 4) If send() returns normally, ownership of completion notification is transferred
    ///    to callback (including immediate-fail path).
    virtual void send(const SendRequestPtr& request, std::function<void(ErrorCode, const std::string&)> callback) = 0;
};
using IKVCacheSenderPtr = std::shared_ptr<IKVCacheSender>;

}  // namespace transfer
}  // namespace rtp_llm
