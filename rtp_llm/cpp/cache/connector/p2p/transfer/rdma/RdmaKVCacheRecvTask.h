#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/common/KVCacheRecvTaskBase.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/Types.h"

#include "kmonitor/client/MetricsReporter.h"

#include <cstdint>
#include <memory>
#include <string>

namespace rtp_llm::transfer::rdma {

/// RDMA-specific receive task.
///
/// Inherits the full state machine from KVCacheRecvTaskBase.
/// The key behavioural difference from TcpKVCacheRecvTask is that markSuccess/markFailed
/// are called from an RDMA worker callback thread (asynchronously) rather than from the
/// RPC service thread (synchronously after data copy).
///
/// The task additionally exposes getExpectedBlockInfo() so RdmaKVCacheReceiverService can
/// validate that the sender's RDMA block layout matches what the receiver expects and build
/// the LocalBuffer list for IRdmaConnection::read().
class RdmaKVCacheRecvTask final: public ::rtp_llm::transfer::KVCacheRecvTaskBase {
public:
    RdmaKVCacheRecvTask(std::string                  unique_key,
                        KeyBlockInfosPtr             expected_block_info,
                        int64_t                      deadline_ms,
                        kmonitor::MetricsReporterPtr metrics_reporter = nullptr);
    ~RdmaKVCacheRecvTask() override = default;

    /// Return the expected local buffer layout.
    /// Used by RdmaKVCacheReceiverService to validate the sender's request and
    /// construct the LocalBuffer list passed to IRdmaConnection::read().
    KeyBlockInfosPtr getExpectedBlockInfo() const {
        return expected_block_info_;
    }

private:
    const KeyBlockInfosPtr expected_block_info_;
};

using RdmaKVCacheRecvTaskPtr = std::shared_ptr<RdmaKVCacheRecvTask>;

}  // namespace rtp_llm::transfer::rdma
