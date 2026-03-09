#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/common/KVCacheRecvTaskBase.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/Types.h"

#include "kmonitor/client/MetricsReporter.h"

#include <cstdint>
#include <memory>
#include <string>

namespace rtp_llm::transfer::tcp {

class TcpKVCacheRecvTask final: public ::rtp_llm::transfer::KVCacheRecvTaskBase {
public:
    TcpKVCacheRecvTask(std::string                  unique_key,
                       KeyBlockInfosPtr             expected_block_info,
                       int64_t                      deadline_ms,
                       kmonitor::MetricsReporterPtr metrics_reporter = nullptr);
    ~TcpKVCacheRecvTask() override = default;

    // RecvTask does NOT depend on tcp service proto.
    // It only provides expected memory layout; state transitions are in the base class.
    KeyBlockInfosPtr getExpectedBlockInfo() const {
        return expected_block_info_;
    }

private:
    const KeyBlockInfosPtr expected_block_info_;
};

using TcpKVCacheRecvTaskPtr = std::shared_ptr<TcpKVCacheRecvTask>;

}  // namespace rtp_llm::transfer::tcp
