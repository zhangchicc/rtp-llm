#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheRecvTask.h"

namespace rtp_llm::transfer::rdma {

RdmaKVCacheRecvTask::RdmaKVCacheRecvTask(std::string                  unique_key,
                                         KeyBlockInfosPtr             expected_block_info,
                                         int64_t                      deadline_ms,
                                         kmonitor::MetricsReporterPtr metrics_reporter):
    KVCacheRecvTaskBase(std::move(unique_key), deadline_ms, std::move(metrics_reporter)),
    expected_block_info_(std::move(expected_block_info)) {
    if (!expected_block_info_) {
        failImmediate(ErrorCode::RECV_FAILED, "expected_block_info is null");
        return;
    }

    int64_t block_count = 0, total_block_size = 0;
    for (const auto& [key, blocks] : *expected_block_info_) {
        (void)key;
        block_count += 1;
        for (const auto& block : blocks) {
            total_block_size += static_cast<int64_t>(block.size_bytes);
        }
    }
    initCollectorMetrics(block_count, total_block_size);
}

}  // namespace rtp_llm::transfer::rdma
