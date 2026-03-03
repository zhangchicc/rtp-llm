#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheReceiver.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/Types.h"

#include "kmonitor/client/MetricsReporter.h"

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace rtp_llm::transfer::tcp {

class TcpKVCacheRecvTask final: public IKVCacheRecvTask {
public:
    TcpKVCacheRecvTask(std::string                  unique_key,
                       KeyBlockInfosPtr             expected_block_info,
                       int64_t                      deadline_ms,
                       kmonitor::MetricsReporterPtr metrics_reporter = nullptr);
    ~TcpKVCacheRecvTask() override;

public:
    bool   waitUntil(int64_t deadline_ms) override;
    void   cancel(const std::string& reason) override;
    Result getResult() const override;

public:
    // RecvTask does NOT depend on tcp service proto.
    // It only provides expected memory layout and task state transitions.
    KeyBlockInfosPtr getExpectedBlockInfo() const {
        return expected_block_info_;
    }

    // mark task processing
    // When task is already terminal (including cancelled), markProcessing is ignored.
    // If cancel is requested while processing, the final state will become CANCELLED
    // after markSuccess/markFailed is called by caller.
    // processing done when call markSuccess or markFailed
    bool markProcessing();

    // Mark the whole recv task succeeded.
    // block_count/total_block_size are precomputed in ctor.
    void markSuccess();

    // Mark task failed (terminal) with code/message.
    void markFailed(ErrorCode code, std::string message);

private:
    void finishLocked(bool success, ErrorCode code, std::string message);

private:
    const std::string      unique_key_;
    const KeyBlockInfosPtr expected_block_info_;
    const int64_t          deadline_ms_;

    mutable std::mutex      mutex_;
    std::condition_variable cv_;
    bool                    processing_       = false;
    bool                    cancel_requested_ = false;
    bool                    terminal_         = false;
    Result                  result_{false, ErrorCode::RECV_FAILED, ""};
    std::string             cancel_reason_;

    // metrics
    kmonitor::MetricsReporterPtr              metrics_reporter_;
    std::shared_ptr<RecvTaskMetricsCollector> collector_;
    int64_t                                   start_time_us_ = 0;
};

}  // namespace rtp_llm::transfer::tcp
