#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheReceiver.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"

#include "kmonitor/client/MetricsReporter.h"

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace rtp_llm::transfer {

/// Generic KV cache receive task base class.
///
/// Encapsulates the common state machine used by both TCP and RDMA receive tasks.
///
/// State transitions:
///   PENDING    → markProcessing()         → PROCESSING
///   PROCESSING → markSuccess()            → SUCCESS (or CANCELLED/TIMEOUT if flagged)
///   PROCESSING → markFailed(code, msg)    → FAILED  (or CANCELLED if cancel was requested)
///   PENDING    → cancel()                 → CANCELLED (immediate)
///   PROCESSING → cancel()                 → sets cancel_requested_; mark* finalises to CANCELLED
///
/// Thread safety: all public methods are thread-safe.
///
/// Subclasses call initCollectorMetrics() in their constructor to record block counts,
/// and may call failImmediate() when the task should start already in a failed state
/// (e.g. when expected_block_info is null).
class KVCacheRecvTaskBase: public IKVCacheRecvTask {
public:
    KVCacheRecvTaskBase(std::string                  unique_key,
                        int64_t                      deadline_ms,
                        kmonitor::MetricsReporterPtr metrics_reporter = nullptr);
    ~KVCacheRecvTaskBase() override;

    // IKVCacheRecvTask
    bool   waitUntil(int64_t deadline_ms) override;
    void   cancel(const std::string& reason) override;
    Result getResult() const override;

    /// Transition to PROCESSING state.
    /// Returns false when task is already terminal (including when cancel() was called
    /// before markProcessing()).
    bool markProcessing();

    /// Finalise task as success.
    /// Honours cancel_requested_ (→ CANCELLED) and deadline (→ TIMEOUT) checks.
    void markSuccess();

    /// Finalise task as failed with the given code/message.
    /// Honours cancel_requested_ (→ CANCELLED) check.
    void markFailed(ErrorCode code, std::string message);

protected:
    /// Record block statistics for metrics; call from subclass constructor.
    void initCollectorMetrics(int64_t block_count, int64_t total_block_size);

    /// Pre-fail the task from the subclass constructor (e.g. null block info).
    /// Must only be called from the subclass constructor (single-threaded context).
    void failImmediate(ErrorCode code, std::string message);

    const std::string unique_key_;
    const int64_t     deadline_ms_;

private:
    void finishLocked(bool success, ErrorCode code, std::string message);

    mutable std::mutex      mutex_;
    std::condition_variable cv_;
    bool                    processing_       = false;
    bool                    cancel_requested_ = false;
    bool                    terminal_         = false;
    Result                  result_{false, ErrorCode::RECV_FAILED, ""};
    std::string             cancel_reason_;

    kmonitor::MetricsReporterPtr              metrics_reporter_;
    std::shared_ptr<RecvTaskMetricsCollector> collector_;
    int64_t                                   start_time_us_ = 0;
};

}  // namespace rtp_llm::transfer
