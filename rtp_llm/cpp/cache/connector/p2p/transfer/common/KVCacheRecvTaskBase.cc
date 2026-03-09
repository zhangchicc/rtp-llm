#include "rtp_llm/cpp/cache/connector/p2p/transfer/common/KVCacheRecvTaskBase.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

#include <chrono>

namespace rtp_llm::transfer {

KVCacheRecvTaskBase::KVCacheRecvTaskBase(std::string                  unique_key,
                                         int64_t                      deadline_ms,
                                         kmonitor::MetricsReporterPtr metrics_reporter):
    unique_key_(std::move(unique_key)),
    deadline_ms_(deadline_ms),
    metrics_reporter_(std::move(metrics_reporter)),
    collector_(std::make_shared<RecvTaskMetricsCollector>()),
    start_time_us_(currentTimeUs()) {}

KVCacheRecvTaskBase::~KVCacheRecvTaskBase() {
    if (metrics_reporter_) {
        metrics_reporter_->report<TransferMetrics, RecvTaskMetricsCollector>(nullptr, collector_.get());
    }
}

void KVCacheRecvTaskBase::initCollectorMetrics(int64_t block_count, int64_t total_block_size) {
    collector_->block_count      = block_count;
    collector_->total_block_size = total_block_size;
}

void KVCacheRecvTaskBase::failImmediate(ErrorCode code, std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    finishLocked(false, code, std::move(message));
}

bool KVCacheRecvTaskBase::waitUntil(int64_t deadline_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (terminal_) {
        return true;
    }
    if (deadline_ms <= 0) {
        return false;
    }
    auto now_ms = currentTimeMs();
    if (now_ms >= deadline_ms) {
        return false;
    }
    auto timeout = std::chrono::milliseconds(static_cast<int64_t>(deadline_ms - now_ms));
    return cv_.wait_for(lock, timeout, [this]() { return terminal_; });
}

void KVCacheRecvTaskBase::cancel(const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminal_) {
        return;
    }
    cancel_requested_ = true;
    cancel_reason_    = reason.empty() ? "cancelled" : reason;
    // Non-blocking cancel: if not processing, enter terminal state immediately.
    if (!processing_) {
        finishLocked(false, ErrorCode::CANCELLED, cancel_reason_);
    }
}

IKVCacheRecvTask::Result KVCacheRecvTaskBase::getResult() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return result_;
}

bool KVCacheRecvTaskBase::markProcessing() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminal_) {
        return false;
    }
    processing_                            = true;
    collector_->wait_processing_latency_us = currentTimeUs() - start_time_us_;
    return true;
}

void KVCacheRecvTaskBase::markSuccess() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminal_) {
        return;
    }
    processing_ = false;
    if (cancel_requested_) {
        finishLocked(false, ErrorCode::CANCELLED, cancel_reason_.empty() ? "cancelled" : cancel_reason_);
        return;
    }
    if (currentTimeMs() > deadline_ms_) {
        finishLocked(false, ErrorCode::TIMEOUT, "recv task timeout");
        return;
    }
    finishLocked(true, ErrorCode::NONE_ERROR, "");
}

void KVCacheRecvTaskBase::markFailed(ErrorCode code, std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminal_) {
        return;
    }
    processing_ = false;
    if (cancel_requested_) {
        finishLocked(false, ErrorCode::CANCELLED, cancel_reason_.empty() ? "cancelled" : cancel_reason_);
        return;
    }
    finishLocked(false, code, std::move(message));
}

void KVCacheRecvTaskBase::finishLocked(bool success, ErrorCode code, std::string message) {
    if (terminal_) {
        return;
    }
    terminal_             = true;
    result_.success       = success;
    result_.error_code    = code;
    result_.error_message = std::move(message);

    collector_->success    = success;
    collector_->latency_us = currentTimeUs() - start_time_us_;

    cv_.notify_all();
}

}  // namespace rtp_llm::transfer
