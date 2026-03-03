#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheRecvTask.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

#include <chrono>

namespace rtp_llm::transfer::tcp {

TcpKVCacheRecvTask::TcpKVCacheRecvTask(std::string                  unique_key,
                                       KeyBlockInfosPtr             expected_block_info,
                                       int64_t                      deadline_ms,
                                       kmonitor::MetricsReporterPtr metrics_reporter):
    unique_key_(std::move(unique_key)),
    expected_block_info_(std::move(expected_block_info)),
    deadline_ms_(deadline_ms),
    metrics_reporter_(std::move(metrics_reporter)),
    collector_(std::make_shared<RecvTaskMetricsCollector>()),
    start_time_us_(currentTimeUs()) {
    if (!expected_block_info_) {
        std::lock_guard<std::mutex> lock(mutex_);
        finishLocked(false, ErrorCode::RECV_FAILED, "expected_block_info is null");
    }

    // 计算 block_count 和 block_size，成功时直接使用预计算结果。
    if (expected_block_info_) {
        int64_t expected_block_count      = 0;
        int64_t expected_total_block_size = 0;
        for (const auto& [key, blocks] : *expected_block_info_) {
            (void)key;
            expected_block_count += 1;
            for (const auto& block : blocks) {
                expected_total_block_size += static_cast<int64_t>(block.size_bytes);
            }
        }
        collector_->block_count      = expected_block_count;
        collector_->total_block_size = expected_total_block_size;
    }
}

TcpKVCacheRecvTask::~TcpKVCacheRecvTask() {
    if (metrics_reporter_) {
        metrics_reporter_->report<rtp_llm::transfer::TransferMetrics, rtp_llm::transfer::RecvTaskMetricsCollector>(
            nullptr, collector_.get());
    }
}

bool TcpKVCacheRecvTask::waitUntil(int64_t deadline_ms) {
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

void TcpKVCacheRecvTask::cancel(const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminal_) {
        return;
    }
    cancel_requested_ = true;
    cancel_reason_    = reason.empty() ? "cancelled" : reason;
    // Non-blocking cancel: if not processing now, enter terminal immediately.
    if (!processing_) {
        finishLocked(false, ErrorCode::CANCELLED, cancel_reason_);
    }
}

IKVCacheRecvTask::Result TcpKVCacheRecvTask::getResult() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return result_;
}

bool TcpKVCacheRecvTask::markProcessing() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminal_) {
        return false;
    }
    processing_                            = true;
    collector_->wait_processing_latency_us = currentTimeUs() - start_time_us_;
    return true;
}

void TcpKVCacheRecvTask::markSuccess() {
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

void TcpKVCacheRecvTask::markFailed(ErrorCode code, std::string message) {
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

void TcpKVCacheRecvTask::finishLocked(bool success, ErrorCode code, std::string message) {
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

}  // namespace rtp_llm::transfer::tcp
