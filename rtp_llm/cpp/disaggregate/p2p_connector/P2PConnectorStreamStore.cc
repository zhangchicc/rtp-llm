#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorStreamStore.h"

#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorMetrics.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include <thread>
#include <chrono>

namespace rtp_llm {

PrefillConnectorStreamStore::PrefillConnectorStreamStore(const kmonitor::MetricsReporterPtr& metrics_reporter):
    metrics_reporter_(metrics_reporter) {}

PrefillConnectorStreamStore::~PrefillConnectorStreamStore() {
    if (check_timeout_thread_) {
        check_timeout_thread_->stop();
    }
}

bool PrefillConnectorStreamStore::init() {
    check_timeout_thread_ =
        autil::LoopThread::createLoopThread(std::bind(&PrefillConnectorStreamStore::checkTimeout, this),
                                            100,  // 100ms
                                            "PrefillConnectorStreamStoreCheckTimeoutThread");
    if (!check_timeout_thread_) {
        RTP_LLM_LOG_ERROR("PrefillConnectorStreamStore init failed: check_timeout_thread is null");
        return false;
    }
    RTP_LLM_LOG_INFO("PrefillConnectorStreamStore init success");
    return true;
}

void PrefillConnectorStreamStore::addStream(const std::string& unique_key, GenerateStreamPtr stream) {
    std::lock_guard<std::mutex> lock(stream_map_mutex_);
    stream_map_[unique_key] = std::make_pair(stream, currentTimeUs());
}

std::shared_ptr<GenerateStream> PrefillConnectorStreamStore::stealStream(const std::string& unique_key) {
    std::lock_guard<std::mutex> lock(stream_map_mutex_);
    auto                        it = stream_map_.find(unique_key);
    if (it == stream_map_.end()) {
        return nullptr;
    }
    auto stream             = it->second.first;
    auto wait_start_time_us = it->second.second;
    stream_map_.erase(it);
    reportStreamMetrics(false, wait_start_time_us);
    return stream;
}

void PrefillConnectorStreamStore::checkTimeout() {
    std::lock_guard<std::mutex> lock(stream_map_mutex_);
    int64_t                     current_time_ms = currentTimeMs();
    for (auto it = stream_map_.begin(); it != stream_map_.end();) {
        auto& [unique_key, stream_pair]   = *it;
        auto [stream, wait_start_time_us] = stream_pair;
        if (stream && current_time_ms >= stream->getDeadlineMs()) {
            RTP_LLM_LOG_WARNING(
                "PrefillConnectorStreamStore: stream timeout, unique_key: %s, deadline_ms: %ld, current_time_ms: %ld",
                it->first.c_str(),
                stream->getDeadlineMs(),
                current_time_ms);
            it = stream_map_.erase(it);
            reportStreamMetrics(true, wait_start_time_us);
        } else {
            ++it;
        }
    }
    if (metrics_reporter_) {
        auto collector          = std::make_shared<P2PConnectorStreamStoreMetricsCollector1>();
        collector->stream_count = stream_map_.size();
        metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorStreamStoreMetricsCollector1>(nullptr,
                                                                                                 collector.get());
    }
}

void PrefillConnectorStreamStore::reportStreamMetrics(bool timeout, int64_t wait_start_time_us) {
    if (metrics_reporter_) {
        auto collector                 = std::make_shared<P2PConnectorStreamStoreMetricsCollector2>();
        collector->timeout             = timeout;
        collector->stream_wait_time_us = currentTimeUs() - wait_start_time_us;
        metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorStreamStoreMetricsCollector2>(nullptr,
                                                                                                 collector.get());
    }
}

}  // namespace rtp_llm
