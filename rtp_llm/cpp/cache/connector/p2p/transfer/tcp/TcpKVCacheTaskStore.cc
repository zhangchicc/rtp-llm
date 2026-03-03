#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheTaskStore.h"

#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"
#include "rtp_llm/cpp/utils/Logger.h"

#include <utility>

namespace rtp_llm::transfer::tcp {

TcpKVCacheTaskStore::TcpKVCacheTaskStore(kmonitor::MetricsReporterPtr metrics_reporter):
    metrics_reporter_(std::move(metrics_reporter)) {}

bool TcpKVCacheTaskStore::addTask(const std::string& unique_key, const TcpKVCacheRecvTaskPtr& task) {
    if (!task) {
        RTP_LLM_LOG_WARNING("TcpKVCacheTaskStore addTask failed, task is null");
        return false;
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto                                it = task_map_.find(unique_key);
    if (it != task_map_.end()) {
        RTP_LLM_LOG_WARNING("TcpKVCacheTaskStore addTask failed, task already exists: %s", unique_key.c_str());
        return false;
    }
    task_map_[unique_key] = task;
    return true;
}

TcpKVCacheRecvTaskPtr TcpKVCacheTaskStore::getTask(const std::string& unique_key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto                                it = task_map_.find(unique_key);
    if (it == task_map_.end()) {
        return nullptr;
    }
    return it->second;
}

TcpKVCacheRecvTaskPtr TcpKVCacheTaskStore::stealTask(const std::string& unique_key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto                                it = task_map_.find(unique_key);
    if (it == task_map_.end()) {
        return nullptr;
    }
    auto task = it->second;
    task_map_.erase(it);
    return task;
}

int64_t TcpKVCacheTaskStore::getTaskCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return static_cast<int64_t>(task_map_.size());
}

void TcpKVCacheTaskStore::reportTaskCount(int64_t task_count) const {
    if (!metrics_reporter_) {
        return;
    }
    RecvStoreMetricsCollector collector;
    collector.task_count = task_count;
    metrics_reporter_->report<TransferMetrics, RecvStoreMetricsCollector>(nullptr, &collector);
}

}  // namespace rtp_llm::transfer::tcp
