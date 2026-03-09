#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"

#include "kmonitor/client/MetricsReporter.h"
#include "rtp_llm/cpp/utils/Logger.h"

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

namespace rtp_llm::transfer {

/// Generic task store keyed by unique_key string.
///
/// Template parameter Task must be a class whose shared_ptr is stored.
/// Thread-safe: addTask/stealTask use an exclusive lock; getTask/getTaskCount use a shared lock.
template<typename Task>
class KVCacheTaskStore {
public:
    using TaskPtr = std::shared_ptr<Task>;

    explicit KVCacheTaskStore(kmonitor::MetricsReporterPtr metrics_reporter):
        metrics_reporter_(std::move(metrics_reporter)) {}

    ~KVCacheTaskStore() = default;

    /// Add a task. Returns false when unique_key already exists or task is null.
    bool addTask(const std::string& unique_key, const TaskPtr& task) {
        if (!task) {
            RTP_LLM_LOG_WARNING("KVCacheTaskStore addTask failed, task is null");
            return false;
        }
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (task_map_.count(unique_key)) {
            RTP_LLM_LOG_WARNING("KVCacheTaskStore addTask failed, task already exists: %s", unique_key.c_str());
            return false;
        }
        task_map_[unique_key] = task;
        return true;
    }

    /// Look up a task by key. Returns nullptr if absent.
    TaskPtr getTask(const std::string& unique_key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto                                it = task_map_.find(unique_key);
        return it != task_map_.end() ? it->second : nullptr;
    }

    /// Remove and return a task. Returns nullptr if absent.
    TaskPtr stealTask(const std::string& unique_key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto                                it = task_map_.find(unique_key);
        if (it == task_map_.end()) {
            return nullptr;
        }
        TaskPtr task = it->second;
        task_map_.erase(it);
        return task;
    }

    int64_t getTaskCount() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return static_cast<int64_t>(task_map_.size());
    }

    void reportTaskCount() const {
        if (!metrics_reporter_) {
            return;
        }
        RecvStoreMetricsCollector c;
        c.task_count = getTaskCount();
        metrics_reporter_->report<TransferMetrics, RecvStoreMetricsCollector>(nullptr, &c);
    }

private:
    mutable std::shared_mutex      mutex_;
    std::map<std::string, TaskPtr> task_map_;
    kmonitor::MetricsReporterPtr   metrics_reporter_;
};

}  // namespace rtp_llm::transfer
