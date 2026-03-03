#pragma once

#include "kmonitor/client/MetricsReporter.h"

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

namespace rtp_llm::transfer::tcp {

class TcpKVCacheRecvTask;
using TcpKVCacheRecvTaskPtr = std::shared_ptr<TcpKVCacheRecvTask>;

class TcpKVCacheTaskStore {
public:
    explicit TcpKVCacheTaskStore(kmonitor::MetricsReporterPtr metrics_reporter);
    ~TcpKVCacheTaskStore() = default;

public:
    // Returns false when unique_key already exists.
    bool addTask(const std::string& unique_key, const TcpKVCacheRecvTaskPtr& task);

    TcpKVCacheRecvTaskPtr getTask(const std::string& unique_key) const;

    // Remove and return task (nullptr if absent).
    TcpKVCacheRecvTaskPtr stealTask(const std::string& unique_key);

    int64_t getTaskCount() const;

private:
    void reportTaskCount(int64_t task_count) const;

private:
    mutable std::shared_mutex                    mutex_;
    std::map<std::string, TcpKVCacheRecvTaskPtr> task_map_;
    kmonitor::MetricsReporterPtr                 metrics_reporter_;
};

}  // namespace rtp_llm::transfer::tcp
