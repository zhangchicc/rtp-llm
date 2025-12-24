#pragma once

#include "rtp_llm/cpp/engine_base/stream/GenerateStream.h"
#include <memory>
#include <string>
#include <map>
#include <mutex>
#include <thread>
#include <atomic>
#include "autil/LoopThread.h"

namespace rtp_llm {

/// @brief Prefill Connector Stream Store，存储 prefill 的 stream
// 实现思路: 当请求中有 pd_sep_unique_key 时, 认为这个 stream 会有一个对应的 P2PConnector load的请求, 用来保证
// p2p_connector 拉取过程中对应的资源不会释放 当 prefill 请求完成时, 需要将 stream 从 store 中移除, 如果超时未完成,
// 需要将 stream 移除, 并返回错误 unique_key 是 目前由 decode 生成, 后续可能由 master 统一生成保证全局唯一
class P2PConnectorStreamStore {
public:
    P2PConnectorStreamStore(const kmonitor::MetricsReporterPtr& metrics_reporter);
    ~P2PConnectorStreamStore();

public:
    bool init();

public:
    void                            addStream(const std::string& unique_key, GenerateStreamPtr stream);
    std::shared_ptr<GenerateStream> stealStream(const std::string& unique_key);

private:
    void checkTimeout();
    void reportStreamMetrics(bool success, int64_t wait_start_time_us);

private:
    mutable std::mutex                                           stream_map_mutex_;
    std::map<std::string, std::pair<GenerateStreamPtr, int64_t>> stream_map_;  // <unique_key, <stream, wait_time_us>>
    kmonitor::MetricsReporterPtr                                 metrics_reporter_;

    autil::LoopThreadPtr check_timeout_thread_;
};

}  // namespace rtp_llm
