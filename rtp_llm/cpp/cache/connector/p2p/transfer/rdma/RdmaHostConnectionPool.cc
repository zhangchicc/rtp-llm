#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaHostConnectionPool.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaConnection.h"

#include "rtp_llm/cpp/utils/Logger.h"

#include <algorithm>

namespace rtp_llm {
namespace transfer {

RdmaHostConnectionPool::RdmaHostConnectionPool(const std::string& ip, uint32_t port): ip_(ip), port_(port) {}

std::shared_ptr<RdmaConnection> RdmaHostConnectionPool::getAvailableConnection() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (connections_.empty()) {
        return nullptr;
    }

    // 清理 FAILED 状态的连接
    connections_.erase(std::remove_if(connections_.begin(),
                                      connections_.end(),
                                      [](const std::shared_ptr<RdmaConnection>& conn) {
                                          return !conn || conn->getState() == RdmaConnectionState::FAILED;
                                      }),
                       connections_.end());

    if (connections_.empty()) {
        return nullptr;
    }

    // 确保轮询索引有效
    if (round_robin_idx_ >= connections_.size()) {
        round_robin_idx_ = 0;
    }

    const size_t pool_size = connections_.size();

    // 优先查找 CONNECTED 状态的连接（从当前轮询索引开始）
    for (size_t i = 0; i < pool_size; ++i) {
        size_t idx  = (round_robin_idx_ + i) % pool_size;
        auto&  conn = connections_[idx];
        if (conn && conn->getState() == RdmaConnectionState::CONNECTED) {
            round_robin_idx_ = (idx + 1) % pool_size;
            RTP_LLM_LOG_DEBUG(
                "rdma host connection pool get CONNECTED connection[%zu] to %s:%u", idx, ip_.c_str(), port_);
            return conn;
        }
    }

    // 如果没有 CONNECTED 连接，查找 CONNECTING 状态的连接（从当前轮询索引开始）
    for (size_t i = 0; i < pool_size; ++i) {
        size_t idx  = (round_robin_idx_ + i) % pool_size;
        auto&  conn = connections_[idx];
        if (conn && conn->getState() == RdmaConnectionState::CONNECTING) {
            round_robin_idx_ = (idx + 1) % pool_size;
            RTP_LLM_LOG_DEBUG(
                "rdma host connection pool get CONNECTING connection[%zu] to %s:%u", idx, ip_.c_str(), port_);
            return conn;
        }
    }

    // 所有连接都是 FAILED 状态（应该已经被清理）
    RTP_LLM_LOG_DEBUG("rdma host connection pool no available connection to %s:%u", ip_.c_str(), port_);
    return nullptr;
}

uint32_t RdmaHostConnectionPool::getConnectionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connections_.size();
}

void RdmaHostConnectionPool::addConnection(const std::shared_ptr<RdmaConnection>& connection) {
    if (!connection) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    connections_.push_back(connection);
    RTP_LLM_LOG_DEBUG(
        "rdma host connection pool add connection to %s:%u, total: %lu", ip_.c_str(), port_, connections_.size());
}

}  // namespace transfer
}  // namespace rtp_llm
