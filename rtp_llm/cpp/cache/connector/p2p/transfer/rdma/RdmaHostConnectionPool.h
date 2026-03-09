#pragma once

#include <memory>
#include <string>
#include <mutex>
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaConnection.h"

namespace rtp_llm {
namespace transfer {

// RDMA 主机连接池类
// 管理特定 host:port 的连接池
class RdmaHostConnectionPool {
public:
    RdmaHostConnectionPool(const std::string& ip, uint32_t port);

    // 从连接池获取可用连接（轮询机制，连接复用）
    // @return: RDMA 连接指针，如果没有可用连接返回 nullptr
    // 实现说明：
    // 1. 优先返回 CONNECTED 状态的连接（从当前轮询索引开始查找）
    // 2. 如果没有 CONNECTED 连接，返回 CONNECTING 状态的连接
    // 3. 如果所有连接都是 FAILED 状态，返回 nullptr 并清理 failed 连接
    // 4. 连接不会从池中移除，支持多个调用者共享同一连接
    std::shared_ptr<RdmaConnection> getAvailableConnection();

    // 获取当前连接数量
    // @return: 当前连接总数（包括所有状态的连接）
    uint32_t getConnectionCount() const;

    // 添加新创建的连接到连接池
    // @param connection: 新创建的连接
    void addConnection(const std::shared_ptr<RdmaConnection>& connection);

    // 获取 IP 地址
    const std::string& getIp() const {
        return ip_;
    }

    // 获取端口
    uint32_t getPort() const {
        return port_;
    }

private:
    std::string                                  ip_;
    uint32_t                                     port_;
    mutable std::mutex                           mutex_;
    std::vector<std::shared_ptr<RdmaConnection>> connections_;          // 连接池中的所有连接
    size_t                                       round_robin_idx_ = 0;  // 轮询索引
};

}  // namespace transfer
}  // namespace rtp_llm
