#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/RdmaInterface.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaHostConnectionPool.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaConnection.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaMemoryManager.h"
#include "aios/network/accl-barex/include/accl/barex/highlevel/xclient_transport.h"
#include <memory>
#include <string>
#include <shared_mutex>

namespace rtp_llm {
namespace transfer {

// RDMA 客户端类
// 整合 RdmaMemoryUtilImpl 的 MR 管理功能和 RdmaClient 的连接管理功能
class RdmaClient: public IRdmaClient {
public:
    // 构造函数
    // @param memory_manager: RDMA 内存管理器
    // @param rdma_connections_per_host: 每个主机的 RDMA 连接数量，默认 2
    // @param connect_timeout_ms: 连接超时时间（毫秒），默认 250ms
    RdmaClient(const std::shared_ptr<IRdmaMemoryManager>&        memory_manager,
               uint32_t                                          rdma_connections_per_host = 2,
               int                                               connect_timeout_ms        = 250,
               const std::shared_ptr<kmonitor::MetricsReporter>& metrics_reporter          = nullptr);
    ~RdmaClient();

public:
    // 初始化 RDMA 客户端
    // @param io_thread_count: IO 线程数量
    // @param worker_thread_count: 工作线程数量
    // @return: 是否初始化成功
    // 实现说明：
    // 1. 从 memory_manager_ 获取 RdmaMempool 和 XMempool
    // 2. 创建 KMonitorAcclMetricReporter
    // 3. 创建 XClientTransport 实例
    bool init(int io_thread_count, int worker_thread_count);

    // 获取到指定地址的 RDMA 连接（连接复用，轮询机制）
    // @param ip: 目标 IP 地址
    // @param port: 目标端口（RDMA 端口）
    // @return: RDMA 连接指针，失败返回 nullptr
    // 实现说明：
    // 1. 获取或创建对应 host:port 的连接池
    // 2. 从连接池中轮询获取可用连接（优先 CONNECTED，其次 CONNECTING）
    // 3. 如果连接数不足，异步补充新连接到连接池
    // 注意：连接是复用的，多个调用者可以共享同一连接
    std::shared_ptr<IRdmaConnection> getConnection(const std::string& ip, uint32_t port) override;

private:
    // 获取或创建连接池
    // @param ip: 目标 IP 地址
    // @param port: 目标端口
    // @return: 连接池指针
    std::shared_ptr<RdmaHostConnectionPool> getOrCreateConnectionPool(const std::string& ip, uint32_t port);

    // 初始化连接池（第一次连接到某个 ip:port 时调用）
    // @param pool: 连接池（ip 和 port 从 pool 中获取）
    // 实现说明：
    // 1. 创建 rdma_connections_per_host_ 个连接
    // 2. 将所有连接加入连接池
    void initializeConnectionPool(const std::shared_ptr<RdmaHostConnectionPool>& pool);

    // 补全连接池中的连接（如果连接数量不足）
    // @param pool: 连接池（ip 和 port 从 pool 中获取）
    // 实现说明：
    // 1. 检查当前连接数是否小于 rdma_connections_per_host_
    // 2. 如果不足，创建 1 个新连接并加入连接池（懒加载补充）
    void fillConnectionPoolIfNeeded(const std::shared_ptr<RdmaHostConnectionPool>& pool);

    // 创建新的 RDMA 连接
    // @param ip: 目标 IP 地址
    // @param port: 目标端口
    // @return: RDMA 连接指针，失败返回 nullptr
    // 实现说明：
    // 1. 创建 RdmaConnection 实例（状态为 CONNECTING）
    // 2. 创建连接回调，回调中调用 RdmaConnection::onConnectSuccess/onConnectFailed
    // 3. 调用 client_->Connect() 创建一个连接
    // 4. 返回 RdmaConnection 实例（连接建立是异步的，不加入连接池）
    std::shared_ptr<RdmaConnection> createNewConnection(const std::string& ip, uint32_t port);

private:
    // 连接管理参数
    uint32_t                                   rdma_connections_per_host_ = 2;
    int                                        connect_timeout_ms_        = 250;
    std::shared_ptr<kmonitor::MetricsReporter> metrics_reporter_;

    // RDMA 基础设施
    std::shared_ptr<arpc::RdmaMempool>               rdma_mempool_;
    std::shared_ptr<::accl::barex::XClientTransport> client_;

    // 连接管理（每个 host:port 对应一个连接池）
    mutable std::shared_mutex                                      host_connections_mutex_;
    std::map<std::string, std::shared_ptr<RdmaHostConnectionPool>> host_connection_pools_;

    // MR 管理（使用独立的 IRdmaMemoryManager）
    std::shared_ptr<RdmaMemoryManager> memory_manager_;

    // 初始化标志
    bool initialized_ = false;
};

}  // namespace transfer
}  // namespace rtp_llm
