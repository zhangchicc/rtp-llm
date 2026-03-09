#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaClient.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaConnection.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaHostConnectionPool.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaMemoryManager.h"

#include "aios/network/accl-barex/include/accl/barex/highlevel/xtransport_config.h"
#include "aios/network/accl-barex/include/accl/barex/highlevel/xconnection_callback.h"
#include "aios/network/accl-barex/include/accl/barex/highlevel/xclient_transport.h"

#include "aios/network/accl-barex/metric/KMonitorAcclMetricReporter.h"

#include "rtp_llm/cpp/utils/Logger.h"

#include <unistd.h>

namespace rtp_llm {
namespace transfer {

// 连接回调类，用于处理连接建立成功/失败
class RdmaClientConnectionCallback: public ::accl::barex::XConnectionCallback {
public:
    RdmaClientConnectionCallback(const std::shared_ptr<RdmaConnection>& rdma_connection):
        rdma_connection_(rdma_connection) {}

    void OnRecvCall(const std::shared_ptr<::accl::barex::XConnection>& conn,
                    const ::accl::barex::XMessage&                     msgs) override {
        RTP_LLM_LOG_ERROR("recv call, should not happen, request from %s", conn->GetPeerAddrAndPort().first.c_str());
    }

    void OnConnectSuccess(const std::shared_ptr<::accl::barex::XConnection>& conn) override {
        if (auto rdma_conn = rdma_connection_.lock()) {
            rdma_conn->onConnectSuccess(conn);
        }
    }

    void OnConnectFailed(const std::shared_ptr<::accl::barex::XConnection>& conn) override {
        if (auto rdma_conn = rdma_connection_.lock()) {
            rdma_conn->onConnectFailed(conn);
        }
    }

private:
    std::weak_ptr<RdmaConnection> rdma_connection_;
};

RdmaClient::RdmaClient(const std::shared_ptr<IRdmaMemoryManager>&        memory_manager,
                       uint32_t                                          rdma_connections_per_host,
                       int                                               connect_timeout_ms,
                       const std::shared_ptr<kmonitor::MetricsReporter>& metrics_reporter):
    rdma_connections_per_host_(rdma_connections_per_host),
    connect_timeout_ms_(connect_timeout_ms),
    metrics_reporter_(metrics_reporter),
    memory_manager_(std::dynamic_pointer_cast<RdmaMemoryManager>(memory_manager)) {}

RdmaClient::~RdmaClient() {
    // 清理所有连接池
    {
        std::unique_lock<std::shared_mutex> lock(host_connections_mutex_);
        host_connection_pools_.clear();
    }
    // RdmaMempoolFactory 的 Shutdown 会在全局析构时调用，这里不需要手动调用
}

bool RdmaClient::init(int io_thread_count, int worker_thread_count) {
    if (initialized_) {
        RTP_LLM_LOG_WARNING("rdma client already initialized");
        return true;
    }

    if (!memory_manager_) {
        RTP_LLM_LOG_ERROR("rdma client init failed, memory manager is nullptr");
        return false;
    }

    auto xmempool = memory_manager_->getXMempool();
    if (!xmempool) {
        RTP_LLM_LOG_ERROR("rdma client init failed, get xmempool failed");
        return false;
    }

    // 创建 metrics reporter
    auto metrics_reporter = std::make_shared<::accl::barex::KMonitorAcclMetricReporter>(true, kmonitor::FATAL);
    if (!metrics_reporter->init()) {
        RTP_LLM_LOG_ERROR("rdma client init failed, metrics reporter init failed");
        return false;
    }

    // 创建 XClientTransport
    ::accl::barex::XClientTransportConfig config(io_thread_count, worker_thread_count, xmempool, metrics_reporter);
    client_ = ::accl::barex::XClientTransport::NewInstance(config);
    if (!client_) {
        RTP_LLM_LOG_ERROR("rdma client init failed, accl client init failed");
        return false;
    }

    initialized_ = true;
    RTP_LLM_LOG_INFO("rdma client init success, io thread count %d, worker thread count %d, connect timeout %d ms",
                     io_thread_count,
                     worker_thread_count,
                     connect_timeout_ms_);
    return true;
}

std::shared_ptr<IRdmaConnection> RdmaClient::getConnection(const std::string& ip, uint32_t port) {
    if (!initialized_) {
        RTP_LLM_LOG_ERROR("rdma client not initialized");
        return nullptr;
    }

    // 1. 获取或创建连接池
    auto pool = getOrCreateConnectionPool(ip, port);

    // 2. 从连接池获取可用连接（轮询机制）
    auto rdma_connection = pool->getAvailableConnection();

    // 3. 如果获取到连接，检查是否需要补全连接池
    if (rdma_connection != nullptr) {
        fillConnectionPoolIfNeeded(pool);
        return rdma_connection;
    }

    // 4. 如果获取不到连接，创建一个新连接并加入连接池
    rdma_connection = createNewConnection(ip, port);
    if (rdma_connection != nullptr) {
        pool->addConnection(rdma_connection);
        return rdma_connection;
    }

    return nullptr;
}

std::shared_ptr<RdmaConnection> RdmaClient::createNewConnection(const std::string& ip, uint32_t port) {
    if (!memory_manager_) {
        RTP_LLM_LOG_WARNING("rdma client create connection failed, memory manager is nullptr");
        return nullptr;
    }

    // 1. 创建 RdmaConnection 实例（状态为 CONNECTING）
    auto rdma_connection = std::make_shared<RdmaConnection>(memory_manager_, ip, port, metrics_reporter_);

    // 2. 创建连接回调，回调中调用 RdmaConnection::onConnectSuccess/onConnectFailed
    auto callback = std::make_shared<RdmaClientConnectionCallback>(rdma_connection);

    // 3. 调用 client_->Connect() 创建一个连接
    client_->Connect(::accl::barex::TCP_V4, ip, port, callback);

    // 4. 返回 RdmaConnection 实例（连接建立是异步的，不加入连接池）
    return rdma_connection;
}

void RdmaClient::fillConnectionPoolIfNeeded(const std::shared_ptr<RdmaHostConnectionPool>& pool) {
    if (!pool) {
        return;
    }

    uint32_t current_count = pool->getConnectionCount();
    if (current_count >= rdma_connections_per_host_) {
        return;  // 连接数量已足够
    }

    // 从连接池获取 IP 和端口
    const std::string& ip   = pool->getIp();
    uint32_t           port = pool->getPort();

    auto rdma_connection = createNewConnection(ip, port);
    if (rdma_connection == nullptr) {
        RTP_LLM_LOG_WARNING(
            "rdma client fill connection pool failed, create connection to %s:%u failed", ip.c_str(), port);
    } else {
        pool->addConnection(rdma_connection);
    }
}

std::shared_ptr<RdmaHostConnectionPool> RdmaClient::getOrCreateConnectionPool(const std::string& ip, uint32_t port) {
    std::string                             spec        = ip + ":" + std::to_string(port);
    bool                                    is_new_pool = false;
    std::shared_ptr<RdmaHostConnectionPool> pool;

    {
        std::unique_lock<std::shared_mutex> lock(host_connections_mutex_);
        auto                                it = host_connection_pools_.find(spec);
        if (it == host_connection_pools_.end()) {
            pool = std::make_shared<RdmaHostConnectionPool>(ip, port);
            host_connection_pools_.insert(std::make_pair(spec, pool));
            is_new_pool = true;
        } else {
            pool = it->second;
        }
    }

    // 如果是新创建的连接池，初始化指定数量的连接
    if (is_new_pool) {
        initializeConnectionPool(pool);
    }

    return pool;
}

void RdmaClient::initializeConnectionPool(const std::shared_ptr<RdmaHostConnectionPool>& pool) {
    if (!pool || rdma_connections_per_host_ == 0) {
        return;
    }

    const std::string& ip   = pool->getIp();
    uint32_t           port = pool->getPort();

    RTP_LLM_LOG_INFO("rdma client initializing connection pool to %s:%u, initial connections: %u",
                     ip.c_str(),
                     port,
                     rdma_connections_per_host_);

    // 创建初始连接数量的连接并加入连接池
    for (uint32_t i = 0; i < rdma_connections_per_host_; ++i) {
        auto rdma_connection = createNewConnection(ip, port);
        if (rdma_connection == nullptr) {
            RTP_LLM_LOG_WARNING("rdma client initialize connection pool failed, create connection %u failed", i);
        } else {
            pool->addConnection(rdma_connection);
        }
    }
}

}  // namespace transfer
}  // namespace rtp_llm
