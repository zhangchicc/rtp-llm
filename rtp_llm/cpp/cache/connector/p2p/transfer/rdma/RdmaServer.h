#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/RdmaInterface.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaMemoryManager.h"
#include "aios/network/accl-barex/include/accl/barex/highlevel/xserver_transport.h"
#include "aios/network/accl-barex/include/accl/barex/highlevel/xconnection_callback.h"
#include <memory>
#include <string>

namespace rtp_llm {
namespace transfer {

// RDMA 服务器类
// 用于接收 RDMA read 请求，提供数据读取服务
class RdmaServer: public IRdmaServer {
public:
    // 构造函数
    // @param memory_manager: RDMA 内存管理器
    RdmaServer(const std::shared_ptr<IRdmaMemoryManager>& memory_manager);
    ~RdmaServer();

public:
    // 初始化 RDMA 服务器
    // @param listen_port: 监听端口
    // @param io_thread_count: IO 线程数量
    // @param worker_thread_count: 工作线程数量
    // @return: 是否初始化成功
    // 实现说明：
    // 1. 从 memory_manager_ 获取 RdmaMempool 和 XMempool
    // 2. 创建 KMonitorAcclMetricReporter
    // 3. 创建 XServerTransport 实例
    // 4. 监听指定端口
    bool init(uint32_t listen_port, int io_thread_count, int worker_thread_count);

    // 获取监听端口
    uint32_t getListenPort() const override {
        return listen_port_;
    }

private:
    // RDMA 基础设施
    std::shared_ptr<::accl::barex::XServerTransport> server_;

    // MR 管理（使用独立的 IRdmaMemoryManager）
    std::shared_ptr<RdmaMemoryManager> memory_manager_;

    // 监听端口
    uint32_t listen_port_ = 0;

    // 初始化标志
    bool initialized_ = false;
};

}  // namespace transfer
}  // namespace rtp_llm
