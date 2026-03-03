#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheReceiver.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheRecvTask.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include "rtp_llm/cpp/utils/AssertUtils.h"

namespace rtp_llm::transfer::tcp {

TcpKVCacheReceiver::TcpKVCacheReceiver(kmonitor::MetricsReporterPtr metrics_reporter):
    metrics_reporter_(std::move(metrics_reporter)) {}

TcpKVCacheReceiver::~TcpKVCacheReceiver() {
    // `server_` keeps raw service pointer after registerService(service_.get()).
    // Stop/destroy server first to avoid potential dangling service access.
    server_.reset();
    service_.reset();
    task_store_.reset();
}

bool TcpKVCacheReceiver::init(uint32_t listen_port,
                              int      io_thread_count,
                              int      worker_thread_count,
                              bool     enable_arpc_metric) {
    task_store_ = std::make_shared<TcpKVCacheTaskStore>(metrics_reporter_);
    service_    = std::make_shared<TcpKVCacheReceiverService>(task_store_, metrics_reporter_);
    server_     = std::make_shared<transfer::TcpServer>();

    RTP_LLM_CHECK_WITH_INFO(io_thread_count > 0 && worker_thread_count > 0,
                            "invalid io thread count or worker thread count");

    if (!server_->init(io_thread_count, worker_thread_count, listen_port, enable_arpc_metric)) {
        RTP_LLM_LOG_ERROR("TcpKVCacheReceiver init tcp server failed");
        return false;
    }
    if (!server_->registerService(service_.get())) {
        RTP_LLM_LOG_ERROR("TcpKVCacheReceiver register service failed");
        return false;
    }
    if (!server_->start()) {
        RTP_LLM_LOG_ERROR("TcpKVCacheReceiver start server failed");
        return false;
    }

    RTP_LLM_LOG_INFO("TcpKVCacheReceiver init success, listen %s:%u", ip().c_str(), port());
    return true;
}

std::string TcpKVCacheReceiver::ip() const {
    return server_ ? server_->getIP() : "";
}

uint32_t TcpKVCacheReceiver::port() const {
    return server_ ? server_->getPort() : 0;
}

bool TcpKVCacheReceiver::regMem(const BlockInfo& block_info, uint64_t aligned_size) {
    (void)block_info;
    (void)aligned_size;
    return true;  // TCP no-op
}

IKVCacheRecvTaskPtr TcpKVCacheReceiver::recv(const RecvRequestPtr& request) {
    if (!request || !task_store_) {
        RTP_LLM_LOG_WARNING("TcpKVCacheReceiver recv failed, request or task store is null");
        return nullptr;
    }
    if (!request->block_info || request->block_info->empty()) {
        RTP_LLM_LOG_WARNING("TcpKVCacheReceiver recv failed, block info is null");
        return nullptr;
    }
    if (request->deadline_ms <= currentTimeMs()) {
        RTP_LLM_LOG_WARNING("TcpKVCacheReceiver recv failed, deadline exceeded");
        return nullptr;
    }
    auto task = std::make_shared<TcpKVCacheRecvTask>(
        request->unique_key, request->block_info, request->deadline_ms, metrics_reporter_);
    if (!task_store_->addTask(request->unique_key, task)) {
        RTP_LLM_LOG_WARNING("TcpKVCacheReceiver recv rejected, duplicate unique_key: %s", request->unique_key.c_str());
        return nullptr;
    }
    return task;
}

}  // namespace rtp_llm::transfer::tcp
