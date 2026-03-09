#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheReceiver.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheRecvTask.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include "rtp_llm/cpp/utils/AssertUtils.h"

namespace rtp_llm::transfer::rdma {

RdmaKVCacheReceiver::RdmaKVCacheReceiver(kmonitor::MetricsReporterPtr metrics_reporter):
    metrics_reporter_(std::move(metrics_reporter)) {}

RdmaKVCacheReceiver::~RdmaKVCacheReceiver() {
    // Stop signal server first to avoid new RPCs reaching a partially-destroyed service.
    server_.reset();
    service_.reset();
    task_store_.reset();
    rdma_client_.reset();
    memory_manager_.reset();
}

bool RdmaKVCacheReceiver::init(uint32_t signal_listen_port,
                               int      rdma_io_threads,
                               int      rpc_io_threads,
                               int      rpc_worker_threads) {
    memory_manager_ = createRdmaMemoryManager();
    if (!memory_manager_) {
        RTP_LLM_LOG_ERROR("RdmaKVCacheReceiver: failed to create RDMA memory manager");
        return false;
    }

    rdma_client_ = createRdmaClient(memory_manager_, rdma_io_threads);
    if (!rdma_client_) {
        RTP_LLM_LOG_ERROR("RdmaKVCacheReceiver: failed to create RDMA client");
        return false;
    }

    task_store_ = std::make_shared<RdmaKVCacheTaskStore>(metrics_reporter_);
    service_    = std::make_shared<RdmaKVCacheReceiverService>(task_store_, rdma_client_, metrics_reporter_);
    server_     = std::make_shared<::rtp_llm::transfer::TcpServer>();

    RTP_LLM_CHECK_WITH_INFO(rpc_io_threads > 0 && rpc_worker_threads > 0, "invalid RPC thread counts");

    if (!server_->init(rpc_io_threads, rpc_worker_threads, signal_listen_port, false)) {
        RTP_LLM_LOG_ERROR("RdmaKVCacheReceiver: failed to init TCP signal server");
        return false;
    }
    if (!server_->registerService(service_.get())) {
        RTP_LLM_LOG_ERROR("RdmaKVCacheReceiver: failed to register RDMA signal service");
        return false;
    }
    if (!server_->start()) {
        RTP_LLM_LOG_ERROR("RdmaKVCacheReceiver: failed to start TCP signal server");
        return false;
    }

    RTP_LLM_LOG_INFO("RdmaKVCacheReceiver init success, signal server %s:%u", ip().c_str(), port());
    return true;
}

std::string RdmaKVCacheReceiver::ip() const {
    return server_ ? server_->getIP() : "";
}

uint32_t RdmaKVCacheReceiver::port() const {
    return server_ ? server_->getPort() : 0;
}

bool RdmaKVCacheReceiver::regMem(const BlockInfo& block_info, uint64_t aligned_size) {
    RTP_LLM_CHECK_WITH_INFO(memory_manager_ != nullptr, "RdmaKVCacheReceiver not initialized");
    return memory_manager_->regUserMr(block_info.addr, block_info.size_bytes, block_info.is_cuda, aligned_size);
}

IKVCacheRecvTaskPtr RdmaKVCacheReceiver::recv(const RecvRequestPtr& request) {
    if (!request || !task_store_) {
        RTP_LLM_LOG_WARNING("RdmaKVCacheReceiver recv failed, request or task store is null");
        return nullptr;
    }
    if (!request->block_info || request->block_info->empty()) {
        RTP_LLM_LOG_WARNING("RdmaKVCacheReceiver recv failed, block_info is null or empty");
        return nullptr;
    }
    if (request->deadline_ms <= currentTimeMs()) {
        RTP_LLM_LOG_WARNING("RdmaKVCacheReceiver recv failed, deadline already exceeded");
        return nullptr;
    }

    auto task = std::make_shared<RdmaKVCacheRecvTask>(
        request->unique_key, request->block_info, request->deadline_ms, metrics_reporter_);
    if (!task_store_->addTask(request->unique_key, task)) {
        RTP_LLM_LOG_WARNING("RdmaKVCacheReceiver recv rejected, duplicate unique_key: %s", request->unique_key.c_str());
        return nullptr;
    }
    return task;
}

}  // namespace rtp_llm::transfer::rdma
