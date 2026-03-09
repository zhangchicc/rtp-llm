#include "rtp_llm/cpp/cache/connector/p2p/transfer/RdmaInterface.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaClient.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaServer.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaMemoryManager.h"

#include "rtp_llm/cpp/utils/Logger.h"

using namespace rtp_llm::transfer;

namespace rtp_llm {

std::shared_ptr<IRdmaMemoryManager> createRdmaMemoryManager() {
    return std::make_shared<RdmaMemoryManager>();
}

std::shared_ptr<IRdmaClient> createRdmaClient(const std::shared_ptr<IRdmaMemoryManager>& memory_manager,
                                              int                                        io_thread_count,
                                              int                                        worker_thread_count,
                                              uint32_t                                   rdma_connections_per_host,
                                              int                                        connect_timeout_ms) {
    if (!memory_manager) {
        RTP_LLM_LOG_ERROR("create rdma client failed, memory manager is nullptr");
        return nullptr;
    }

    auto client = std::make_shared<RdmaClient>(memory_manager, rdma_connections_per_host, connect_timeout_ms);
    if (!client) {
        RTP_LLM_LOG_ERROR("create rdma client failed, make_shared failed");
        return nullptr;
    }

    if (!client->init(io_thread_count, worker_thread_count)) {
        RTP_LLM_LOG_ERROR("create rdma client failed, init failed");
        return nullptr;
    }

    return client;
}

std::shared_ptr<IRdmaServer> createRdmaServer(const std::shared_ptr<IRdmaMemoryManager>& memory_manager,
                                              uint32_t                                   listen_port,
                                              int                                        io_thread_count,
                                              int                                        worker_thread_count) {
    if (!memory_manager) {
        RTP_LLM_LOG_ERROR("create rdma server failed, memory manager is nullptr");
        return nullptr;
    }

    auto server = std::make_shared<RdmaServer>(memory_manager);
    if (!server) {
        RTP_LLM_LOG_ERROR("create rdma server failed, make_shared failed");
        return nullptr;
    }

    if (!server->init(listen_port, io_thread_count, worker_thread_count)) {
        RTP_LLM_LOG_ERROR("create rdma server failed, init failed, listen port %u", listen_port);
        return nullptr;
    }

    return server;
}

}  // namespace rtp_llm
