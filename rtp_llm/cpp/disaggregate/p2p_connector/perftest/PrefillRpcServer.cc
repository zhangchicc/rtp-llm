#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/PrefillRpcServer.h"

#include <iostream>

namespace rtp_llm {

PrefillRpcServiceImpl::PrefillRpcServiceImpl(const GptInitParameter&                  gpt_init_parameter,
                                             const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                                             const kmonitor::MetricsReporterPtr&      metrics_reporter):
    PerfTestRpcServiceBase(gpt_init_parameter, kv_cache_allocator, metrics_reporter) {}

bool PrefillRpcServiceImpl::init() {
    if (initialized_) {
        std::cerr << "PrefillRpcServiceImpl already initialized" << std::endl;
        return false;
    }

    if (!kv_cache_allocator_) {
        std::cerr << "PrefillRpcServiceImpl: kv_cache_allocator is null" << std::endl;
        return false;
    }

    // Create P2PConnectorPrefill (which internally manages scheduler and worker)
    connector_ = std::make_shared<P2PConnectorPrefill>(gpt_params_, kv_cache_allocator_, metrics_reporter_);
    if (!connector_->init()) {
        std::cerr << "Failed to initialize P2PConnectorPrefill" << std::endl;
        return false;
    }

    // Create TP broadcast service and register connector callback
    auto callback = connector_->makeCallback();
    if (callback) {
        tp_broadcast_service_->registerCallback(callback);
    }

    initialized_ = true;
    return true;
}

grpc::Status PrefillRpcServiceImpl::StartLoad(grpc::ServerContext*                  context,
                                              const P2PConnectorStartLoadRequestPB* request,
                                              P2PConnectorStartLoadResponsePB*      response) {
    if (!connector_) {
        std::cerr << "PrefillRpcServiceImpl not initialized" << std::endl;
        return grpc::Status(grpc::StatusCode::INTERNAL, "PrefillRpcServiceImpl not initialized");
    }
    std::vector<std::pair<std::string, uint32_t>> decode_transfer_servers;
    for (const auto& worker : request->workers()) {
        decode_transfer_servers.push_back(std::make_pair(worker.ip(), worker.cache_store_port()));
    }
    return connector_->handleWrite(request->unique_key(), decode_transfer_servers, request->deadline_ms());
}

PrefillRpcServer::PrefillRpcServer(const GrpcServerConfig&             config,
                                   DeviceBase*                         device,
                                   const kmonitor::MetricsReporterPtr& metrics_reporter):
    GrpcServer(config, device, metrics_reporter) {}

PrefillRpcServer::~PrefillRpcServer() {
    stop();
}

std::shared_ptr<PerfTestRpcServiceBase> PrefillRpcServer::createRpcService() {
    auto rpc_service = std::make_shared<PrefillRpcServiceImpl>(gpt_params_, kv_cache_allocator_, metrics_reporter_);
    if (!rpc_service->init()) {
        std::cerr << "Failed to initialize PrefillRpcServiceImpl for TP rank " << config_.tp_rank << std::endl;
        return nullptr;
    }
    return rpc_service;
}

std::shared_ptr<P2PConnectorPrefill> PrefillRpcServer::getConnector() const {
    auto prefill_service = std::dynamic_pointer_cast<PrefillRpcServiceImpl>(rpc_service_);
    return prefill_service ? prefill_service->getConnector() : nullptr;
}

}  // namespace rtp_llm
