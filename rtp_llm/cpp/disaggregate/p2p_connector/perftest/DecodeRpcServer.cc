#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/DecodeRpcServer.h"

#include <iostream>

namespace rtp_llm {

DecodeRpcServiceImpl::DecodeRpcServiceImpl(const GptInitParameter&                  gpt_init_parameter,
                                           const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                                           const kmonitor::MetricsReporterPtr&      metrics_reporter):
    PerfTestRpcServiceBase(gpt_init_parameter, kv_cache_allocator, metrics_reporter) {}

bool DecodeRpcServiceImpl::init() {
    if (!kv_cache_allocator_) {
        std::cerr << "DecodeRpcServiceImpl: kv_cache_allocator is null" << std::endl;
        return false;
    }

    // Create P2PConnectorClient (which internally manages scheduler and worker)
    connector_ = std::make_shared<P2PConnectorClient>(gpt_params_, kv_cache_allocator_, metrics_reporter_);
    if (!connector_->init()) {
        std::cerr << "Failed to initialize P2PConnectorClient" << std::endl;
        return false;
    }

    // Create TP broadcast service and register connector callback
    auto callback = connector_->makeCallback();
    if (callback) {
        tp_broadcast_service_->registerCallback(callback);
    }

    return true;
}

DecodeRpcServer::DecodeRpcServer(const GrpcServerConfig&             config,
                                 DeviceBase*                         device,
                                 const kmonitor::MetricsReporterPtr& metrics_reporter):
    GrpcServer(config, device, metrics_reporter) {}

DecodeRpcServer::~DecodeRpcServer() {
    stop();
}

std::shared_ptr<PerfTestRpcServiceBase> DecodeRpcServer::createRpcService() {
    auto rpc_service = std::make_shared<DecodeRpcServiceImpl>(gpt_params_, kv_cache_allocator_, metrics_reporter_);
    if (!rpc_service->init()) {
        std::cerr << "Failed to initialize DecodeRpcServiceImpl for TP rank " << config_.tp_rank << std::endl;
        return nullptr;
    }
    return rpc_service;
}

std::shared_ptr<P2PConnectorClient> DecodeRpcServer::getConnector() const {
    auto decode_service = std::dynamic_pointer_cast<DecodeRpcServiceImpl>(rpc_service_);
    return decode_service ? decode_service->getConnector() : nullptr;
}

}  // namespace rtp_llm
