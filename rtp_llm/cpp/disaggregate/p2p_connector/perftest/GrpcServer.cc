#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/GrpcServer.h"

#include <iostream>

namespace rtp_llm {

// PerfTestRpcServiceBase implementation
PerfTestRpcServiceBase::PerfTestRpcServiceBase(const GptInitParameter&                  gpt_init_parameter,
                                               const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                                               const kmonitor::MetricsReporterPtr&      metrics_reporter):
    gpt_params_(gpt_init_parameter),
    kv_cache_allocator_(kv_cache_allocator),
    metrics_reporter_(metrics_reporter),
    tp_broadcast_service_(std::make_shared<TPBroadcastService>()) {}

grpc::Status PerfTestRpcServiceBase::BroadcastTp(grpc::ServerContext*        context,
                                                 const BroadcastTpRequestPB* request,
                                                 BroadcastTpResponsePB*      response) {
    return tp_broadcast_service_->broadcast(context, request, response);
}

// GrpcServer implementation
GrpcServer::GrpcServer(const GrpcServerConfig&             config,
                       DeviceBase*                         device,
                       const kmonitor::MetricsReporterPtr& metrics_reporter):
    config_(config), device_(device), metrics_reporter_(metrics_reporter) {}

GrpcServer::~GrpcServer() {
    stop();
}

GptInitParameter GrpcServer::createGptInitParameter() const {
    GptInitParameter params;

    // Basic params
    params.num_layers_ = config_.num_layers;
    params.tp_rank_    = config_.tp_rank;

    // Cache store config
    params.cache_store_rdma_mode_                          = config_.use_rdma;
    params.cache_store_config.cache_store_rdma_mode        = config_.use_rdma;
    params.cache_store_config.messager_io_thread_count     = config_.tcp_io_thread_count;
    params.cache_store_config.messager_worker_thread_count = config_.tcp_worker_thread_count;

    // Transfer port for TransferServer
    params.cache_store_listen_port_ = config_.transfer_port;

    return params;
}

std::shared_ptr<KVCacheAllocator> GrpcServer::createKVCacheAllocator() const {
    PerfTestKVCacheAllocator::Config alloc_config;
    alloc_config.num_layers  = config_.num_layers;
    alloc_config.num_blocks  = config_.num_blocks;
    alloc_config.block_size  = config_.block_size;
    alloc_config.separate_kv = config_.separate_kv;
    auto kv_cache_allocator  = std::make_shared<PerfTestKVCacheAllocator>(alloc_config, device_);
    if (!kv_cache_allocator->init()) {
        std::cerr << "Failed to initialize KV cache allocator for TP rank " << config_.tp_rank << std::endl;
        return nullptr;
    }
    return kv_cache_allocator;
}

bool GrpcServer::start() {
    if (running_) {
        std::cerr << "GrpcServer already running" << std::endl;
        return false;
    }

    gpt_params_ = createGptInitParameter();

    kv_cache_allocator_ = createKVCacheAllocator();
    if (!kv_cache_allocator_) {
        std::cerr << "Failed to create KV cache allocator" << std::endl;
        return false;
    }

    rpc_service_ = createRpcService();
    if (!rpc_service_) {
        std::cerr << "Failed to create RPC service" << std::endl;
        return false;
    }

    std::string addr = "0.0.0.0:" + std::to_string(config_.grpc_port);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(rpc_service_.get());

    grpc_server_ = builder.BuildAndStart();
    if (!grpc_server_) {
        std::cerr << "Failed to start gRPC server on " << addr << std::endl;
        return false;
    }

    running_ = true;

    std::cout << "gRPC server started on " << addr << " for TP rank " << config_.tp_rank << std::endl;
    return true;
}

void GrpcServer::stop() {
    if (!running_) {
        return;
    }

    running_ = false;
    if (grpc_server_) {
        grpc_server_->Shutdown();
        grpc_server_->Wait();
        grpc_server_.reset();
    }
}

}  // namespace rtp_llm
