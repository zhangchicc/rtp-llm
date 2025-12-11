#pragma once

#include <memory>
#include <string>
#include <thread>
#include <atomic>

#include "grpc++/grpc++.h"
#include "kmonitor/client/MetricsReporter.h"
#include "rtp_llm/cpp/config/GptInitParameter.h"
#include "rtp_llm/cpp/devices/DeviceBase.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/PerfTestKVCacheAllocator.h"
#include "rtp_llm/cpp/cache_new/TpBroadcastManager.h"
#include "rtp_llm/cpp/model_rpc/proto/model_rpc_service.grpc.pb.h"

namespace rtp_llm {

/// @brief Server type enumeration
enum class ServerType {
    PREFILL = 0,
    DECODE  = 1,
};

/// @brief Base configuration for GrpcServer
struct GrpcServerConfig {
    int      tp_rank       = 0;
    uint32_t grpc_port     = 50051;
    uint32_t transfer_port = 60051;

    // KV cache settings
    int    num_layers  = 32;
    int    num_blocks  = 100;
    size_t block_size  = 1024 * 1024;  // 1MB
    bool   separate_kv = true;

    // Transfer settings
    int  tcp_io_thread_count     = 4;
    int  tcp_worker_thread_count = 8;
    bool use_rdma                = false;
};

/// @brief Base gRPC service implementation for perftest
class PerfTestRpcServiceBase: public RpcService::Service {
public:
    PerfTestRpcServiceBase(const GptInitParameter&                  gpt_init_parameter,
                           const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                           const kmonitor::MetricsReporterPtr&      metrics_reporter);
    virtual ~PerfTestRpcServiceBase() = default;

    /// @brief Handle BroadcastTp request
    grpc::Status BroadcastTp(grpc::ServerContext*        context,
                             const BroadcastTpRequestPB* request,
                             BroadcastTpResponsePB*      response) override;

protected:
    GptInitParameter                    gpt_params_;
    std::shared_ptr<KVCacheAllocator>   kv_cache_allocator_;
    kmonitor::MetricsReporterPtr        metrics_reporter_;
    std::shared_ptr<TPBroadcastService> tp_broadcast_service_;
    std::atomic<bool>                   initialized_{false};
};

/// @brief Base GrpcServer class containing gRPC server, TP_RANK and GptInitParameter
class GrpcServer {
public:
    GrpcServer(const GrpcServerConfig&             config,
               DeviceBase*                         device,
               const kmonitor::MetricsReporterPtr& metrics_reporter);
    virtual ~GrpcServer();

    /// @brief Start the gRPC server
    bool start();

    /// @brief Stop the gRPC server
    void stop();

    // Accessors
    int tpRank() const {
        return config_.tp_rank;
    }
    uint32_t grpcPort() const {
        return config_.grpc_port;
    }
    uint32_t transferPort() const {
        return config_.transfer_port;
    }

protected:
    /// @brief Create GptInitParameter from config
    virtual GptInitParameter createGptInitParameter() const;

    /// @brief Create KVCacheAllocator
    virtual std::shared_ptr<KVCacheAllocator> createKVCacheAllocator() const;

    /// @brief Create RPC service (must be implemented by subclasses)
    virtual std::shared_ptr<PerfTestRpcServiceBase> createRpcService() = 0;

protected:
    GrpcServerConfig             config_;
    GptInitParameter             gpt_params_;
    DeviceBase*                  device_ = nullptr;
    kmonitor::MetricsReporterPtr metrics_reporter_;

    std::shared_ptr<KVCacheAllocator>       kv_cache_allocator_;
    std::shared_ptr<PerfTestRpcServiceBase> rpc_service_;
    std::unique_ptr<grpc::Server>           grpc_server_;

    std::atomic<bool> running_{false};
};

}  // namespace rtp_llm
