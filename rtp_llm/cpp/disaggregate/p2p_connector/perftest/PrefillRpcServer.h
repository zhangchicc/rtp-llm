#pragma once

#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/GrpcServer.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorPrefill.h"
#include "rtp_llm/cpp/model_rpc/proto/model_rpc_service.grpc.pb.h"

namespace rtp_llm {

class PrefillRpcServiceImpl: public PerfTestRpcServiceBase {
public:
    PrefillRpcServiceImpl(const GptInitParameter&                  gpt_init_parameter,
                          const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                          const kmonitor::MetricsReporterPtr&      metrics_reporter);
    ~PrefillRpcServiceImpl() = default;

public:
    bool init();

    grpc::Status StartLoad(grpc::ServerContext*                  context,
                           const P2PConnectorStartLoadRequestPB* request,
                           P2PConnectorStartLoadResponsePB*      response) override;

    std::shared_ptr<P2PConnectorPrefill> getConnector() const {
        return connector_;
    }

private:
    std::shared_ptr<P2PConnectorPrefill> connector_;
};

/// @brief Prefill-specific gRPC server
/// Contains P2PConnectorPrefill which internally manages scheduler and worker
class PrefillRpcServer: public GrpcServer {
public:
    PrefillRpcServer(const GrpcServerConfig&             config,
                     DeviceBase*                         device,
                     const kmonitor::MetricsReporterPtr& metrics_reporter);
    ~PrefillRpcServer() override;

    /// @brief Get the P2PConnectorPrefill instance
    std::shared_ptr<P2PConnectorPrefill> getConnector() const;

protected:
    std::shared_ptr<PerfTestRpcServiceBase> createRpcService() override;
};

}  // namespace rtp_llm
