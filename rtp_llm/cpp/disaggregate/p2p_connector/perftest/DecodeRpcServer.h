#pragma once

#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/GrpcServer.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorDecode.h"

namespace rtp_llm {

class DecodeRpcServiceImpl: public PerfTestRpcServiceBase {
public:
    DecodeRpcServiceImpl(const GptInitParameter&                  gpt_init_parameter,
                         const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                         const kmonitor::MetricsReporterPtr&      metrics_reporter);
    ~DecodeRpcServiceImpl() = default;

public:
    bool init();

    std::shared_ptr<P2PConnectorDecode> getConnector() const {
        return connector_;
    }

private:
    std::shared_ptr<P2PConnectorDecode> connector_;
};

/// @brief Decode-specific gRPC server
/// Contains P2PConnectorDecode which internally manages scheduler and worker
class DecodeRpcServer: public GrpcServer {
public:
    DecodeRpcServer(const GrpcServerConfig&             config,
                    DeviceBase*                         device,
                    const kmonitor::MetricsReporterPtr& metrics_reporter);
    ~DecodeRpcServer() override;

    /// @brief Get the P2PConnectorDecode instance
    std::shared_ptr<P2PConnectorDecode> getConnector() const;

protected:
    std::shared_ptr<PerfTestRpcServiceBase> createRpcService() override;
};

}  // namespace rtp_llm
