#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/RdmaInterface.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaMemoryManager.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaMetric.h"
#include "aios/network/accl-barex/include/accl/barex/highlevel/xconnection.h"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

namespace rtp_llm {
namespace transfer {

enum class RdmaConnectionState {
    CONNECTING,
    CONNECTED,
    FAILED,
};

/// Wraps a single ::accl::barex::XConnection and implements IRdmaConnection.
///
/// Requests arriving before the underlying connection is established are queued
/// and flushed as soon as onConnectSuccess() is called.
class RdmaConnection: public IRdmaConnection {
public:
    RdmaConnection(const std::shared_ptr<RdmaMemoryManager>&         memory_manager,
                   const std::string&                                ip,
                   uint32_t                                          port,
                   const std::shared_ptr<kmonitor::MetricsReporter>& metrics_reporter);
    ~RdmaConnection();

    RdmaConnectionState getState() const;

    void onConnectSuccess(const std::shared_ptr<::accl::barex::XConnection>& connection);
    void onConnectFailed(const std::shared_ptr<::accl::barex::XConnection>& connection);

    // IRdmaConnection
    void read(const std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>& pairs,
              std::function<void(bool)>                                                 callback,
              uint64_t                                                                  deadline_ms) override;

    const std::string& getIp() const {
        return ip_;
    }
    uint32_t getPort() const {
        return port_;
    }

private:
    struct PendingReadRequest {
        std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>> pairs;
        std::function<void(bool)>                                          callback;
        std::shared_ptr<RdmaMetricsCollector>                              collector;
        uint64_t                                                           deadline_ms;
    };

    std::vector<PendingReadRequest> drainPendingRequests();
    void                            processPendingRequests();

    bool validateAndBuildRwMemps(const std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>& pairs,
                                 std::vector<::accl::barex::rw_memp_t>&                                    mems,
                                 uint64_t                                                                  deadline_ms);

    void sendReadRequest(const std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>& pairs,
                         const std::shared_ptr<RdmaMetricsCollector>&                              collector,
                         std::function<void(bool)>                                                 callback,
                         uint64_t                                                                  deadline_ms);

private:
    std::shared_ptr<RdmaMemoryManager>         memory_manager_;
    std::string                                ip_;
    uint32_t                                   port_;
    std::shared_ptr<kmonitor::MetricsReporter> metrics_reporter_;

    mutable std::mutex               state_mutex_;
    std::atomic<RdmaConnectionState> state_;

    std::shared_ptr<::accl::barex::XConnection> connection_;

    std::mutex                     pending_mutex_;
    std::queue<PendingReadRequest> pending_requests_;
};

}  // namespace transfer
}  // namespace rtp_llm
