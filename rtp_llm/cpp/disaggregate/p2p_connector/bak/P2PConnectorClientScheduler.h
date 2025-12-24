#pragma once

#include "rtp_llm/cpp/cache/KVCacheConnector.h"
#include "rtp_llm/cpp/config/GptInitParameter.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/TPBroadcastClient.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorServerCaller.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorMetrics.h"
#include "autil/LoopThread.h"
#include <memory>
#include <string>

namespace rtp_llm {

class P2PConnectorClientAsyncContext: public KVCacheConnector::AsyncContext {
public:
    P2PConnectorClientAsyncContext(const std::shared_ptr<KVCacheResourceV1>&                resource,
                                   const std::shared_ptr<TPBroadcastClient::Result>&        tp_sync_result,
                                   const std::shared_ptr<P2PConnectorServerCaller::Result>& server_call_result,
                                   const std::shared_ptr<P2PConnectorClientSchedulerMetricsCollector>& collector);
    virtual ~P2PConnectorClientAsyncContext();

public:
    bool done() const override;
    bool success() const override;
    void checkDone();

private:
    std::shared_ptr<KVCacheResourceV1>                           resource_;  // hold resource till done
    std::shared_ptr<TPBroadcastClient::Result>                   tp_sync_result_;
    std::shared_ptr<P2PConnectorServerCaller::Result>            server_call_result_;
    std::shared_ptr<P2PConnectorClientSchedulerMetricsCollector> collector_;
};

class P2PConnectorClientScheduler {
public:
    P2PConnectorClientScheduler(const GptInitParameter&             gpt_init_parameter,
                                const kmonitor::MetricsReporterPtr& metrics_reporter);
    ~P2PConnectorClientScheduler();

public:
    bool init();

private:
    void checkOnce();

private:
    const GptInitParameter&                              gpt_init_parameter_;
    kmonitor::MetricsReporterPtr                         metrics_reporter_;
    std::shared_ptr<TPBroadcastClient>                   tp_broadcast_client_;
    std::shared_ptr<P2PConnectorServerCaller>            server_caller_;
    std::shared_ptr<P2PConnectorAsyncReadContextChecker> checker_;
};

}  // namespace rtp_llm
