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

    std::shared_ptr<P2PConnectorClientAsyncContext> asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                              int64_t                                   request_id,
                                                              const std::string&                        unique_key,
                                                              const std::string&                        prefill_ip,
                                                              uint32_t                                  prefill_port,
                                                              int64_t                                   deadline_ms);

private:
    void checkOnce();

private:
    const GptInitParameter&                   gpt_init_parameter_;
    kmonitor::MetricsReporterPtr              metrics_reporter_;
    std::shared_ptr<TPBroadcastClient>        tp_broadcast_client_;
    std::shared_ptr<P2PConnectorServerCaller> server_caller_;

    mutable std::mutex                                           async_contexts_mutex_;
    std::vector<std::shared_ptr<P2PConnectorClientAsyncContext>> async_contexts_;
    autil::LoopThreadPtr                                         check_done_thread_;
};

}  // namespace rtp_llm
