#pragma once

#include "rtp_llm/cpp/cache_new/KVCacheConnector.h"
#include "rtp_llm/cpp/config/GptInitParameter.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/TPBroadcastClient.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/PrefillLoadClient.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorMetrics.h"
#include "autil/LoopThread.h"
#include <memory>
#include <string>

namespace rtp_llm {

class P2PConnectorDecodeAsyncContext: public KVCacheConnector::AsyncContext {
public:
    P2PConnectorDecodeAsyncContext(const std::shared_ptr<KVCacheResourceV1>&         resource,
                                   const std::shared_ptr<TPBroadcastClient::Result>& tp_sync_result,
                                   const std::shared_ptr<PrefillLoadClient::Result>& prefill_load_result,
                                   const std::shared_ptr<P2PConnectorDecodeSchedulerMetricsCollector>& collector);
    virtual ~P2PConnectorDecodeAsyncContext();

public:
    bool done() const override;
    bool success() const override;
    void checkDone();

private:
    std::shared_ptr<KVCacheResourceV1>                           resource_;  // hold resource till done
    std::shared_ptr<TPBroadcastClient::Result>                   tp_sync_result_;
    std::shared_ptr<PrefillLoadClient::Result>                   prefill_load_result_;
    std::shared_ptr<P2PConnectorDecodeSchedulerMetricsCollector> collector_;
};

class P2PConnectorDecodeScheduler {
public:
    P2PConnectorDecodeScheduler(const GptInitParameter&             gpt_init_parameter,
                                const kmonitor::MetricsReporterPtr& metrics_reporter);
    ~P2PConnectorDecodeScheduler();

public:
    bool init();

    std::shared_ptr<P2PConnectorDecodeAsyncContext> asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                              int64_t                                   request_id,
                                                              const std::string&                        unique_key,
                                                              const std::string&                        prefill_ip,
                                                              uint32_t                                  prefill_port,
                                                              int64_t                                   deadline_ms);

private:
    void checkOnce();

private:
    const GptInitParameter&            gpt_init_parameter_;
    kmonitor::MetricsReporterPtr       metrics_reporter_;
    std::shared_ptr<TPBroadcastClient> tp_broadcast_client_;
    std::shared_ptr<PrefillLoadClient> prefill_load_client_;

    mutable std::mutex                                           async_contexts_mutex_;
    std::vector<std::shared_ptr<P2PConnectorDecodeAsyncContext>> async_contexts_;
    autil::LoopThreadPtr                                         check_done_thread_;
};

}  // namespace rtp_llm
