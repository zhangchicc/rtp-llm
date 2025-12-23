#pragma once

#include "rtp_llm/cpp/cache/KVCacheConnector.h"
#include "rtp_llm/cpp/config/GptInitParameter.h"
#include "rtp_llm/cpp/cache/KVCacheAllocator.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorClientScheduler.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorClientWorker.h"
#include "rtp_llm/cpp/cache/TpBroadcastManager.h"
#include <memory>

namespace rtp_llm {

class P2PConnectorClientMeta: public KVCacheConnector::Meta {
public:
    P2PConnectorClientMeta(int64_t            request_id,
                           const std::string& unique_key,
                           const std::string& prefill_ip,
                           uint32_t           prefill_port,
                           int64_t            deadline_ms):
        request_id_(request_id),
        unique_key_(unique_key),
        prefill_ip_(prefill_ip),
        prefill_port_(prefill_port),
        deadline_ms_(deadline_ms) {}
    ~P2PConnectorClientMeta() override = default;

public:
    int64_t requestId() const {
        return request_id_;
    }
    const std::string& uniqueKey() const {
        return unique_key_;
    }
    const std::string& prefillIp() const {
        return prefill_ip_;
    }
    uint32_t prefillPort() const {
        return prefill_port_;
    }
    int64_t deadlineMs() const {
        return deadline_ms_;
    }

private:
    int64_t     request_id_;
    std::string unique_key_;
    std::string prefill_ip_;
    uint32_t    prefill_port_;
    int64_t     deadline_ms_;
};

class P2PConnectorClient: public KVCacheConnector {

public:
    P2PConnectorClient(const GptInitParameter&                  gpt_init_parameter,
                       const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                       const kmonitor::MetricsReporterPtr&      metrics_reporter);
    virtual ~P2PConnectorClient();

public:
    bool                                            init() override;
    std::shared_ptr<KVCacheConnector::AsyncContext> asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                              const std::shared_ptr<Meta>&              meta) override;
    std::shared_ptr<KVCacheConnector::AsyncContext> asyncWrite(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                               const std::shared_ptr<Meta>&              meta) override;
    std::shared_ptr<KVCacheConnector::AsyncContext> asyncWriteByLayer(
        int layer_id, const std::shared_ptr<KVCacheResourceV1>& resource, const std::shared_ptr<Meta>& meta) override;

    std::shared_ptr<TPBroadcastService::Callback> makeCallback();

private:
    const GptInitParameter&                      gpt_init_parameter_;
    std::shared_ptr<KVCacheAllocator>            kv_cache_allocator_;
    kmonitor::MetricsReporterPtr                 metrics_reporter_;
    std::shared_ptr<P2PConnectorClientScheduler> scheduler_;
    std::shared_ptr<P2PConnectorClientWorker>    worker_;
};
}  // namespace rtp_llm
