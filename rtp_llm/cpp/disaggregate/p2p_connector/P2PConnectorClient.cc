#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorClient.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorClientScheduler.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorClientWorker.h"

namespace rtp_llm {

P2PConnectorClient::P2PConnectorClient(const GptInitParameter&                  gpt_init_parameter,
                                       const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                                       const kmonitor::MetricsReporterPtr&      metrics_reporter):
    gpt_init_parameter_(gpt_init_parameter),
    kv_cache_allocator_(kv_cache_allocator),
    metrics_reporter_(metrics_reporter) {}

P2PConnectorClient::~P2PConnectorClient() {}

bool P2PConnectorClient::init() {
    if (gpt_init_parameter_.tp_rank_ == 0) {
        scheduler_ = std::make_shared<P2PConnectorClientScheduler>(gpt_init_parameter_, metrics_reporter_);
        if (!scheduler_->init()) {
            RTP_LLM_LOG_ERROR("P2PConnectorClient init failed: scheduler is null");
            return false;
        }
    }

    worker_ = std::make_shared<P2PConnectorClientWorker>(gpt_init_parameter_, kv_cache_allocator_, metrics_reporter_);
    if (!worker_->init()) {
        RTP_LLM_LOG_ERROR("P2PConnectorClient init failed: worker is null");
        return false;
    }

    RTP_LLM_LOG_INFO("P2PConnectorClient init success");
    return true;
}

std::shared_ptr<KVCacheConnector::AsyncContext>
P2PConnectorClient::asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource, const std::shared_ptr<Meta>& meta) {
    if (!scheduler_) {
        RTP_LLM_LOG_WARNING("P2PConnectorClient::asyncRead: scheduler is null, only rank 0 can call this");
        return nullptr;
    }

    auto decode_meta = std::dynamic_pointer_cast<P2PConnectorClientMeta>(meta);
    if (!decode_meta) {
        RTP_LLM_LOG_WARNING("P2PConnectorClient::asyncRead: meta is not P2PConnectorClientMeta");
        return nullptr;
    }

    auto async_context = scheduler_->asyncRead(resource,
                                               decode_meta->requestId(),
                                               decode_meta->uniqueKey(),
                                               decode_meta->prefillIp(),
                                               decode_meta->prefillPort(),
                                               decode_meta->deadlineMs());
    if (!async_context) {
        RTP_LLM_LOG_WARNING("P2PConnectorClient::asyncRead: scheduler_->asyncRead failed");
        return nullptr;
    }

    return async_context;
}

std::shared_ptr<KVCacheConnector::AsyncContext>
P2PConnectorClient::asyncWrite(const std::shared_ptr<KVCacheResourceV1>& resource, const std::shared_ptr<Meta>& meta) {
    RTP_LLM_LOG_ERROR("P2PConnectorClient::asyncWrite not supported");
    return nullptr;
}

std::shared_ptr<KVCacheConnector::AsyncContext> P2PConnectorClient::asyncWriteByLayer(
    int layer_id, const std::shared_ptr<KVCacheResourceV1>& resource, const std::shared_ptr<Meta>& meta) {
    RTP_LLM_LOG_ERROR("P2PConnectorClient::asyncWriteByLayer not supported");
    return nullptr;
}

std::shared_ptr<TPBroadcastService::Callback> P2PConnectorClient::makeCallback() {
    if (!worker_) {
        RTP_LLM_LOG_ERROR("P2PConnectorClient::makeCallback: worker is null");
        return nullptr;
    }
    return std::make_shared<P2PConnectorClientWorkerTPCallback>(worker_);
}

}  // namespace rtp_llm
