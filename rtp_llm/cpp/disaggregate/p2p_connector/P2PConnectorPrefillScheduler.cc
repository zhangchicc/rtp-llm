#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorPrefillScheduler.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/cache_new/TpBroadcastManager.h"
#include "rtp_llm/cpp/disaggregate/transfer/LayerCacheBufferUtil.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorMetrics.h"
#include <grpc++/grpc++.h>

namespace rtp_llm {

P2PConnectorPrefillScheduler::P2PConnectorPrefillScheduler(const GptInitParameter&             gpt_init_parameter,
                                                           const kmonitor::MetricsReporterPtr& metrics_reporter):
    gpt_init_parameter_(gpt_init_parameter), metrics_reporter_(metrics_reporter) {}

P2PConnectorPrefillScheduler::~P2PConnectorPrefillScheduler() {}

bool P2PConnectorPrefillScheduler::init() {
    tp_broadcast_client_ = std::make_shared<TPBroadcastClient>(gpt_init_parameter_);
    if (!tp_broadcast_client_->init()) {
        RTP_LLM_LOG_ERROR("P2PConnectorPrefillScheduler init failed: tp_broadcast_client is null");
        return false;
    }
    RTP_LLM_LOG_INFO("P2PConnectorPrefillScheduler init success");
    return true;
}

grpc::Status
P2PConnectorPrefillScheduler::write(const std::shared_ptr<KVCacheResourceV1>&            resource,
                                    const std::string&                                   unique_key,
                                    int64_t                                              request_id,
                                    const std::vector<std::pair<std::string, uint32_t>>& decode_transfer_servers,
                                    int64_t                                              deadline_ms) {
    int64_t start_time_us = currentTimeUs();
    auto    collector     = std::make_shared<P2PConnectorPrefillSchedulerMetricsCollector>();

    // 转换为 layer_cache_buffers
    auto layer_cache_buffers = LayerCacheBufferUtil::convert(*resource, 0);
    if (layer_cache_buffers.empty()) {
        RTP_LLM_LOG_WARNING("P2PConnectorPrefillScheduler write: layer_cache_buffers is empty");
        if (metrics_reporter_) {
            collector->success = false;
            metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorPrefillSchedulerMetricsCollector>(
                nullptr, collector.get());
        }
        return grpc::Status(grpc::StatusCode::INTERNAL, "layer_cache_buffers is empty");
    }

    // 调用 broadcast
    auto result = tp_broadcast_client_->broadcast(
        request_id, layer_cache_buffers, decode_transfer_servers, unique_key, deadline_ms);
    if (!result) {
        RTP_LLM_LOG_WARNING("P2PConnectorPrefillScheduler write: broadcast failed");
        if (metrics_reporter_) {
            collector->success = false;
            metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorPrefillSchedulerMetricsCollector>(
                nullptr, collector.get());
        }
        return grpc::Status(grpc::StatusCode::INTERNAL, "broadcast failed");
    }

    while (!result->done()) {
        result->checkDone();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    collector->total_cost_time_us = currentTimeUs() - start_time_us;
    collector->success            = result->success();

    if (!result->success()) {
        RTP_LLM_LOG_WARNING("P2PConnectorPrefillScheduler write: broadcast result wait done failed");
        return grpc::Status(grpc::StatusCode::INTERNAL, "broadcast result wait done failed");
    }
    RTP_LLM_LOG_INFO("P2PConnectorPrefillScheduler write: broadcast result wait done success");
    return grpc::Status::OK;
}

}  // namespace rtp_llm
