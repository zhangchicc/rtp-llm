#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorScheduler.h"

#include "rtp_llm/cpp/disaggregate/transfer/LayerCacheBufferUtil.h"
#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include <memory>
#include <thread>
#include <chrono>

namespace rtp_llm {

P2PConnectorScheduler::P2PConnectorScheduler(const RuntimeConfig&                runtime_config,
                                             const kmonitor::MetricsReporterPtr& metrics_reporter):
    runtime_config_(runtime_config), metrics_reporter_(metrics_reporter) {}

P2PConnectorScheduler::~P2PConnectorScheduler() {
    if (checker_) {
        checker_->stop();
    }
}

bool P2PConnectorScheduler::init() {
    // init tp broadcast client
    tp_broadcast_client_ = std::make_shared<TPBroadcastClient>(runtime_config_.worker_grpc_addrs);
    if (!tp_broadcast_client_) {
        RTP_LLM_LOG_ERROR("P2PConnectorScheduler init failed: tp_broadcast_client is null");
        return false;
    }
    if (!tp_broadcast_client_->init()) {
        RTP_LLM_LOG_ERROR("P2PConnectorScheduler init failed: tp_broadcast_client init failed");
        return false;
    }

    // init server caller (for decode side to call prefill)
    server_caller_ = std::make_shared<P2PConnectorServerCaller>(runtime_config_.worker_addrs);
    if (!server_caller_) {
        RTP_LLM_LOG_ERROR("P2PConnectorScheduler init failed: server_caller is null");
        return false;
    }

    // init checker for async read contexts
    checker_ = std::make_shared<P2PConnectorAsyncReadContextChecker>();
    if (!checker_->init(metrics_reporter_)) {
        RTP_LLM_LOG_ERROR("P2PConnectorScheduler init failed: checker init failed");
        return false;
    }

    RTP_LLM_LOG_INFO("P2PConnectorScheduler init success");
    return true;
}

std::shared_ptr<P2PConnectorAsyncReadContext>
P2PConnectorScheduler::asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource,
                                 int64_t                                   request_id,
                                 const std::string&                        unique_key,
                                 const std::string&                        prefill_ip,
                                 uint32_t                                  prefill_port,
                                 int64_t                                   deadline_ms,
                                 const std::shared_ptr<ICompleteTokenIds>& complete_token_ids) {
    auto collector = std::make_shared<P2PConnectorClientSchedulerMetricsCollector>(metrics_reporter_);
    if (!resource) {
        RTP_LLM_LOG_WARNING("P2PConnectorScheduler asyncRead: resource is null");
        collector->success = false;
        return nullptr;
    }

    // convert resource to layer cache buffers
    auto layer_cache_buffers = LayerCacheBufferUtil::convert(*resource, 0);
    if (layer_cache_buffers.empty()) {
        RTP_LLM_LOG_WARNING("P2PConnectorScheduler asyncRead: layer_cache_buffers is empty");
        collector->success = false;
        return nullptr;
    }

    // call prefill server to trigger write (higher failure probability, execute first)
    auto server_call_result =
        server_caller_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, complete_token_ids);
    if (!server_call_result) {
        RTP_LLM_LOG_WARNING("P2PConnectorScheduler asyncRead: server_caller load failed");
        collector->success = false;
        return nullptr;
    }

    // broadcast to all TP workers
    auto tp_sync_result = tp_broadcast_client_->broadcast(
        request_id, layer_cache_buffers, {}, unique_key, deadline_ms, P2PConnectorBroadcastType::READ);
    if (!tp_sync_result) {
        collector->success = false;
        RTP_LLM_LOG_WARNING("P2PConnectorScheduler asyncRead: broadcast failed");
        return nullptr;
    }

    // create async context and add to checker
    auto async_context =
        std::make_shared<P2PConnectorAsyncReadContext>(resource, tp_sync_result, server_call_result, collector);
    checker_->addContext(async_context);

    return async_context;
}

grpc::Status
P2PConnectorScheduler::handleRead(const std::shared_ptr<KVCacheResourceV1>&            resource,
                                  const std::string&                                   unique_key,
                                  int64_t                                              request_id,
                                  const std::vector<std::pair<std::string, uint32_t>>& decode_transfer_servers,
                                  int64_t                                              deadline_ms) {
    int64_t start_time_us      = currentTimeUs();
    auto    collector          = std::make_shared<P2PConnectorServerSchedulerMetricsCollector>();
    auto    report_metric_func = [start_time_us, collector, metrics_reporter = metrics_reporter_](bool success) {
        collector->total_cost_time_us = currentTimeUs() - start_time_us;
        collector->success            = success;
        if (metrics_reporter) {
            metrics_reporter->report<P2PConnectorMetrics, P2PConnectorServerSchedulerMetricsCollector>(nullptr,
                                                                                                       collector.get());
        }
    };

    // convert resource to layer cache buffers
    auto layer_cache_buffers = LayerCacheBufferUtil::convert(*resource, 0);
    if (layer_cache_buffers.empty()) {
        RTP_LLM_LOG_WARNING("P2PConnectorScheduler handleWrite: layer_cache_buffers is empty");
        report_metric_func(false);
        return grpc::Status(grpc::StatusCode::INTERNAL, "layer_cache_buffers is empty");
    }

    // broadcast to all TP workers with decode transfer servers
    auto result = tp_broadcast_client_->broadcast(request_id,
                                                  layer_cache_buffers,
                                                  decode_transfer_servers,
                                                  unique_key,
                                                  deadline_ms,
                                                  P2PConnectorBroadcastType::HANDLE_READ);
    if (!result) {
        RTP_LLM_LOG_WARNING("P2PConnectorScheduler handleWrite: broadcast failed");
        report_metric_func(false);
        return grpc::Status(grpc::StatusCode::INTERNAL, "broadcast failed");
    }

    // wait for broadcast to complete (sync call)
    while (!result->done()) {
        result->checkDone();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    report_metric_func(result->success());

    if (!result->success()) {
        RTP_LLM_LOG_WARNING("P2PConnectorScheduler handleWrite: broadcast result failed");
        return grpc::Status(grpc::StatusCode::INTERNAL, "broadcast result failed");
    }

    RTP_LLM_LOG_INFO("P2PConnectorScheduler handleWrite: success, request_id: %ld", request_id);
    return grpc::Status::OK;
}

}  // namespace rtp_llm
