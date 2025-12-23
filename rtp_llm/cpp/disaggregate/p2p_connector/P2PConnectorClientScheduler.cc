#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorClientScheduler.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/disaggregate/transfer/LayerCacheBufferUtil.h"
#include "rtp_llm/cpp/model_rpc/RPCPool.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include <memory>

namespace rtp_llm {

P2PConnectorClientAsyncContext::P2PConnectorClientAsyncContext(
    const std::shared_ptr<KVCacheResourceV1>&                           resource,
    const std::shared_ptr<TPBroadcastClient::Result>&                   tp_sync_result,
    const std::shared_ptr<P2PConnectorServerCaller::Result>&            server_call_result,
    const std::shared_ptr<P2PConnectorClientSchedulerMetricsCollector>& collector):
    resource_(resource),
    tp_sync_result_(tp_sync_result),
    server_call_result_(server_call_result),
    collector_(collector) {}

P2PConnectorClientAsyncContext::~P2PConnectorClientAsyncContext() {}

bool P2PConnectorClientAsyncContext::done() const {
    return tp_sync_result_->done() && server_call_result_->done();
}

bool P2PConnectorClientAsyncContext::success() const {
    return tp_sync_result_->success() && server_call_result_->success();
}

void P2PConnectorClientAsyncContext::checkDone() {
    if (!tp_sync_result_->done()) {
        tp_sync_result_->checkDone();
    }
    if (!server_call_result_->done()) {
        server_call_result_->checkDone();
    }
    if (done()) {
        collector_->success                  = success();
        collector_->total_cost_time_us       = currentTimeUs() - collector_->start_time_us;
        collector_->tp_sync_cost_time_us     = tp_sync_result_->totalCostTimeUs();
        collector_->server_call_cost_time_us = server_call_result_->totalCostTimeUs();
    }
}

P2PConnectorClientScheduler::P2PConnectorClientScheduler(const GptInitParameter&             gpt_init_parameter,
                                                         const kmonitor::MetricsReporterPtr& metrics_reporter):
    gpt_init_parameter_(gpt_init_parameter), metrics_reporter_(metrics_reporter) {}

P2PConnectorClientScheduler::~P2PConnectorClientScheduler() {
    if (check_done_thread_) {
        check_done_thread_->stop();
    }
}

bool P2PConnectorClientScheduler::init() {
    tp_broadcast_client_ = std::make_shared<TPBroadcastClient>(gpt_init_parameter_.worker_grpc_addrs_);
    if (!tp_broadcast_client_) {
        RTP_LLM_LOG_ERROR("P2PConnectorClientScheduler init failed: tp_broadcast_client is null");
        return false;
    }
    if (!tp_broadcast_client_->init()) {
        RTP_LLM_LOG_ERROR("P2PConnectorClientScheduler init failed: tp_broadcast_client init failed");
        return false;
    }

    auto rpc_pool = std::make_shared<RPCPool>();
    if (!rpc_pool) {
        RTP_LLM_LOG_ERROR("P2PConnectorClientScheduler init failed: rpc_pool is null");
        return false;
    }

    server_caller_ = std::make_shared<P2PConnectorServerCaller>(gpt_init_parameter_);
    if (!server_caller_) {
        RTP_LLM_LOG_ERROR("P2PConnectorClientScheduler init failed: server_caller is null");
        return false;
    }

    check_done_thread_ = autil::LoopThread::createLoopThread(std::bind(&P2PConnectorClientScheduler::checkOnce, this),
                                                             10 * 1000,  // 10ms
                                                             "P2PConnectorClientSchedulerCheckOnceThread");
    if (!check_done_thread_) {
        RTP_LLM_LOG_ERROR("P2PConnectorClientScheduler init failed: check_once_thread is null");
        return false;
    }
    return true;
}

std::shared_ptr<P2PConnectorClientAsyncContext>
P2PConnectorClientScheduler::asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource,
                                       int64_t                                   request_id,
                                       const std::string&                        unique_key,
                                       const std::string&                        prefill_ip,
                                       uint32_t                                  prefill_port,
                                       int64_t                                   deadline_ms) {
    auto collector = std::make_shared<P2PConnectorClientSchedulerMetricsCollector>(metrics_reporter_);
    if (!resource) {
        RTP_LLM_LOG_WARNING("P2PConnectorClientScheduler asyncRead: resource is null");
        collector->success = false;
        return nullptr;
    }

    auto layer_cache_buffers = LayerCacheBufferUtil::convert(*resource, 0);
    if (layer_cache_buffers.empty()) {
        RTP_LLM_LOG_WARNING("P2PConnectorClientScheduler asyncRead: layer_cache_buffers is empty");
        collector->success = false;
        return nullptr;
    }
    RTP_LLM_LOG_INFO("P2PConnectorClientScheduler asyncRead: layer_cache_buffers: %zu", layer_cache_buffers.size());

    // 失败概率大的先执行
    auto server_call_result = server_caller_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms);
    if (!server_call_result) {
        RTP_LLM_LOG_WARNING("P2PConnectorClientScheduler asyncRead: load failed");
        collector->success = false;
        return nullptr;
    }
    RTP_LLM_LOG_INFO("P2PConnectorClientScheduler asyncRead: server_call_result: %p", server_call_result.get());

    // 先执行 TP broadcast
    auto tp_sync_result = tp_broadcast_client_->broadcast(request_id, layer_cache_buffers, {}, unique_key, deadline_ms);
    if (!tp_sync_result) {
        collector->success = false;
        RTP_LLM_LOG_WARNING("P2PConnectorClientScheduler asyncRead: broadcast failed");
        return nullptr;
    }
    RTP_LLM_LOG_INFO("P2PConnectorClientScheduler asyncRead: tp_sync_result: %p", tp_sync_result.get());

    auto async_context =
        std::make_shared<P2PConnectorClientAsyncContext>(resource, tp_sync_result, server_call_result, collector);
    {
        std::lock_guard<std::mutex> lock(async_contexts_mutex_);
        async_contexts_.push_back(async_context);
    }
    return async_context;
}

void P2PConnectorClientScheduler::checkOnce() {
    int64_t start_time_us = currentTimeUs();

    std::lock_guard<std::mutex> lock(async_contexts_mutex_);
    for (auto& async_context : async_contexts_) {
        async_context->checkDone();
    }
    async_contexts_.erase(
        std::remove_if(async_contexts_.begin(),
                       async_contexts_.end(),
                       [](const std::shared_ptr<P2PConnectorClientAsyncContext>& async_context) -> bool {
                           return async_context->done();
                       }),
        async_contexts_.end());

    if (metrics_reporter_) {
        auto collector                     = std::make_shared<P2PConnectorClientSchedulerStatusMetricsCollector>();
        collector->check_once_cost_time_us = currentTimeUs() - start_time_us;
        collector->inflight_context_count  = async_contexts_.size();
        metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorClientSchedulerStatusMetricsCollector>(
            nullptr, collector.get());
    }
}

}  // namespace rtp_llm
