#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorDecodeScheduler.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/disaggregate/transfer/LayerCacheBufferUtil.h"
#include "rtp_llm/cpp/model_rpc/RPCPool.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include <memory>

namespace rtp_llm {

P2PConnectorDecodeAsyncContext::P2PConnectorDecodeAsyncContext(
    const std::shared_ptr<KVCacheResourceV1>&                           resource,
    const std::shared_ptr<TPBroadcastClient::Result>&                   tp_sync_result,
    const std::shared_ptr<PrefillLoadClient::Result>&                   prefill_load_result,
    const std::shared_ptr<P2PConnectorDecodeSchedulerMetricsCollector>& collector):
    resource_(resource),
    tp_sync_result_(tp_sync_result),
    prefill_load_result_(prefill_load_result),
    collector_(collector) {}

P2PConnectorDecodeAsyncContext::~P2PConnectorDecodeAsyncContext() {}

bool P2PConnectorDecodeAsyncContext::done() const {
    return tp_sync_result_->done() && prefill_load_result_->done();
}

bool P2PConnectorDecodeAsyncContext::success() const {
    return tp_sync_result_->success() && prefill_load_result_->success();
}

void P2PConnectorDecodeAsyncContext::checkDone() {
    if (!tp_sync_result_->done()) {
        tp_sync_result_->checkDone();
    }
    if (!prefill_load_result_->done()) {
        prefill_load_result_->checkDone();
    }
    if (done()) {
        collector_->success                   = success();
        collector_->total_cost_time_us        = currentTimeUs() - collector_->start_time_us;
        collector_->tp_sync_cost_time_us      = tp_sync_result_->totalCostTimeUs();
        collector_->prefill_load_cost_time_us = prefill_load_result_->totalCostTimeUs();
    }
}

P2PConnectorDecodeScheduler::P2PConnectorDecodeScheduler(const GptInitParameter&             gpt_init_parameter,
                                                         const kmonitor::MetricsReporterPtr& metrics_reporter):
    gpt_init_parameter_(gpt_init_parameter), metrics_reporter_(metrics_reporter) {}

P2PConnectorDecodeScheduler::~P2PConnectorDecodeScheduler() {
    if (check_done_thread_) {
        check_done_thread_->stop();
    }
}

bool P2PConnectorDecodeScheduler::init() {
    tp_broadcast_client_ = std::make_shared<TPBroadcastClient>(gpt_init_parameter_);
    if (!tp_broadcast_client_) {
        RTP_LLM_LOG_ERROR("P2PConnectorDecodeScheduler init failed: tp_broadcast_client is null");
        return false;
    }
    if (!tp_broadcast_client_->init()) {
        RTP_LLM_LOG_ERROR("P2PConnectorDecodeScheduler init failed: tp_broadcast_client init failed");
        return false;
    }

    auto rpc_pool = std::make_shared<RPCPool>();
    if (!rpc_pool) {
        RTP_LLM_LOG_ERROR("P2PConnectorDecodeScheduler init failed: rpc_pool is null");
        return false;
    }

    prefill_load_client_ = std::make_shared<PrefillLoadClient>(gpt_init_parameter_);
    if (!prefill_load_client_) {
        RTP_LLM_LOG_ERROR("P2PConnectorDecodeScheduler init failed: prefill_load_client is null");
        return false;
    }

    check_done_thread_ = autil::LoopThread::createLoopThread(std::bind(&P2PConnectorDecodeScheduler::checkOnce, this),
                                                             10 * 1000,  // 10ms
                                                             "P2PConnectorDecodeSchedulerCheckOnceThread");
    if (!check_done_thread_) {
        RTP_LLM_LOG_ERROR("P2PConnectorDecodeScheduler init failed: check_once_thread is null");
        return false;
    }
    return true;
}

std::shared_ptr<P2PConnectorDecodeAsyncContext>
P2PConnectorDecodeScheduler::asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource,
                                       int64_t                                   request_id,
                                       const std::string&                        unique_key,
                                       const std::string&                        prefill_ip,
                                       uint32_t                                  prefill_port,
                                       int64_t                                   deadline_ms) {
    auto collector = std::make_shared<P2PConnectorDecodeSchedulerMetricsCollector>(metrics_reporter_);
    if (!resource) {
        RTP_LLM_LOG_WARNING("P2PConnectorDecodeScheduler asyncRead: resource is null");
        collector->success = false;
        return nullptr;
    }

    auto layer_cache_buffers = LayerCacheBufferUtil::convert(*resource, 0);
    if (layer_cache_buffers.empty()) {
        RTP_LLM_LOG_WARNING("P2PConnectorDecodeScheduler asyncRead: layer_cache_buffers is empty");
        collector->success = false;
        return nullptr;
    }
    RTP_LLM_LOG_INFO("P2PConnectorDecodeScheduler asyncRead: layer_cache_buffers: %zu", layer_cache_buffers.size());

    // 失败概率大的先执行
    auto prefill_load_result =
        prefill_load_client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms);
    if (!prefill_load_result) {
        RTP_LLM_LOG_WARNING("P2PConnectorDecodeScheduler asyncRead: load failed");
        collector->success = false;
        return nullptr;
    }
    RTP_LLM_LOG_INFO("P2PConnectorDecodeScheduler asyncRead: prefill_load_result: %p", prefill_load_result.get());

    // 先执行 TP broadcast
    auto tp_sync_result = tp_broadcast_client_->broadcast(request_id, layer_cache_buffers, {}, unique_key, deadline_ms);
    if (!tp_sync_result) {
        collector->success = false;
        RTP_LLM_LOG_WARNING("P2PConnectorDecodeScheduler asyncRead: broadcast failed");
        return nullptr;
    }
    RTP_LLM_LOG_INFO("P2PConnectorDecodeScheduler asyncRead: tp_sync_result: %p", tp_sync_result.get());

    auto async_context =
        std::make_shared<P2PConnectorDecodeAsyncContext>(resource, tp_sync_result, prefill_load_result, collector);
    {
        std::lock_guard<std::mutex> lock(async_contexts_mutex_);
        async_contexts_.push_back(async_context);
    }
    return async_context;
}

void P2PConnectorDecodeScheduler::checkOnce() {
    int64_t start_time_us = currentTimeUs();

    std::lock_guard<std::mutex> lock(async_contexts_mutex_);
    for (auto& async_context : async_contexts_) {
        async_context->checkDone();
    }
    async_contexts_.erase(
        std::remove_if(async_contexts_.begin(),
                       async_contexts_.end(),
                       [](const std::shared_ptr<P2PConnectorDecodeAsyncContext>& async_context) -> bool {
                           return async_context->done();
                       }),
        async_contexts_.end());

    if (metrics_reporter_) {
        auto collector                     = std::make_shared<P2PConnectorDecodeSchedulerStatusMetricsCollector>();
        collector->check_once_cost_time_us = currentTimeUs() - start_time_us;
        collector->inflight_context_count  = async_contexts_.size();
        metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorDecodeSchedulerStatusMetricsCollector>(
            nullptr, collector.get());
    }
}

}  // namespace rtp_llm
