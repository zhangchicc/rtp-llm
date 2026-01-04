#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorServerWorker.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include "rtp_llm/cpp/disaggregate/transfer/LayerCacheBufferUtil.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/LayerBlockConvertorImpl.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorMetrics.h"
#include <thread>
#include <chrono>
#include <algorithm>

namespace rtp_llm {

P2PConnectorServerWorker::P2PConnectorServerWorker(const GptInitParameter&                  gpt_init_parameter,
                                                   const std::shared_ptr<KVCacheAllocator>& kv_cache_allocator,
                                                   const kmonitor::MetricsReporterPtr&      metrics_reporter):
    gpt_init_parameter_(gpt_init_parameter),
    kv_cache_allocator_(kv_cache_allocator),
    metrics_reporter_(metrics_reporter),
    asymmetric_tp_util_(std::make_shared<AsymmetricTpUtil>(gpt_init_parameter)),
    computed_buffers_(std::make_shared<ComputedLayerCacheBufferStore>()),
    load_contexts_(std::make_shared<PrefillWorkerLoadContextStore>()),
    store_wait_thread_stop_(false) {}

P2PConnectorServerWorker::~P2PConnectorServerWorker() {
    if (store_wait_thread_) {
        store_wait_thread_->stop();
    }
}

bool P2PConnectorServerWorker::init() {
    if (!kv_cache_allocator_) {
        RTP_LLM_LOG_ERROR("P2PConnectorServerWorker init failed: kv_cache_allocator is null");
        return false;
    }

    // init layer block converter
    auto layer_block_converter = std::make_shared<LayerBlockConvertorImpl>(kv_cache_allocator_);

    // init transfer client
    transfer_client_ = std::make_shared<TransferClient>(layer_block_converter, metrics_reporter_);
    if (!transfer_client_) {
        RTP_LLM_LOG_ERROR("P2PConnectorServerWorker init failed: transfer_client is null");
        return false;
    }
    if (!transfer_client_->init(gpt_init_parameter_.cache_store_config.cache_store_rdma_mode,
                                gpt_init_parameter_.cache_store_config.messager_io_thread_count,
                                gpt_init_parameter_.cache_store_config.messager_io_thread_count,
                                gpt_init_parameter_.cache_store_config.messager_worker_thread_count)) {
        RTP_LLM_LOG_ERROR("P2PConnectorServerWorker init failed: transfer_client init failed");
        return false;
    }

    // register buffers
    auto buffers = kv_cache_allocator_->getAllBuffers();
    for (auto& [buffer, size] : buffers) {
        if (!transfer_client_->registerUserMr(buffer, size)) {
            RTP_LLM_LOG_ERROR("P2PConnectorServerWorker init failed: register user mr failed, buffer: %p, size: %ld",
                              buffer->data(),
                              size);
            return false;
        }
    }

    // init store wait thread
    store_wait_thread_ =
        autil::LoopThread::createLoopThread(std::bind(&P2PConnectorServerWorker::storeWaitThreadProcess, this),
                                            100,
                                            "P2PConnectorServerWorkerStoreWaitThread");
    if (!store_wait_thread_) {
        RTP_LLM_LOG_ERROR("P2PConnectorServerWorker init failed: store_wait_thread is null");
        return false;
    }
    RTP_LLM_LOG_INFO("P2PConnectorServerWorker init success");
    return true;
}

bool P2PConnectorServerWorker::writeByLayer(int                                       layer_id,
                                            const std::shared_ptr<KVCacheResourceV1>& resource,
                                            int64_t                                   request_id,
                                            DeviceEventPtr                            event) {
    auto collector = std::make_shared<P2PConnectorServerWorkerStoreMetricsCollector>();

    auto layer_cache_buffer = LayerCacheBufferUtil::convert(*resource, 0, layer_id);
    if (!layer_cache_buffer) {
        RTP_LLM_LOG_ERROR("P2PConnectorServerWorker writeByLayer failed: layer_cache_buffer is null");
        if (metrics_reporter_) {
            collector->success = false;
            metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorServerWorkerStoreMetricsCollector>(
                nullptr, collector.get());
        }
        return false;
    }
    collector->total_block_count = layer_cache_buffer->blockIdMap().size();

    int64_t deadline_ms = currentTimeMs() + store_wait_timeout_ms_;
    {
        std::unique_lock<std::mutex> lock(store_wait_mutex_);
        store_wait_contexts_.emplace_back(request_id, event, layer_cache_buffer, deadline_ms, collector);
    }
    RTP_LLM_LOG_INFO("P2PConnectorServerWorker writeByLayer end, request_id: %ld, layer_id: %d", request_id, layer_id);
    return true;
}

void P2PConnectorServerWorker::storeWaitThreadProcess() {
    {
        std::unique_lock<std::mutex> lock(store_wait_mutex_);
        auto                         iter = store_wait_contexts_.begin();
        while (iter != store_wait_contexts_.end()) {
            auto& [request_id, device_event, computed_layer_cache_buffer, deadline_ms, collector] = *iter;
            int64_t current_time_ms                                                               = currentTimeMs();
            if (current_time_ms >= deadline_ms) {
                RTP_LLM_LOG_WARNING("store wait timeout, request_id: %ld, deadline_ms: %ld, current_time_ms: %ld",
                                    request_id,
                                    deadline_ms,
                                    current_time_ms);
                iter                               = store_wait_contexts_.erase(iter);
                collector->success                 = false;
                collector->store_wait_done_time_us = currentTimeUs() - collector->start_time_us;
                if (metrics_reporter_) {
                    metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorServerWorkerStoreMetricsCollector>(
                        nullptr, collector.get());
                }
                continue;
            }

            if (device_event == nullptr || device_event->checkReadiness()) {
                computed_buffers_->addBuffer(request_id, computed_layer_cache_buffer, deadline_ms);
                iter = store_wait_contexts_.erase(iter);
                if (metrics_reporter_) {
                    collector->store_wait_done_time_us = currentTimeUs() - collector->start_time_us;
                    metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorServerWorkerStoreMetricsCollector>(
                        nullptr, collector.get());
                }
            } else {
                ++iter;
            }
        }
    }

    // clear expired computed layer cache buffers
    computed_buffers_->checkTimeout();
    load_contexts_->checkTimeout();
    if (metrics_reporter_) {
        auto collector                    = std::make_shared<P2PConnectorServerWorkerStatusMetricsCollector>();
        collector->wait_store_event_count = store_wait_contexts_.size();
        collector->task_count             = load_contexts_->getContextsCount();
        collector->computed_request_count = computed_buffers_->getBuffersCount();
        metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorServerWorkerStatusMetricsCollector>(nullptr,
                                                                                                       collector.get());
    }
}

bool P2PConnectorServerWorker::write(int64_t                                              request_id,
                                     const std::string&                                   unique_key,
                                     int64_t                                              deadline_ms,
                                     const std::vector<std::pair<std::string, uint32_t>>& decode_transfer_servers) {
    int64_t start_time_us = currentTimeUs();
    auto    collector     = std::make_shared<P2PConnectorServerWorkerWriteMetricsCollector>();

    auto asymmetric_tp_contexts = asymmetric_tp_util_->handleAsymmetricTP(decode_transfer_servers);
    if (asymmetric_tp_contexts.empty()) {
        RTP_LLM_LOG_ERROR("P2PConnectorServerWorker write: asymmetric_tp_contexts is empty");
        if (metrics_reporter_) {
            collector->success = false;
            metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorServerWorkerWriteMetricsCollector>(
                nullptr, collector.get());
        }
        return false;
    }

    auto load_context = load_contexts_->addContext(
        request_id, unique_key, deadline_ms, asymmetric_tp_contexts, gpt_init_parameter_.num_layers_);

    // wait until computed layer cache buffer is ready
    std::shared_ptr<ComputedLayerCacheBuffer> computed_layer_cache_buffer = nullptr;
    while (!computed_layer_cache_buffer || !load_context->canceled() || !load_context->timeout()) {
        computed_layer_cache_buffer = computed_buffers_->getBuffer(request_id);
        if (computed_layer_cache_buffer) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    collector->first_layer_wait_time_us = currentTimeUs() - start_time_us;

    if (!computed_layer_cache_buffer) {
        if (metrics_reporter_) {
            collector->success = false;
            metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorServerWorkerWriteMetricsCollector>(
                nullptr, collector.get());
        }
        return false;
    }

    // wait until all transfers are started
    while (!load_context->isAllTransferStarted() && !load_context->canceled() && !load_context->timeout()) {
        std::vector<int> layer_ids;
        for (auto [layer_id, layer_cache_buffer] : computed_layer_cache_buffer->layer_cache_buffers) {
            for (size_t i = 0; i < asymmetric_tp_contexts.size(); i++) {
                auto id = layer_id * static_cast<int>(asymmetric_tp_contexts.size()) + static_cast<int>(i);
                if (!load_context->startTransfer(id)) {
                    continue;
                }
                transfer_client_->transfer(
                    asymmetric_tp_contexts[i].decode_ip,
                    asymmetric_tp_contexts[i].decode_port,
                    unique_key,
                    layer_cache_buffer,
                    static_cast<uint32_t>(asymmetric_tp_contexts[i].local_partition_count),
                    static_cast<uint32_t>(asymmetric_tp_contexts[i].local_partition_id),
                    static_cast<uint32_t>(asymmetric_tp_contexts[i].remote_partition_count),
                    static_cast<uint32_t>(asymmetric_tp_contexts[i].remote_partition_id),
                    [load_context, id](bool success) { load_context->notifyDone(id, success); },
                    deadline_ms - currentTimeMs());
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    collector->last_layer_wait_time_us = currentTimeUs() - start_time_us;

    // write task done , remove task from store
    load_contexts_->removeContext(request_id);

    // wait until all transfers are done
    while (!load_context->isAllTransfersDone()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (metrics_reporter_) {
        collector->success            = load_context->success();
        collector->total_cost_time_us = currentTimeUs() - start_time_us;
        metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorServerWorkerWriteMetricsCollector>(nullptr,
                                                                                                      collector.get());
    }
    return load_context->success();
}

void P2PConnectorServerWorker::cancelWrite(int64_t request_id, const std::string& unique_key) {
    auto load_context = load_contexts_->getContext(request_id);
    if (load_context) {
        load_context->setCanceled();
    }
}

P2PConnectorServerWorkerTPCallback::P2PConnectorServerWorkerTPCallback(
    const std::shared_ptr<P2PConnectorServerWorker>& p2p_connector_prefill_worker):
    p2p_connector_prefill_worker_(p2p_connector_prefill_worker) {}

bool P2PConnectorServerWorkerTPCallback::shouldProcess(const BroadcastTpRequestPB& request) {
    return request.has_p2p_request();
}

grpc::Status P2PConnectorServerWorkerTPCallback::onBroadcastTp(const BroadcastTpRequestPB& request,
                                                               BroadcastTpResponsePB&      response) {
    auto p2p_request  = request.p2p_request();
    auto request_id   = p2p_request.request_id();
    auto unique_key   = p2p_request.unique_key();
    auto deadline_ms  = p2p_request.deadline_ms();
    auto layer_blocks = p2p_request.layer_blocks();

    std::vector<std::pair<std::string, uint32_t>> decode_transfer_servers;
    for (const auto& peer_worker : p2p_request.peer_workers()) {
        decode_transfer_servers.push_back(std::make_pair(peer_worker.ip(), peer_worker.cache_store_port()));
    }
    bool success = p2p_connector_prefill_worker_->write(request_id, unique_key, deadline_ms, decode_transfer_servers);

    RTP_LLM_LOG_INFO("P2PConnectorServerWorkerTPCallback::onBroadcastTp: write success: %d", success);
    response.mutable_p2p_response()->set_success(success);
    return success ? grpc::Status::OK : grpc::Status(grpc::StatusCode::INTERNAL, "write failed");
}

}  // namespace rtp_llm
