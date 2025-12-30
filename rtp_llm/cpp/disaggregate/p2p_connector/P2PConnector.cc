#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnector.h"

#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorAsyncContext.h"
#include "rtp_llm/cpp/disaggregate/transfer/LayerCacheBuffer.h"
#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include <chrono>
#include <thread>

namespace rtp_llm {

P2PConnector::P2PConnector(const KVCacheConfig&                        cache_config,
                           const RuntimeConfig&                        runtime_config,
                           const CacheStoreConfig&                     cache_store_config,
                           const ParallelismConfig&                    parallelism_config,
                           const PDSepConfig&                          pd_sep_config,
                           const ModelConfig&                          model_config,
                           const std::shared_ptr<LayerBlockConvertor>& layer_block_convertor,
                           const kmonitor::MetricsReporterPtr&         metrics_reporter):
    cache_config_(cache_config),
    runtime_config_(runtime_config),
    cache_store_config_(cache_store_config),
    parallelism_config_(parallelism_config),
    pd_sep_config_(pd_sep_config),
    model_config_(model_config),
    layer_block_convertor_(layer_block_convertor),
    metrics_reporter_(metrics_reporter) {}

P2PConnector::~P2PConnector() = default;

bool P2PConnector::init() {
    RTP_LLM_LOG_INFO("P2PConnector init start");

    // 只有 tp_rank == 0 才创建 scheduler（用于调度）
    if (parallelism_config_.tp_rank == 0) {
        scheduler_ = std::make_shared<P2PConnectorScheduler>(runtime_config_, metrics_reporter_);
        if (!scheduler_->init()) {
            RTP_LLM_LOG_ERROR("P2PConnector init failed: scheduler init failed");
            return false;
        }
    }

    // 每个 rank 都创建 worker（用于实际的数据传输）
    worker_ = std::make_shared<P2PConnectorWorker>(cache_config_,
                                                   cache_store_config_,
                                                   parallelism_config_,
                                                   pd_sep_config_,
                                                   model_config_,
                                                   layer_block_convertor_,
                                                   metrics_reporter_);
    if (!worker_->init()) {
        RTP_LLM_LOG_ERROR("P2PConnector init failed: worker init failed");
        return false;
    }

    // 创建 stream store（用于管理 stream）
    stream_store_ = std::make_shared<P2PConnectorStreamStore>(metrics_reporter_);
    if (!stream_store_->init()) {
        RTP_LLM_LOG_ERROR("P2PConnector init failed: stream_store init failed");
        return false;
    }

    RTP_LLM_LOG_INFO("P2PConnector init success");
    return true;
}

std::shared_ptr<AsyncMatchContext> P2PConnector::asyncMatch(const std::shared_ptr<KVCacheResourceV1>&    resource,
                                                            const std::shared_ptr<KVCacheConnectorMeta>& meta) {
    if (meta->unique_key.empty() || meta->prefill_ip.empty() || meta->prefill_port == 0) {
        RTP_LLM_LOG_WARNING("P2PConnector asyncMatch failed, unique_key: %s, prefill_ip: %s, prefill_port: %d",
                            meta->unique_key.c_str(),
                            meta->prefill_ip.c_str(),
                            meta->prefill_port);
        return nullptr;
    }

    // P2PConnector 不需要 match，直接返回一个简单的 context
    return std::make_shared<P2PConnectorAsyncMatchContext>(resource);
}

std::shared_ptr<AsyncContext> P2PConnector::asyncRead(const std::shared_ptr<KVCacheResourceV1>&    resource,
                                                      const std::shared_ptr<KVCacheConnectorMeta>& meta,
                                                      const std::shared_ptr<AsyncMatchContext>&    match_context,
                                                      const std::pair<int, int>&                   block_range) {
    if (scheduler_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector read failed, scheduler not ready (only tp_rank 0 has scheduler)");
        return nullptr;
    }

    // TODO: support block range
    return scheduler_->asyncRead(resource,
                                 meta->request_id,
                                 meta->unique_key,
                                 meta->prefill_ip,
                                 meta->prefill_port,
                                 meta->deadline_ms,
                                 meta->complete_token_ids);
}

std::shared_ptr<AsyncContext> P2PConnector::asyncWrite(const std::shared_ptr<KVCacheResourceV1>&    resource,
                                                       const std::shared_ptr<KVCacheConnectorMeta>& meta) {
    RTP_LLM_LOG_ERROR("P2PConnector::asyncWrite not supported, use asyncWriteByLayer instead");
    return nullptr;
}

std::shared_ptr<AsyncContext> P2PConnector::asyncWriteByLayer(int                                          layer_id,
                                                              const std::shared_ptr<KVCacheResourceV1>&    resource,
                                                              const std::shared_ptr<KVCacheConnectorMeta>& meta) {
    if (worker_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector write by layer failed, worker not init");
        return nullptr;
    }

    // writeByLayer is called by each rank
    worker_->writeByLayer(layer_id, resource, meta->request_id, meta->attention_event);
    RTP_LLM_LOG_DEBUG("P2PConnector::asyncWriteByLayer: writeByLayer called, layer_id: %d", layer_id);

    return std::make_shared<P2PConnectorAsyncWriteByLayerContext>(resource);
}

grpc::Status P2PConnector::handleRead(const P2PConnectorStartLoadRequestPB& request,
                                      P2PConnectorStartLoadResponsePB&      response) {
    RTP_LLM_LOG_DEBUG("P2PConnector::handleRead start, unique_key: %s, deadline_ms: %lld",
                      request.unique_key().c_str(),
                      request.deadline_ms());
    // 从 request 中提取参数
    const std::string& unique_key  = request.unique_key();
    int64_t            deadline_ms = request.deadline_ms();

    // 构建 decode_transfer_servers
    std::vector<std::pair<std::string, uint32_t>> decode_transfer_servers;
    for (const auto& worker : request.workers()) {
        decode_transfer_servers.emplace_back(worker.ip(), worker.cache_store_port());
    }

    if (stream_store_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector handleRead failed, stream_store not init");
        response.set_success(false);
        return grpc::Status(grpc::StatusCode::INTERNAL, "stream_store not init");
    }

    // 等待获取资源
    std::shared_ptr<P2PConnectorResourceEntry> resource_entry;
    while (currentTimeMs() < deadline_ms) {
        resource_entry = stream_store_->stealResource(unique_key);
        if (resource_entry) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!resource_entry) {
        RTP_LLM_LOG_ERROR("P2PConnector::handleRead failed: resource not found, unique_key: %s", unique_key.c_str());
        response.set_success(false);
        return grpc::Status(grpc::StatusCode::INTERNAL, "resource not found");
    }

    // 执行 handleRead 操作 (发送 KV cache 到 decode 端)
    int64_t request_id = resource_entry->request_id;
    bool    success    = scheduler_->handleRead(
        resource_entry->kv_cache_resource, unique_key, request_id, decode_transfer_servers, deadline_ms);
    if (!success) {
        RTP_LLM_LOG_ERROR("P2PConnector::handleRead failed: worker handleRead failed, unique_key: %s",
                          unique_key.c_str());
        response.set_success(false);
        return grpc::Status(grpc::StatusCode::INTERNAL, "worker handleRead failed");
    }

    RTP_LLM_LOG_DEBUG("P2PConnector::handleRead resource_entry use_count: %zu, resource use_count: %zu",
                      resource_entry.use_count(),
                      resource_entry->kv_cache_resource.use_count());

    // 获取 first_generate_token_id（最后一个 token）
    int first_token = 0;
    if (resource_entry->complete_token_ids) {
        auto tokens = resource_entry->complete_token_ids->currentExecuteTokens(0);
        if (!tokens.empty()) {
            first_token = tokens.back();
        }
    }

    response.set_success(true);
    response.set_first_generate_token_id(first_token);
    return grpc::Status::OK;
}

void P2PConnector::addResource(const std::string&                        unique_key,
                               int64_t                                   request_id,
                               const std::shared_ptr<ICompleteTokenIds>& complete_token_ids,
                               const std::shared_ptr<KVCacheResourceV1>& kv_cache_resource,
                               int64_t                                   deadline_ms) {
    if (stream_store_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector addResource failed, stream_store not init");
        return;
    }
    stream_store_->addResource(unique_key, request_id, complete_token_ids, kv_cache_resource, deadline_ms);
}

bool P2PConnector::handleTpBroadcast(const BroadcastTpRequestPB request, BroadcastTpResponsePB& response) {
    if (worker_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector handleTpBroadcast failed, worker not init");
        return false;
    }

    if (!request.has_p2p_request()) {
        RTP_LLM_LOG_WARNING("P2PConnector handleTpBroadcast failed, no p2p_request in BroadcastTpRequestPB");
        return false;
    }

    const auto& p2p_request = request.p2p_request();
    int64_t     request_id  = p2p_request.request_id();
    std::string unique_key  = p2p_request.unique_key();
    int64_t     deadline_ms = p2p_request.deadline_ms();

    if (p2p_request.type() == P2PConnectorBroadcastType::HANDLE_READ) {
        // Prefill 端: handleRead 请求
        std::vector<std::pair<std::string, uint32_t>> decode_transfer_servers;
        for (const auto& peer_worker : p2p_request.peer_workers()) {
            decode_transfer_servers.emplace_back(peer_worker.ip(), peer_worker.cache_store_port());
        }
        bool ret = worker_->handleRead(request_id, unique_key, deadline_ms, decode_transfer_servers);
        response.mutable_p2p_response()->set_success(ret);
        return ret;
    } else if (p2p_request.type() == P2PConnectorBroadcastType::READ) {
        // Decode 端: read 请求
        std::vector<std::shared_ptr<LayerCacheBuffer>> layer_cache_buffers;
        for (const auto& layer_block_pb : p2p_request.layer_blocks()) {
            auto layer_id           = layer_block_pb.layer_id();
            auto layer_cache_buffer = std::make_shared<LayerCacheBuffer>(layer_id);
            auto cache_keys         = layer_block_pb.cache_keys();
            auto block_ids          = layer_block_pb.block_ids();
            if (cache_keys.size() != block_ids.size()) {
                RTP_LLM_LOG_WARNING("P2PConnector handleTpBroadcast: cache_keys and block_ids size mismatch");
                response.mutable_p2p_response()->set_success(false);
                return false;
            }
            for (size_t i = 0; i < cache_keys.size(); i++) {
                layer_cache_buffer->addBlockId(cache_keys[i], block_ids[i]);
            }
            layer_cache_buffers.push_back(layer_cache_buffer);
        }
        bool ret = worker_->read(request_id, unique_key, deadline_ms, layer_cache_buffers);
        response.mutable_p2p_response()->set_success(ret);
        return ret;
    } else {
        RTP_LLM_LOG_WARNING("P2PConnector handleTpBroadcast failed, unknown p2p_request type");
        response.mutable_p2p_response()->set_success(false);
        return false;
    }
}

}  // namespace rtp_llm
