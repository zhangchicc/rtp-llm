#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnector.h"

#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorAsyncContext.h"
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

std::shared_ptr<AsyncMatchContext> P2PConnector::asyncMatch(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                            const std::shared_ptr<Meta>&              meta) {
    // P2PConnector 不需要 match，直接返回一个简单的 context
    return std::make_shared<P2PConnectorAsyncMatchContext>(resource);
}

std::shared_ptr<AsyncContext> P2PConnector::asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                      const std::shared_ptr<Meta>&              meta,
                                                      const std::shared_ptr<AsyncMatchContext>& match_context,
                                                      const std::pair<int, int>&                block_range) {
    if (scheduler_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector read failed, scheduler not ready (only tp_rank 0 has scheduler)");
        return nullptr;
    }

    auto read_meta = std::dynamic_pointer_cast<P2PConnectorReadMeta>(meta);
    if (!read_meta) {
        RTP_LLM_LOG_WARNING("P2PConnector read failed, meta type error");
        return nullptr;
    }

    // TODO: support block range
    return scheduler_->asyncRead(resource,
                                 read_meta->request_id,
                                 read_meta->unique_key,
                                 read_meta->prefill_ip,
                                 read_meta->prefill_port,
                                 read_meta->deadline_ms);
}

std::shared_ptr<AsyncContext> P2PConnector::asyncWrite(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                       const std::shared_ptr<Meta>&              meta) {
    RTP_LLM_LOG_ERROR("P2PConnector::asyncWrite not supported, use asyncWriteByLayer instead");
    return nullptr;
}

std::shared_ptr<AsyncContext> P2PConnector::asyncWriteByLayer(int                                       layer_id,
                                                              const std::shared_ptr<KVCacheResourceV1>& resource,
                                                              const std::shared_ptr<Meta>&              meta) {
    if (worker_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector write by layer failed, worker not init");
        return nullptr;
    }

    auto write_meta = std::dynamic_pointer_cast<P2PConnectorWriteMeta>(meta);
    if (!write_meta) {
        RTP_LLM_LOG_WARNING("P2PConnector write by layer failed, meta type error");
        return nullptr;
    }

    // writeByLayer is called by each rank
    worker_->writeByLayer(layer_id, resource, write_meta->request_id, write_meta->event);
    RTP_LLM_LOG_DEBUG("P2PConnector::asyncWriteByLayer: writeByLayer called, layer_id: %d", layer_id);

    return std::make_shared<P2PConnectorAsyncWriteByLayerContext>(resource);
}

grpc::Status P2PConnector::handleRead(const std::shared_ptr<KVCacheResourceV1>&            resource,
                                      const std::string&                                   unique_key,
                                      int64_t                                              request_id,
                                      const std::vector<std::pair<std::string, uint32_t>>& decode_transfer_servers,
                                      int64_t                                              deadline_ms) {
    if (stream_store_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector handleWrite failed, stream_store not init");
        return grpc::Status(grpc::StatusCode::INTERNAL, "stream_store not init");
    }

    // 等待获取 stream
    std::shared_ptr<GenerateStream> stream;
    while (currentTimeMs() < deadline_ms) {
        stream = stream_store_->stealStream(unique_key);
        if (stream) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (!stream) {
        RTP_LLM_LOG_ERROR("P2PConnector::handleWrite failed: stream is null");
        return grpc::Status(grpc::StatusCode::INTERNAL, "stream is null");
    }

    if (worker_ == nullptr) {
        RTP_LLM_LOG_ERROR("P2PConnector::handleWrite failed: worker is null");
        return grpc::Status(grpc::StatusCode::INTERNAL, "worker is null");
    }

    // 执行 write 操作
    bool success = worker_->handleRead(request_id, unique_key, deadline_ms, decode_transfer_servers);
    if (!success) {
        RTP_LLM_LOG_ERROR("P2PConnector::handleWrite failed: worker write failed");
        return grpc::Status(grpc::StatusCode::INTERNAL, "worker write failed");
    }

    return grpc::Status::OK;
}

void P2PConnector::addStream(const std::string& unique_key, GenerateStreamPtr stream) {
    if (stream_store_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector addStream failed, stream_store not init");
        return;
    }
    stream_store_->addStream(unique_key, stream);
}

std::shared_ptr<TPBroadcastService::Callback> P2PConnector::makeCallback() {
    if (worker_ == nullptr) {
        RTP_LLM_LOG_WARNING("P2PConnector makeCallback failed, worker not init");
        return nullptr;
    }
    return std::make_shared<P2PConnectorWorkerTPCallback>(worker_);
}

}  // namespace rtp_llm
