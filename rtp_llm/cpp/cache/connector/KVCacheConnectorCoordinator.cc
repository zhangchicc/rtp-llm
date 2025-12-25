#include "rtp_llm/cpp/cache/connector/KVCacheConnectorCoordinator.h"

#include <utility>

#include "rtp_llm/cpp/cache/KVCacheAllocator.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnector.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/LayerBlockConvertorImpl.h"

namespace rtp_llm {

// --------------------------------- FusedAsyncContext ---------------------------------

FusedAsyncContext::FusedAsyncContext(const std::vector<std::shared_ptr<AsyncContext>>& contexts): contexts_(contexts) {}

bool FusedAsyncContext::done() const {
    for (const auto& context : contexts_) {
        if (context && !context->done()) {
            return false;
        }
    }
    return true;
}

bool FusedAsyncContext::success() const {
    for (const auto& context : contexts_) {
        if (context && !context->success()) {
            return false;
        }
    }
    return true;
}

// --------------------------------- FusedAsyncReadContext ---------------------------------

FusedAsyncReadContext::FusedAsyncReadContext(const std::shared_ptr<FusedAsyncContext>&    fused_match_context,
                                             const std::shared_ptr<KVCacheResourceV1>&    resource,
                                             const std::shared_ptr<KVCacheConnectorMeta>& meta):
    fused_match_context_(fused_match_context), resource_(resource), meta_(meta) {}

FusedAsyncReadContext::~FusedAsyncReadContext() {}

bool FusedAsyncReadContext::done() const {
    if (!fused_match_context_) {
        return true;
    }
    if (!fused_match_context_->done()) {
        return false;
    }
    if (!fused_match_context_->success()) {
        return true;
    }
    return fused_read_context_ && fused_read_context_->done();
}

bool FusedAsyncReadContext::success() const {
    return done() && (fused_match_context_ && fused_match_context_->success())
           && (!fused_read_context_ || fused_read_context_->success());
}

void FusedAsyncReadContext::setFusedReadContext(const std::shared_ptr<FusedAsyncContext>& fused_read_context) {
    fused_read_context_ = fused_read_context;
}

KVCacheConnectorCoordinator::KVCacheConnectorCoordinator(const CacheConfig&                       cache_config,
                                                         const KVCacheConfig&                     kv_cache_config,
                                                         const RuntimeConfig&                     runtime_config,
                                                         const CacheStoreConfig&                  cache_store_config,
                                                         const ParallelismConfig&                 parallelism_config,
                                                         const PDSepConfig&                       pd_sep_config,
                                                         const ModelConfig&                       model_config,
                                                         const std::shared_ptr<KVCacheAllocator>& allocator,
                                                         rtp_llm::DeviceBase*                     device,
                                                         const kmonitor::MetricsReporterPtr&      metrics_reporter):
    cache_config_(cache_config),
    kv_cache_config_(kv_cache_config),
    runtime_config_(runtime_config),
    cache_store_config_(cache_store_config),
    parallelism_config_(parallelism_config),
    pd_sep_config_(pd_sep_config),
    model_config_(model_config),
    allocator_(allocator),
    device_(device),
    metrics_reporter_(metrics_reporter) {}

KVCacheConnectorCoordinator::~KVCacheConnectorCoordinator() {
    stop_.store(true);
    while (true) {
        {
            std::lock_guard<std::mutex> lock(update_mutex_);
            if (fused_async_read_context_list_.empty() && fused_async_write_context_list_.empty()) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (update_thread_) {
        update_thread_->stop();
        update_thread_.reset();
    }
    connectors_.clear();
    memory_connector_.reset();
    remote_connector_.reset();
    p2p_connector_.reset();
}

bool KVCacheConnectorCoordinator::init() {
    if (kv_cache_config_.memory_block_cache_size_mb > 0) {
        if (!initMemoryConnector()) {
            RTP_LLM_LOG_ERROR("init memory connector failed");
            return false;
        }
    }

    if (pd_sep_config_.role_type == RoleType::PREFILL || pd_sep_config_.role_type == RoleType::DECODE) {
        if (!initP2PConnector()) {
            RTP_LLM_LOG_ERROR("init p2p connector failed");
            return false;
        }
    }
    if (!initUpdateThread()) {
        return false;
    }
    return true;
}

std::shared_ptr<AsyncContext>
KVCacheConnectorCoordinator::asyncRead(const KVCacheResourceV1&                     resource,
                                       const std::shared_ptr<KVCacheConnectorMeta>& meta,
                                       const KVCacheConnectorControlParams&         control_params) {
    if (stop_.load()) {
        return nullptr;
    }

    auto resource_ptr = allocator_->incrKVCacheRef(resource, resource.cacheKeys());

    std::vector<std::shared_ptr<AsyncContext>> contexts;
    contexts.reserve(connectors_.size());
    for (const auto& [type, connector] : connectors_) {
        if (!connector) {
            continue;
        }
        if (type == ConnectorType::Memory && control_params.enable_memory_cache) {
            auto match_context = connector->asyncMatch(resource_ptr, meta);
            if (match_context) {
                contexts.emplace_back(match_context);
            }
        }
        if (type == ConnectorType::P2P) {
            auto match_context = connector->asyncMatch(resource_ptr, meta);
            if (match_context) {
                contexts.emplace_back(match_context);
            }
        }
    }
    if (contexts.empty()) {
        return nullptr;
    }

    auto fused_match_context = std::make_shared<FusedAsyncContext>(contexts);
    auto deleter             = [allocator = allocator_, resource_ptr](FusedAsyncReadContext* context) {
        allocator->decrKVCacheRef(*resource_ptr);
        delete context;
    };
    std::shared_ptr<FusedAsyncReadContext> fused_read_context(
        new FusedAsyncReadContext(fused_match_context, resource_ptr, meta), deleter);
    {
        std::lock_guard<std::mutex> lock(update_mutex_);
        fused_async_read_context_list_.push_back(fused_read_context);
    }
    return fused_read_context;
}

std::shared_ptr<AsyncContext>
KVCacheConnectorCoordinator::asyncWrite(const KVCacheResourceV1&                     resource,
                                        const std::shared_ptr<KVCacheConnectorMeta>& meta,
                                        const KVCacheConnectorControlParams&         control_params) {
    if (stop_.load()) {
        return nullptr;
    }

    auto resource_ptr = allocator_->incrKVCacheRef(resource, resource.cacheKeys());

    std::vector<std::shared_ptr<AsyncContext>> write_contexts;
    for (const auto& [type, connector] : connectors_) {
        if (!connector) {
            continue;
        }
        if (type == ConnectorType::Memory && control_params.enable_memory_cache) {
            auto write_context = connector->asyncWrite(resource_ptr, meta);
            if (write_context) {
                write_contexts.emplace_back(write_context);
            }
        }
    }
    if (write_contexts.empty()) {
        return nullptr;
    }
    auto deleter = [allocator = allocator_, resource_ptr](FusedAsyncContext* context) {
        allocator->decrKVCacheRef(*resource_ptr);
        delete context;
    };
    std::shared_ptr<FusedAsyncContext> fused_write_context(new FusedAsyncContext(std::move(write_contexts)), deleter);
    {
        std::lock_guard<std::mutex> lock(update_mutex_);
        fused_async_write_context_list_.push_back(fused_write_context);
    }
    return fused_write_context;
}

std::shared_ptr<AsyncContext>
KVCacheConnectorCoordinator::asyncWriteByLayer(int                                          layer_id,
                                               const KVCacheResourceV1&                     resource,
                                               const std::shared_ptr<KVCacheConnectorMeta>& meta,
                                               const KVCacheConnectorControlParams&         control_params) {
    if (stop_.load()) {
        return nullptr;
    }

    auto resource_ptr = allocator_->incrKVCacheRef(resource, resource.cacheKeys());

    std::vector<std::shared_ptr<AsyncContext>> write_contexts;
    for (const auto& [type, connector] : connectors_) {
        if (!connector) {
            continue;
        }
        if (type == ConnectorType::P2P) {
            auto write_context = connector->asyncWriteByLayer(layer_id, resource_ptr, meta);
            if (write_context) {
                write_contexts.emplace_back(write_context);
            }
        }
    }
    if (write_contexts.empty()) {
        return nullptr;
    }
    auto deleter = [allocator = allocator_, resource_ptr](FusedAsyncContext* context) {
        allocator->decrKVCacheRef(*resource_ptr);
        delete context;
    };
    std::shared_ptr<FusedAsyncContext> fused_write_context(new FusedAsyncContext(std::move(write_contexts)), deleter);
    {
        std::lock_guard<std::mutex> lock(update_mutex_);
        fused_async_write_context_list_.push_back(fused_write_context);
    }
    return fused_write_context;
}

bool KVCacheConnectorCoordinator::initMemoryConnector() {
    const auto memory_block_cache_size_mb         = kv_cache_config_.memory_block_cache_size_mb;
    const auto memory_block_cache_sync_timeout_ms = kv_cache_config_.memory_block_cache_sync_timeout_ms;
    if (memory_block_cache_size_mb <= 0 || memory_block_cache_sync_timeout_ms <= 0) {
        RTP_LLM_LOG_WARNING(
            "init memory connector failed, memory size or sync timeout is invalid, memory size: %ld MB, sync timeout: %ld ms",
            memory_block_cache_size_mb,
            memory_block_cache_sync_timeout_ms);
        return false;
    }

    // TODO(LXQ): init memory connector

    connectors_[ConnectorType::Memory] = memory_connector_;
    return true;
}

bool KVCacheConnectorCoordinator::initP2PConnector() {
    auto layer_block_convertor = std::make_shared<LayerBlockConvertorImpl>(allocator_);
    p2p_connector_             = std::make_shared<P2PConnector>(kv_cache_config_,
                                                    runtime_config_,
                                                    cache_store_config_,
                                                    parallelism_config_,
                                                    pd_sep_config_,
                                                    model_config_,
                                                    layer_block_convertor,
                                                    metrics_reporter_);
    if (!p2p_connector_->init()) {
        RTP_LLM_LOG_ERROR("init p2p connector failed");
        return false;
    }
    connectors_[ConnectorType::P2P] = p2p_connector_;
    return true;
}

bool KVCacheConnectorCoordinator::initUpdateThread() {
    update_thread_ = autil::LoopThread::createLoopThread(
        [self = shared_from_this()]() { self->updateOnce(); }, update_interval_ms_, "CoordinatorUpdateThread");
    return update_thread_ != nullptr;
}

void KVCacheConnectorCoordinator::updateOnce() {
    std::lock_guard<std::mutex> lock(update_mutex_);
    for (auto it = fused_async_read_context_list_.begin(); it != fused_async_read_context_list_.end();) {
        auto fused_read_context = *it;
        if (fused_read_context->done()) {
            it = fused_async_read_context_list_.erase(it);
            continue;
        }
        if (fused_read_context->fusedMatchContext()->done() && fused_read_context->fusedReadContext() == nullptr) {
            if (!fused_read_context->fusedMatchContext()->success()) {
                // match failed, cancel
                it = fused_async_read_context_list_.erase(it);
                continue;
            }
            // match success, start read
            int  reuse_num      = fused_read_context->resource()->reuseBlocksNum();
            auto match_contexts = fused_read_context->fusedMatchContext()->contexts();
            std::vector<std::shared_ptr<AsyncContext>> connector_read_contexts;
            for (int i = 0; i < match_contexts.size(); i++) {
                auto match_context = std::dynamic_pointer_cast<AsyncMatchContext>(match_contexts.at(i));
                if (!match_context) {
                    continue;
                }
                if (match_context->matchedBlockCount() <= reuse_num) {
                    continue;
                }
                auto connector = connectors_.at(match_context->connectorType());
                auto connector_read_context =
                    connector->asyncRead(fused_read_context->resource(),
                                         fused_read_context->meta(),
                                         match_context,
                                         {reuse_num, match_context->matchedBlockCount() - reuse_num});
                if (connector_read_context) {
                    connector_read_contexts.emplace_back(connector_read_context);
                    reuse_num = match_context->matchedBlockCount();
                }
            }
            fused_read_context->setFusedReadContext(std::make_shared<FusedAsyncContext>(connector_read_contexts));
        }
        it++;
    }
    for (auto it = fused_async_write_context_list_.begin(); it != fused_async_write_context_list_.end();) {
        auto fused_write_context = *it;
        if (fused_write_context->done()) {
            it = fused_async_write_context_list_.erase(it);
            continue;
        }
        it++;
    }
}

bool KVCacheConnectorCoordinator::broadcastTp(const BroadcastTpRequestPB& request, BroadcastTpResponsePB& response) {
    if (stop_.load()) {
        return false;
    }

    if (request.has_p2p_request()) {
        if (!p2p_connector_) {
            RTP_LLM_LOG_WARNING("broadcast tp failed, p2p connector is null, request: [%s]",
                                request.DebugString().c_str());
            response.mutable_p2p_response()->set_success(false);
            return false;
        }
        return p2p_connector_->handleTpBroadcast(request, response);
    }

    if (request.has_mem_request()) {
        if (!memory_connector_) {
            RTP_LLM_LOG_WARNING("broadcast tp failed, memory connector is null, request: [%s]",
                                request.DebugString().c_str());
            response.mutable_mem_response()->set_success(false);
            return false;
        }
        // TODO(LXQ): broadcast tp for memory connector
        return false;
    } else {
        return false;
    }
}

bool KVCacheConnectorCoordinator::handleRead(const P2PConnectorStartLoadRequestPB& request,
                                             P2PConnectorStartLoadResponsePB&      response) {
    if (stop_.load()) {
        response.set_success(false);
        return false;
    }

    if (!p2p_connector_) {
        RTP_LLM_LOG_WARNING("handleRead failed, p2p connector is null");
        response.set_success(false);
        return false;
    }

    auto ret = p2p_connector_->handleRead(request, response);
    return ret.ok();
}

}  // namespace rtp_llm