#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "autil/LoopThread.h"
#include "rtp_llm/cpp/cache/BatchKVCacheResource.h"
#include "rtp_llm/cpp/cache/CacheConfig.h"
#include "rtp_llm/cpp/cache/connector/AsyncContext.h"
#include "rtp_llm/cpp/cache/connector/KVCacheConnector.h"
#include "rtp_llm/cpp/cache/KVCacheAllocator.h"
#include "rtp_llm/cpp/config/ConfigModules.h"
#include "rtp_llm/cpp/model_rpc/proto/model_rpc_service.grpc.pb.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnector.h"

namespace rtp_llm {

class DeviceBase;
class KVCacheAllocator;
class KVCacheMemoryConnector;
class KVCacheConnectorReadWriteContext;

struct KVCacheConnectorControlParams {
    bool enable_memory_cache = false;
};

class KVCacheConnectorCoordinator: public std::enable_shared_from_this<KVCacheConnectorCoordinator> {
public:
    KVCacheConnectorCoordinator(const CacheConfig&                       cache_config,
                                const KVCacheConfig&                     kv_cache_config,
                                const RuntimeConfig&                     runtime_config,
                                const CacheStoreConfig&                  cache_store_config,
                                const ParallelismConfig&                 parallelism_config,
                                const PDSepConfig&                       pd_sep_config,
                                const ModelConfig&                       model_config,
                                const std::shared_ptr<KVCacheAllocator>& allocator,
                                rtp_llm::DeviceBase*                     device,
                                const kmonitor::MetricsReporterPtr&      metrics_reporter = nullptr);
    ~KVCacheConnectorCoordinator();

public:
    bool init();

    std::shared_ptr<AsyncContext> asyncRead(const KVCacheResourceV1&                     resource,
                                            const std::shared_ptr<KVCacheConnectorMeta>& meta,
                                            const KVCacheConnectorControlParams&         control_params);
    std::shared_ptr<AsyncContext> asyncWrite(const KVCacheResourceV1&                     resource,
                                             const std::shared_ptr<KVCacheConnectorMeta>& meta,
                                             const KVCacheConnectorControlParams&         control_params);
    std::shared_ptr<AsyncContext> asyncWriteByLayer(int                                          layer_id,
                                                    const KVCacheResourceV1&                     resource,
                                                    const std::shared_ptr<KVCacheConnectorMeta>& meta,
                                                    const KVCacheConnectorControlParams&         control_params);
    bool                          broadcastTp(const BroadcastTpRequestPB& request, BroadcastTpResponsePB& response);
    bool handleRead(const P2PConnectorStartLoadRequestPB& request, P2PConnectorStartLoadResponsePB& response);

private:
    bool initMemoryConnector();
    bool initP2PConnector();
    bool initUpdateThread();
    void updateOnce();

private:
    const CacheConfig                 cache_config_;
    const KVCacheConfig               kv_cache_config_;
    const RuntimeConfig               runtime_config_;
    const CacheStoreConfig            cache_store_config_;
    const ParallelismConfig           parallelism_config_;
    const PDSepConfig                 pd_sep_config_;
    const ModelConfig                 model_config_;
    std::shared_ptr<KVCacheAllocator> allocator_;
    rtp_llm::DeviceBase*              device_{nullptr};
    kmonitor::MetricsReporterPtr      metrics_reporter_;

    std::shared_ptr<KVCacheConnector> memory_connector_;
    std::shared_ptr<KVCacheConnector> remote_connector_;
    std::shared_ptr<P2PConnector>     p2p_connector_;

    std::map<ConnectorType, std::shared_ptr<KVCacheConnector>> connectors_;

    mutable std::mutex                                update_mutex_;
    std::list<std::shared_ptr<FusedAsyncReadContext>> fused_async_read_context_list_;
    std::list<std::shared_ptr<FusedAsyncContext>>     fused_async_write_context_list_;
    autil::LoopThreadPtr                              update_thread_;
    const int                                         update_interval_ms_{1};
    std::atomic<bool>                                 stop_{false};
};

}  // namespace rtp_llm
