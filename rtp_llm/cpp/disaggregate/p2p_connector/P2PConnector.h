#pragma once

#include "rtp_llm/cpp/cache/connector/KVCacheConnector.h"
#include "rtp_llm/cpp/cache/KVCacheAllocator.h"
#include "rtp_llm/cpp/config/ConfigModules.h"
#include "rtp_llm/cpp/core/Event.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorScheduler.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorStreamStore.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorWorker.h"
#include <grpc++/grpc++.h>
#include <memory>
#include <string>
#include <vector>

namespace rtp_llm {

// Decode 端用于 asyncRead 的 Meta 类
class P2PConnectorReadMeta: public KVCacheConnector::Meta {
public:
    P2PConnectorReadMeta(int64_t            request_id_in,
                         const std::string& unique_key_in,
                         const std::string& prefill_ip_in,
                         uint32_t           prefill_port_in,
                         int64_t            deadline_ms_in):
        request_id(request_id_in),
        unique_key(unique_key_in),
        prefill_ip(prefill_ip_in),
        prefill_port(prefill_port_in),
        deadline_ms(deadline_ms_in) {}

    ~P2PConnectorReadMeta() override = default;

public:
    int64_t     request_id;
    std::string unique_key;
    std::string prefill_ip;
    uint32_t    prefill_port;
    int64_t     deadline_ms;
};

// Prefill 端用于 asyncWriteByLayer 的 Meta 类
class P2PConnectorWriteMeta: public KVCacheConnector::Meta {
public:
    P2PConnectorWriteMeta(int64_t request_id, DeviceEventPtr event = nullptr): request_id(request_id), event(event) {}

    ~P2PConnectorWriteMeta() override = default;

public:
    int64_t        request_id;
    DeviceEventPtr event;
};

// 统一的 P2PConnector 类，合并 P2PConnectorClient 和 P2PConnectorServer 的功能
class P2PConnector: public KVCacheConnector {
public:
    P2PConnector(const KVCacheConfig&                        cache_config,
                 const RuntimeConfig&                        runtime_config,
                 const CacheStoreConfig&                     cache_store_config,
                 const ParallelismConfig&                    parallelism_config,
                 const PDSepConfig&                          pd_sep_config,
                 const ModelConfig&                          model_config,
                 const std::shared_ptr<LayerBlockConvertor>& layer_block_convertor,
                 const kmonitor::MetricsReporterPtr&         metrics_reporter);
    ~P2PConnector() override;

public:
    bool init() override;

public:
    // KVCacheConnector interface
    std::shared_ptr<AsyncMatchContext> asyncMatch(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                  const std::shared_ptr<Meta>&              meta) override;
    std::shared_ptr<AsyncContext>      asyncRead(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                 const std::shared_ptr<Meta>&              meta,
                                                 const std::shared_ptr<AsyncMatchContext>& match_context,
                                                 const std::pair<int, int>&                block_range) override;
    std::shared_ptr<AsyncContext>      asyncWrite(const std::shared_ptr<KVCacheResourceV1>& resource,
                                                  const std::shared_ptr<Meta>&              meta) override;
    std::shared_ptr<AsyncContext>      asyncWriteByLayer(int                                       layer_id,
                                                         const std::shared_ptr<KVCacheResourceV1>& resource,
                                                         const std::shared_ptr<Meta>&              meta) override;

public:
    // Prefill side: handle write request from decode side
    grpc::Status handleRead(const std::shared_ptr<KVCacheResourceV1>&            resource,
                            const std::string&                                   unique_key,
                            int64_t                                              request_id,
                            const std::vector<std::pair<std::string, uint32_t>>& decode_transfer_servers,
                            int64_t                                              deadline_ms);

    // Prefill side: reserve stream
    void addStream(const std::string& unique_key, GenerateStreamPtr stream);

    // Make TP broadcast callback
    std::shared_ptr<TPBroadcastService::Callback> makeCallback();

private:
    const KVCacheConfig&                 cache_config_;
    const RuntimeConfig&                 runtime_config_;
    const CacheStoreConfig&              cache_store_config_;
    const ParallelismConfig&             parallelism_config_;
    const PDSepConfig&                   pd_sep_config_;
    const ModelConfig&                   model_config_;
    std::shared_ptr<LayerBlockConvertor> layer_block_convertor_;
    kmonitor::MetricsReporterPtr         metrics_reporter_;

    std::shared_ptr<P2PConnectorScheduler>   scheduler_;
    std::shared_ptr<P2PConnectorWorker>      worker_;
    std::shared_ptr<P2PConnectorStreamStore> stream_store_;
};

}  // namespace rtp_llm
