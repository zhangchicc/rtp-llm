#pragma once

#include "rtp_llm/cpp/cache/BatchKVCacheResource.h"
#include "rtp_llm/cpp/cache/connector/AsyncContext.h"
#include "rtp_llm/cpp/engine_base/stream/CompleteTokenIds.h"
#include "rtp_llm/cpp/core/Event.h"

namespace rtp_llm {

struct KVCacheConnectorMeta {
    int64_t                           request_id;
    std::string                       unique_key;
    std::string                       prefill_ip;
    uint32_t                          prefill_port;
    int64_t                           deadline_ms;
    std::shared_ptr<CompleteTokenIds> complete_token_ids;
    DeviceEventPtr                    attention_event;
};

class KVCacheConnector {
public:
    KVCacheConnector()          = default;
    virtual ~KVCacheConnector() = default;

public:
    class Meta {
    public:
        virtual ~Meta() = default;
    };

    virtual bool init() = 0;

public:
    virtual std::shared_ptr<AsyncMatchContext> asyncMatch(const std::shared_ptr<KVCacheResourceV1>&    resource,
                                                          const std::shared_ptr<KVCacheConnectorMeta>& meta)        = 0;
    virtual std::shared_ptr<AsyncContext>      asyncRead(const std::shared_ptr<KVCacheResourceV1>&    resource,
                                                         const std::shared_ptr<KVCacheConnectorMeta>& meta,
                                                         const std::shared_ptr<AsyncMatchContext>&    match_context,
                                                         const std::pair<int, int>&                   block_range)                    = 0;
    virtual std::shared_ptr<AsyncContext>      asyncWrite(const std::shared_ptr<KVCacheResourceV1>&    resource,
                                                          const std::shared_ptr<KVCacheConnectorMeta>& meta)        = 0;
    virtual std::shared_ptr<AsyncContext>      asyncWriteByLayer(int                                          layer_id,
                                                                 const std::shared_ptr<KVCacheResourceV1>&    resource,
                                                                 const std::shared_ptr<KVCacheConnectorMeta>& meta) = 0;
};

}  // namespace rtp_llm