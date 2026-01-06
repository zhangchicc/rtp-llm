#pragma once

#include "rtp_llm/cpp/cache/BatchKVCacheResource.h"
#include "rtp_llm/cpp/cache/connector/AsyncContext.h"
#include "rtp_llm/cpp/core/Event.h"
#include "rtp_llm/cpp/engine_base/stream/ReuseInfo.h"

namespace rtp_llm {

class ICompleteTokenIds {
public:
    virtual ~ICompleteTokenIds() = default;

public:
    virtual void             appendTokenId(int batch_id, int token_id) = 0;
    virtual std::vector<int> currentExecuteTokens(int batch_id)        = 0;
};

typedef std::shared_ptr<ICompleteTokenIds> ICompleteTokenIdsPtr;

struct KVCacheConnectorMeta {
    int64_t              request_id;
    std::string          unique_key;
    std::string          prefill_ip;
    uint32_t             prefill_port;
    int64_t              deadline_ms;
    ICompleteTokenIdsPtr complete_token_ids;
    DeviceEventPtr       attention_event;
    ReuseInfoPtr         reuse_info;
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