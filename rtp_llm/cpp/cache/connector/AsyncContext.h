#pragma once

#include <memory>
#include <vector>

namespace rtp_llm {

class KVCacheResourceV1;

enum class ConnectorType {
    Memory = 0,
    Remote = 1,
    P2P    = 2
};

class AsyncContext {
public:
    AsyncContext()          = default;
    virtual ~AsyncContext() = default;

public:
    virtual bool done() const    = 0;
    virtual bool success() const = 0;
};

class AsyncMatchContext: public AsyncContext {
public:
    AsyncMatchContext()                             = default;
    ~AsyncMatchContext() override                   = default;
    virtual size_t        matchedBlockCount() const = 0;
    virtual ConnectorType connectorType() const     = 0;
};

class FusedAsyncContext: public AsyncContext {
public:
    FusedAsyncContext(const std::vector<std::shared_ptr<AsyncContext>>& contexts);
    ~FusedAsyncContext() override = default;

public:
    bool done() const override;
    bool success() const override;

    const std::vector<std::shared_ptr<AsyncContext>>& contexts() const {
        return contexts_;
    }

private:
    std::vector<std::shared_ptr<AsyncContext>> contexts_;
};

class FusedAsyncReadContext: public AsyncContext {
public:
    FusedAsyncReadContext(const std::shared_ptr<FusedAsyncContext>& fused_match_context,
                          const std::shared_ptr<KVCacheResourceV1>& resource);
    ~FusedAsyncReadContext() override;

public:
    bool done() const override;
    bool success() const override;
    void setFusedReadContext(const std::shared_ptr<FusedAsyncContext>& fused_read_context);
    const std::shared_ptr<FusedAsyncContext>& fusedMatchContext() const {
        return fused_match_context_;
    }
    const std::shared_ptr<FusedAsyncContext>& fusedReadContext() const {
        return fused_read_context_;
    }
    const std::shared_ptr<KVCacheResourceV1>& resource() const {
        return resource_;
    }

private:
    std::shared_ptr<FusedAsyncContext> fused_match_context_;
    std::shared_ptr<FusedAsyncContext> fused_read_context_;
    std::shared_ptr<KVCacheResourceV1> resource_;
};

}  // namespace rtp_llm