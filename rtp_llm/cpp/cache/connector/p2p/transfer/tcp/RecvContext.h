#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheRecvTask.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/proto/service.pb.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/CudaCopyUtil.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

#include <functional>
#include <memory>
#include <string>

namespace rtp_llm::transfer::tcp {

class RecvContext {
public:
    using DoneFn = std::function<void(::transfer::tcp::ErrorCodePB, std::string)>;

    RecvContext(const ::transfer::tcp::TransferRequestPB* request,
                DoneFn                                    done,
                const std::shared_ptr<CudaCopyUtil>&      cuda_copy_util);
    ~RecvContext() = default;

public:
    const std::string& getUniqueKey() const {
        return request_->unique_key();
    }
    int64_t getDeadlineMs() const {
        return request_->deadline_ms();
    }
    bool isTimeout() const {
        return currentTimeMs() > request_->deadline_ms();
    }

    void run(const std::shared_ptr<TcpKVCacheRecvTask>& task);

private:
    bool bufferMatch(const std::shared_ptr<TcpKVCacheRecvTask>& task) const;
    bool copyBuffer(const std::shared_ptr<TcpKVCacheRecvTask>& task);

private:
    const ::transfer::tcp::TransferRequestPB* request_;
    DoneFn                                    done_;
    std::shared_ptr<CudaCopyUtil>             cuda_copy_util_;
};

}  // namespace rtp_llm::transfer::tcp
