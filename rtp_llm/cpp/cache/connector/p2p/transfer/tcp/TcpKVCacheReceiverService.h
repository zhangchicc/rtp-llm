#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheTaskStore.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/proto/service.pb.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/CudaCopyUtil.h"

#include "kmonitor/client/MetricsReporter.h"

#include <memory>
#include <string>

namespace rtp_llm::transfer::tcp {

class TcpKVCacheReceiverService final: public ::transfer::tcp::TcpTransferService {
public:
    TcpKVCacheReceiverService(std::shared_ptr<TcpKVCacheTaskStore> task_store,
                              kmonitor::MetricsReporterPtr         metrics_reporter = nullptr);
    ~TcpKVCacheReceiverService() override = default;

public:
    void transfer(::google::protobuf::RpcController*        controller,
                  const ::transfer::tcp::TransferRequestPB* request,
                  ::transfer::tcp::TransferResponsePB*      response,
                  ::google::protobuf::Closure*              done) override;

private:
    bool
    validate(const ::transfer::tcp::TransferRequestPB* request, int64_t& block_count, int64_t& total_block_size) const;

    std::shared_ptr<TcpKVCacheTaskStore> task_store_;
    kmonitor::MetricsReporterPtr         metrics_reporter_;
    std::shared_ptr<CudaCopyUtil>        cuda_copy_util_;
};

}  // namespace rtp_llm::transfer::tcp
