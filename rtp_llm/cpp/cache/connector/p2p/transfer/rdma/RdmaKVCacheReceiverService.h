#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/RdmaInterface.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheTaskStore.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/proto/service.pb.h"

#include "kmonitor/client/MetricsReporter.h"

#include <memory>
#include <string>
#include <vector>

namespace rtp_llm::transfer::rdma {

/// RDMA signal RPC service.
///
/// Invoked by the sender's TcpClient.  Receives the RDMA block address/rkey info from
/// the sender, matches it against the pre-registered receive task, and initiates an
/// RDMA ReadBatch from the sender into the receiver's pre-registered local buffers.
///
/// The RPC done callback is deferred until the asynchronous RDMA read completes (i.e. the
/// function returns without calling done->Run(); done is called from the RDMA callback thread).
class RdmaKVCacheReceiverService final: public ::transfer::rdma::RdmaSignalService {
public:
    RdmaKVCacheReceiverService(std::shared_ptr<RdmaKVCacheTaskStore> task_store,
                               std::shared_ptr<IRdmaClient>          rdma_client,
                               kmonitor::MetricsReporterPtr          metrics_reporter = nullptr);
    ~RdmaKVCacheReceiverService() override = default;

    void transfer(::google::protobuf::RpcController*             controller,
                  const ::transfer::rdma::RdmaTransferRequestPB* request,
                  ::transfer::rdma::RdmaTransferResponsePB*      response,
                  ::google::protobuf::Closure*                   done) override;

private:
    bool validate(const ::transfer::rdma::RdmaTransferRequestPB* request,
                  int64_t&                                       block_count,
                  int64_t&                                       total_block_size) const;

    bool bufferMatch(const ::transfer::rdma::RdmaTransferRequestPB* request, const RdmaKVCacheRecvTaskPtr& task) const;

    std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>
    buildLocalRemoteBuffers(const ::transfer::rdma::RdmaTransferRequestPB* request,
                            const RdmaKVCacheRecvTaskPtr&                  task) const;

    std::shared_ptr<RdmaKVCacheTaskStore> task_store_;
    std::shared_ptr<IRdmaClient>          rdma_client_;
    kmonitor::MetricsReporterPtr          metrics_reporter_;
};

}  // namespace rtp_llm::transfer::rdma
