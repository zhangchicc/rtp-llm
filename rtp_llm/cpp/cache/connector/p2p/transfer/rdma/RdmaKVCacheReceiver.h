#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheReceiver.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/RdmaInterface.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheTaskStore.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheReceiverService.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/TcpServer.h"

#include "kmonitor/client/MetricsReporter.h"

#include <memory>
#include <string>

namespace rtp_llm::transfer::rdma {

/// RDMA KV cache receiver.
///
/// Lifecycle:
///   1) init() – start RPC signal server (TcpServer) + RDMA client for outgoing reads.
///   2) regMem() – register each destination buffer as RDMA MR so the hardware can DMA into it.
///   3) recv()   – create a RdmaKVCacheRecvTask and park it in the task store; return the task.
///   4) The RdmaKVCacheReceiverService receives the sender's signal RPC and drives the RDMA read.
///   5) When the RDMA read completes, the task transitions to SUCCESS/FAILED.
///   6) Caller waits on task->waitUntil() and calls task->getResult().
class RdmaKVCacheReceiver final: public ::rtp_llm::transfer::IKVCacheReceiver {
public:
    explicit RdmaKVCacheReceiver(kmonitor::MetricsReporterPtr metrics_reporter = nullptr);
    ~RdmaKVCacheReceiver() override;

    /// Initialize all components.
    /// @param signal_listen_port  TCP port for the ARPC signal service (0 = OS-assigned).
    /// @param rdma_io_threads     RDMA client IO thread count.
    /// @param rpc_io_threads      TCP server IO thread count.
    /// @param rpc_worker_threads  TCP server worker thread count.
    bool init(uint32_t signal_listen_port, int rdma_io_threads, int rpc_io_threads, int rpc_worker_threads);

    std::string ip() const;
    uint32_t    port() const;

    // IKVCacheReceiver
    bool                regMem(const BlockInfo& block_info, uint64_t aligned_size = 0) override;
    IKVCacheRecvTaskPtr recv(const RecvRequestPtr& request) override;

private:
    std::shared_ptr<IRdmaMemoryManager>             memory_manager_;
    std::shared_ptr<IRdmaClient>                    rdma_client_;
    std::shared_ptr<::rtp_llm::transfer::TcpServer> server_;
    std::shared_ptr<RdmaKVCacheTaskStore>           task_store_;
    std::shared_ptr<RdmaKVCacheReceiverService>     service_;
    kmonitor::MetricsReporterPtr                    metrics_reporter_;
};

}  // namespace rtp_llm::transfer::rdma
