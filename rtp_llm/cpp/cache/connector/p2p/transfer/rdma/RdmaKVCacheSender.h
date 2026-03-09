#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheSender.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/RdmaInterface.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/proto/service.pb.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/TcpClient.h"

#include "kmonitor/client/MetricsReporter.h"

#include <cstdint>
#include <memory>
#include <string>

namespace rtp_llm::transfer::rdma {

/// RDMA KV cache sender.
///
/// Workflow:
///   1) Caller calls regMem() for each local block to register it as an RDMA MR.
///   2) Caller calls send() with a SendRequest.  The sender builds an RdmaTransferRequestPB
///      (containing RDMA addresses/rkeys of the local blocks) and sends it via TCP ARPC to
///      the receiver's RdmaSignalService.
///   3) The receiver's service issues an RDMA Read from the sender's RDMA server
///      to pull the data into the receiver's pre-registered buffers.
///   4) The ARPC response arrives after the RDMA Read completes; the callback is invoked.
class RdmaKVCacheSender final: public ::rtp_llm::transfer::IKVCacheSender {
public:
    explicit RdmaKVCacheSender(kmonitor::MetricsReporterPtr metrics_reporter = nullptr);
    ~RdmaKVCacheSender() override = default;

    /// Initialize RDMA server + TCP client.
    /// @param rdma_ip        Sender's own RDMA-reachable IP (sent to receiver).
    /// @param rdma_port      RDMA listen port (0 = let OS pick).
    /// @param rdma_io_threads Number of RDMA IO threads.
    /// @param rpc_io_threads  Number of ARPC client IO threads.
    bool init(const std::string& rdma_ip, uint32_t rdma_port, int rdma_io_threads, int rpc_io_threads);

    // IKVCacheSender
    bool regMem(const BlockInfo& block_info, uint64_t aligned_size = 0) override;
    void send(const SendRequestPtr& request, std::function<void(ErrorCode, const std::string&)> callback) override;

private:
    std::shared_ptr<::transfer::rdma::RdmaTransferRequestPB> makeTransferRequest(const SendRequestPtr& request);

    std::pair<ErrorCode, std::string>
    validate(const SendRequestPtr& request, int64_t& block_count, int64_t& total_block_size);

    void callSend(const SendRequestPtr& request, std::function<void(ErrorCode, const std::string&)> callback);

private:
    std::string                                     rdma_ip_;
    std::shared_ptr<IRdmaMemoryManager>             memory_manager_;
    std::shared_ptr<IRdmaServer>                    rdma_server_;
    std::shared_ptr<::rtp_llm::transfer::TcpClient> tcp_client_;
    kmonitor::MetricsReporterPtr                    metrics_reporter_;
};

}  // namespace rtp_llm::transfer::rdma
