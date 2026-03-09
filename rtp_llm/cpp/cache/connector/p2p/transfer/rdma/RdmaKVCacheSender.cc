#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheSender.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaSendClosure.h"

#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"
#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include "rtp_llm/cpp/utils/AssertUtils.h"

namespace rtp_llm::transfer::rdma {

RdmaKVCacheSender::RdmaKVCacheSender(kmonitor::MetricsReporterPtr metrics_reporter):
    metrics_reporter_(std::move(metrics_reporter)) {}

bool RdmaKVCacheSender::init(const std::string& rdma_ip, uint32_t rdma_port, int rdma_io_threads, int rpc_io_threads) {
    rdma_ip_        = rdma_ip;
    memory_manager_ = createRdmaMemoryManager();
    if (!memory_manager_) {
        RTP_LLM_LOG_ERROR("RdmaKVCacheSender: failed to create RDMA memory manager");
        return false;
    }

    rdma_server_ = createRdmaServer(memory_manager_, rdma_port, rdma_io_threads);
    if (!rdma_server_) {
        RTP_LLM_LOG_ERROR("RdmaKVCacheSender: failed to create RDMA server on port %u", rdma_port);
        return false;
    }

    tcp_client_ = std::make_shared<::rtp_llm::transfer::TcpClient>();
    if (!tcp_client_->init(rpc_io_threads)) {
        RTP_LLM_LOG_ERROR("RdmaKVCacheSender: failed to init TCP client");
        tcp_client_.reset();
        return false;
    }

    RTP_LLM_LOG_INFO(
        "RdmaKVCacheSender init success, rdma_ip: %s, rdma_port: %u", rdma_ip_.c_str(), rdma_server_->getListenPort());
    return true;
}

bool RdmaKVCacheSender::regMem(const BlockInfo& block_info, uint64_t aligned_size) {
    RTP_LLM_CHECK_WITH_INFO(memory_manager_ != nullptr, "RdmaKVCacheSender not initialized");
    return memory_manager_->regUserMr(block_info.addr, block_info.size_bytes, block_info.is_cuda, aligned_size);
}

std::pair<IKVCacheSender::ErrorCode, std::string>
RdmaKVCacheSender::validate(const SendRequestPtr& request, int64_t& block_count, int64_t& total_block_size) {
    RTP_LLM_CHECK_WITH_INFO(memory_manager_ && rdma_server_ && tcp_client_, "RdmaKVCacheSender not initialized");

    if (!request->block_info) {
        return {ErrorCode::FAILED, "invalid request: block_info is null"};
    }
    if (request->deadline_ms <= currentTimeMs()) {
        return {ErrorCode::TIMEOUT, "deadline exceeded"};
    }
    if (request->ip.empty() || request->port == 0) {
        return {ErrorCode::FAILED, "invalid receiver endpoint"};
    }
    if (request->block_info->empty()) {
        return {ErrorCode::NONE_ERROR, ""};
    }

    for (const auto& [key, blocks] : *request->block_info) {
        for (const auto& blk : blocks) {
            if (blk.addr == nullptr || blk.size_bytes == 0) {
                return {ErrorCode::FAILED, "key: " + std::to_string(key) + ", block addr is null or size is 0"};
            }
            block_count += 1;
            total_block_size += blk.size_bytes;
        }
    }
    return {ErrorCode::NONE_ERROR, ""};
}

std::shared_ptr<::transfer::rdma::RdmaTransferRequestPB>
RdmaKVCacheSender::makeTransferRequest(const SendRequestPtr& request) {
    auto req_pb = std::make_shared<::transfer::rdma::RdmaTransferRequestPB>();
    req_pb->set_unique_key(request->unique_key);
    req_pb->set_rdma_ip(rdma_ip_);
    req_pb->set_rdma_port(rdma_server_->getListenPort());
    req_pb->set_deadline_ms(request->deadline_ms);

    for (const auto& [key, blocks] : *request->block_info) {
        auto* key_blocks_pb = req_pb->add_blocks();
        key_blocks_pb->set_key(key);

        for (const auto& blk : blocks) {
            auto remote_buf = memory_manager_->findMemoryMr(blk.addr, blk.size_bytes, blk.is_cuda);
            if (!remote_buf) {
                RTP_LLM_LOG_WARNING("RdmaKVCacheSender makeTransferRequest: MR not found for addr %p, "
                                    "key: %ld. Was regMem called?",
                                    blk.addr,
                                    key);
                return nullptr;
            }

            auto* block_pb = key_blocks_pb->add_blocks();
            block_pb->set_addr(static_cast<uint64_t>(remote_buf->addr));
            block_pb->set_len(remote_buf->len);

            if (remote_buf->nic_rkeys) {
                for (const auto& [nic_id, rkey] : *remote_buf->nic_rkeys) {
                    auto* rkey_pb = block_pb->add_nic_rkeys();
                    rkey_pb->set_nic_id(nic_id);
                    rkey_pb->set_rkey(rkey);
                }
            }
        }
    }
    return req_pb;
}

void RdmaKVCacheSender::callSend(const SendRequestPtr&                              request,
                                 std::function<void(ErrorCode, const std::string&)> callback) {
    auto channel = tcp_client_->getChannel(request->ip, request->port);
    if (!channel) {
        callback(ErrorCode::FAILED, "get RPC channel failed");
        return;
    }

    auto transfer_request = makeTransferRequest(request);
    if (!transfer_request) {
        callback(ErrorCode::FAILED, "failed to build RDMA transfer request (MR not registered?)");
        return;
    }

    if (request->deadline_ms <= currentTimeMs()) {
        callback(ErrorCode::TIMEOUT, "deadline exceeded before RPC send");
        return;
    }

    auto                                     closure = new RdmaSendClosure(request, transfer_request, callback);
    ::transfer::rdma::RdmaSignalService_Stub stub(static_cast<::google::protobuf::RpcChannel*>(channel.get()),
                                                  ::google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    stub.transfer(closure->getController(), transfer_request.get(), closure->getResponse(), closure);
}

void RdmaKVCacheSender::send(const SendRequestPtr&                              request,
                             std::function<void(ErrorCode, const std::string&)> callback) {
    if (!request || !callback) {
        if (callback) {
            callback(ErrorCode::FAILED, "request or callback is null");
        }
        return;
    }

    auto collector     = std::make_shared<::rtp_llm::transfer::SendMetricsCollector>();
    auto start_time_us = currentTimeUs();

    auto real_callback = [callback, request, collector, start_time_us, reporter = metrics_reporter_](
                             ErrorCode ec, const std::string& msg) {
        if (reporter) {
            collector->success    = (ec == ErrorCode::NONE_ERROR);
            collector->latency_us = currentTimeUs() - start_time_us;
            reporter->report<::rtp_llm::transfer::TransferMetrics, ::rtp_llm::transfer::SendMetricsCollector>(
                nullptr, collector.get());
        }
        if (ec != ErrorCode::NONE_ERROR) {
            RTP_LLM_LOG_WARNING("RdmaKVCacheSender send failed, unique_key: %s, ip: %s, port: %u, "
                                "error_code: %d, error_message: %s",
                                request->unique_key.c_str(),
                                request->ip.c_str(),
                                request->port,
                                static_cast<int>(ec),
                                msg.c_str());
        }
        callback(ec, msg);
    };

    int64_t block_count = 0, total_block_size = 0;
    auto [ec, msg] = validate(request, block_count, total_block_size);
    if (ec != ErrorCode::NONE_ERROR) {
        real_callback(ec, msg);
        return;
    }
    collector->block_count      = block_count;
    collector->total_block_size = total_block_size;

    if (request->block_info->empty()) {
        real_callback(ErrorCode::NONE_ERROR, "");
        return;
    }

    callSend(request, real_callback);
}

}  // namespace rtp_llm::transfer::rdma
