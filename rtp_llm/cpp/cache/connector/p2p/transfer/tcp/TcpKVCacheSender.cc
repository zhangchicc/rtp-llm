#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheSender.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/SendClosure.h"

#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"
#include "rtp_llm/cpp/utils/ErrorCode.h"
#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include "rtp_llm/cpp/utils/AssertUtils.h"

#include <algorithm>
#include <cstring>

namespace rtp_llm::transfer::tcp {

TcpKVCacheSender::TcpKVCacheSender(kmonitor::MetricsReporterPtr metrics_reporter):
    metrics_reporter_(std::move(metrics_reporter)), cuda_copy_util_(std::make_unique<CudaCopyUtil>()) {}

bool TcpKVCacheSender::init(int io_thread_count) {
    tcp_client_ = std::make_shared<TcpClient>();
    if (!tcp_client_->init(io_thread_count)) {
        RTP_LLM_LOG_ERROR("TcpKVCacheSender init tcp client failed");
        tcp_client_.reset();
        return false;
    }
    RTP_LLM_LOG_INFO("TcpKVCacheSender init success, io thread count %d", io_thread_count);
    return true;
}

bool TcpKVCacheSender::regMem(const BlockInfo& block_info, uint64_t aligned_size) {
    (void)block_info;
    (void)aligned_size;
    return true;  // TCP no-op
}

std::pair<IKVCacheSender::ErrorCode, std::string>
TcpKVCacheSender::validate(const SendRequestPtr& request, int64_t& block_count, int64_t& total_block_size) {
    RTP_LLM_CHECK_WITH_INFO(tcp_client_ && cuda_copy_util_, "TcpKVCacheSender not initialized");

    if (!request->block_info) {
        return {ErrorCode::FAILED, "invalid request"};
    }

    if (request->deadline_ms <= currentTimeMs()) {
        return {ErrorCode::TIMEOUT, "deadline exceeded"};
    }

    if (request->ip.empty() || request->port == 0) {
        return {ErrorCode::FAILED, "invalid endpoint"};
    }

    if (request->block_info->empty()) {
        return {ErrorCode::NONE_ERROR, ""};
    }

    for (const auto& [key, blocks] : *request->block_info) {
        for (const auto& blk : blocks) {
            if (blk.addr == nullptr || blk.size_bytes == 0) {
                return {ErrorCode::FAILED, "key: " + std::to_string(key) + ", block address is null or size is 0"};
            }
            block_count += 1;
            total_block_size += blk.size_bytes;
        }
    }
    return {ErrorCode::NONE_ERROR, ""};
}

std::shared_ptr<::transfer::tcp::TransferRequestPB>
TcpKVCacheSender::makeTransferRequest(const SendRequestPtr& request) {
    auto req_pb = std::make_shared<::transfer::tcp::TransferRequestPB>();
    req_pb->set_unique_key(request->unique_key);
    req_pb->set_deadline_ms(request->deadline_ms);
    req_pb->set_ip(request->ip);
    req_pb->set_port(request->port);

    std::vector<CopyTask> copy_tasks;

    for (const auto& [key, blocks] : *request->block_info) {
        auto* blocks_pb = req_pb->add_blocks();
        blocks_pb->set_key(key);

        for (const auto& blk : blocks) {
            auto* b = blocks_pb->add_blocks();
            b->set_len(static_cast<uint32_t>(blk.size_bytes));
            auto* content = b->mutable_content();
            content->resize(blk.size_bytes);

            if (blk.is_cuda) {
                CopyTask task;
                task.src_ptr = blk.addr;
                task.size    = blk.size_bytes;
                task.dst_ptr = content->data();
                copy_tasks.push_back(task);
            } else {
                std::memcpy(content->data(), blk.addr, blk.size_bytes);
            }
        }
    }

    if (!copy_tasks.empty()) {
        if (!cuda_copy_util_->batchCopyToHost(copy_tasks)) {
            return nullptr;
        }
    }
    return req_pb;
}

void TcpKVCacheSender::callSend(const SendRequestPtr&                              request,
                                std::function<void(ErrorCode, const std::string&)> callback) {
    auto channel = tcp_client_->getChannel(request->ip, request->port);
    if (!channel) {
        callback(ErrorCode::FAILED, "get channel failed");
        return;
    }

    auto transfer_request = makeTransferRequest(request);
    if (!transfer_request) {
        callback(ErrorCode::FAILED, "make transfer request failed");
        return;
    }

    if (request->deadline_ms <= currentTimeMs()) {
        callback(ErrorCode::TIMEOUT, "deadline exceeded before rpc send");
        return;
    }

    auto closure = new SendClosure(request, transfer_request, callback);

    ::transfer::tcp::TcpTransferService_Stub stub((::google::protobuf::RpcChannel*)(channel.get()),
                                                  ::google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    stub.transfer(closure->getController(), transfer_request.get(), closure->getResponse(), closure);
}

void TcpKVCacheSender::send(const SendRequestPtr&                              request,
                            std::function<void(ErrorCode, const std::string&)> callback) {
    if (!request || !callback) {
        if (callback) {
            callback(ErrorCode::FAILED, "request or callback is null");
        }
        return;
    }

    auto collector     = std::make_shared<SendMetricsCollector>();
    auto start_time_us = currentTimeUs();

    auto real_callback = [callback, request, collector, start_time_us, metrics_reporter = metrics_reporter_](
                             ErrorCode ec, const std::string& msg) {
        if (metrics_reporter) {
            collector->success    = (ec == ErrorCode::NONE_ERROR);
            collector->latency_us = currentTimeUs() - start_time_us;
            metrics_reporter->report<TransferMetrics, SendMetricsCollector>(nullptr, collector.get());
        }

        if (ec != ErrorCode::NONE_ERROR) {
            RTP_LLM_LOG_WARNING(
                "TcpKVCacheSender send failed, unique_key: %s, ip: %s, port: %u, error_code: %d, error_message: %s",
                request->unique_key.c_str(),
                request->ip.c_str(),
                request->port,
                static_cast<int>(ec),
                msg.c_str());
        }
        callback(ec, msg);
    };

    int64_t block_count      = 0;
    int64_t total_block_size = 0;
    auto [ec, msg]           = validate(request, block_count, total_block_size);
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

}  // namespace rtp_llm::transfer::tcp
