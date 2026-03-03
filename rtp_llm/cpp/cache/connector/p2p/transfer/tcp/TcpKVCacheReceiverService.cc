#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheReceiverService.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/RecvContext.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

namespace rtp_llm::transfer::tcp {

TcpKVCacheReceiverService::TcpKVCacheReceiverService(std::shared_ptr<TcpKVCacheTaskStore> task_store,
                                                     kmonitor::MetricsReporterPtr         metrics_reporter):
    task_store_(std::move(task_store)),
    metrics_reporter_(std::move(metrics_reporter)),
    cuda_copy_util_(std::make_shared<CudaCopyUtil>()) {}

bool TcpKVCacheReceiverService::validate(const ::transfer::tcp::TransferRequestPB* request,
                                         int64_t&                                  block_count,
                                         int64_t&                                  total_block_size) const {
    if (!request || !request->has_unique_key() || request->blocks_size() <= 0 || request->deadline_ms() <= 0) {
        return false;
    }
    for (int i = 0; i < request->blocks_size(); ++i) {
        const auto& key_blocks = request->blocks(i);
        if (!key_blocks.has_key()) {
            return false;
        }
        ++block_count;
        for (int j = 0; j < key_blocks.blocks_size(); ++j) {
            total_block_size += static_cast<int64_t>(key_blocks.blocks(j).len());
        }
    }
    return true;
}

void TcpKVCacheReceiverService::transfer(::google::protobuf::RpcController*        controller,
                                         const ::transfer::tcp::TransferRequestPB* request,
                                         ::transfer::tcp::TransferResponsePB*      response,
                                         ::google::protobuf::Closure*              done) {
    (void)controller;
    if (!request || !request->has_unique_key() || request->blocks_size() <= 0) {
        response->set_error_code(::transfer::tcp::INVALID_PARAMS);
        response->set_error_message("invalid request");
        done->Run();
        return;
    }

    auto collector     = std::make_shared<RecvMetricsCollector>();
    auto start_time_us = currentTimeUs();

    auto real_done = [request, response, done, collector, start_time_us, metrics_reporter = metrics_reporter_](
                         ::transfer::tcp::ErrorCodePB ec, const std::string& msg) {
        if (metrics_reporter) {
            collector->success    = (ec == ::transfer::tcp::ERROR_NONE);
            collector->latency_us = currentTimeUs() - start_time_us;
            metrics_reporter->report<TransferMetrics, RecvMetricsCollector>(nullptr, collector.get());
        }

        response->set_error_code(ec);
        response->set_error_message(msg);

        if (ec != ::transfer::tcp::ERROR_NONE) {
            RTP_LLM_LOG_WARNING("TcpKVCacheReceiverService recv failed, unique_key: %s, ip: %s, port: %u, "
                                "error_code: %d, error_message: %s",
                                request->unique_key().c_str(),
                                request->has_ip() ? request->ip().c_str() : "",
                                request->has_port() ? request->port() : 0,
                                static_cast<int>(ec),
                                msg.c_str());
        }
        done->Run();
    };

    if (!validate(request, collector->block_count, collector->total_block_size)) {
        real_done(::transfer::tcp::INVALID_PARAMS, "invalid request");
        return;
    }

    auto context = std::make_shared<RecvContext>(request, real_done, cuda_copy_util_);
    while (!context->isTimeout()) {
        auto task = task_store_->getTask(context->getUniqueKey());
        if (task) {
            collector->wait_task_latency_us = currentTimeUs() - start_time_us;
            context->run(task);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    collector->wait_task_latency_us = currentTimeUs() - start_time_us;
    real_done(::transfer::tcp::NO_RECV_TASK, "timeout waiting recv task");
}

}  // namespace rtp_llm::transfer::tcp
