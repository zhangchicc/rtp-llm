#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheReceiverService.h"

#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"
#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

#include <chrono>
#include <thread>

namespace rtp_llm::transfer::rdma {

RdmaKVCacheReceiverService::RdmaKVCacheReceiverService(std::shared_ptr<RdmaKVCacheTaskStore> task_store,
                                                       std::shared_ptr<IRdmaClient>          rdma_client,
                                                       kmonitor::MetricsReporterPtr          metrics_reporter):
    task_store_(std::move(task_store)),
    rdma_client_(std::move(rdma_client)),
    metrics_reporter_(std::move(metrics_reporter)) {}

bool RdmaKVCacheReceiverService::validate(const ::transfer::rdma::RdmaTransferRequestPB* request,
                                          int64_t&                                       block_count,
                                          int64_t&                                       total_block_size) const {
    if (!request || !request->has_unique_key() || request->blocks_size() <= 0 || !request->has_rdma_ip()
        || !request->has_rdma_port() || request->deadline_ms() <= 0) {
        return false;
    }
    for (int i = 0; i < request->blocks_size(); ++i) {
        const auto& kb = request->blocks(i);
        if (!kb.has_key()) {
            return false;
        }
        ++block_count;
        for (int j = 0; j < kb.blocks_size(); ++j) {
            total_block_size += static_cast<int64_t>(kb.blocks(j).len());
        }
    }
    return true;
}

bool RdmaKVCacheReceiverService::bufferMatch(const ::transfer::rdma::RdmaTransferRequestPB* request,
                                             const RdmaKVCacheRecvTaskPtr&                  task) const {
    const auto* expected = task->getExpectedBlockInfo().get();
    if (!expected) {
        return false;
    }

    // Check that the request has the same number of cache keys.
    if (static_cast<int>(expected->size()) != request->blocks_size()) {
        RTP_LLM_LOG_WARNING("RdmaKVCacheReceiverService bufferMatch failed: key count mismatch "
                            "expected %zu, got %d",
                            expected->size(),
                            request->blocks_size());
        return false;
    }

    // For each key in the request, verify it exists in expected and blocks match.
    for (int i = 0; i < request->blocks_size(); ++i) {
        const auto& req_kb = request->blocks(i);
        auto        exp_it = expected->find(req_kb.key());
        if (exp_it == expected->end()) {
            RTP_LLM_LOG_WARNING("RdmaKVCacheReceiverService bufferMatch: unexpected key %ld in request", req_kb.key());
            return false;
        }
        const auto& exp_blocks = exp_it->second;
        if (static_cast<int>(exp_blocks.size()) != req_kb.blocks_size()) {
            RTP_LLM_LOG_WARNING("RdmaKVCacheReceiverService bufferMatch: block count mismatch for key %ld: "
                                "expected %zu, got %d",
                                req_kb.key(),
                                exp_blocks.size(),
                                req_kb.blocks_size());
            return false;
        }
        for (int j = 0; j < req_kb.blocks_size(); ++j) {
            if (exp_blocks[j].size_bytes != req_kb.blocks(j).len()) {
                RTP_LLM_LOG_WARNING("RdmaKVCacheReceiverService bufferMatch: size mismatch for key %ld block %d: "
                                    "expected %zu, got %u",
                                    req_kb.key(),
                                    j,
                                    exp_blocks[j].size_bytes,
                                    req_kb.blocks(j).len());
                return false;
            }
        }
    }
    return true;
}

std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>
RdmaKVCacheReceiverService::buildLocalRemoteBuffers(const ::transfer::rdma::RdmaTransferRequestPB* request,
                                                    const RdmaKVCacheRecvTaskPtr&                  task) const {
    std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>> result;
    const auto*                                                        expected = task->getExpectedBlockInfo().get();

    for (int i = 0; i < request->blocks_size(); ++i) {
        const auto& req_kb     = request->blocks(i);
        const auto& exp_blocks = expected->at(req_kb.key());

        for (int j = 0; j < req_kb.blocks_size(); ++j) {
            const auto& exp_blk = exp_blocks[j];
            const auto& req_blk = req_kb.blocks(j);

            LocalBuffer local;
            local.addr   = exp_blk.addr;
            local.size   = exp_blk.size_bytes;
            local.is_gpu = exp_blk.is_cuda;

            auto nic_rkeys = std::make_shared<std::map<uint32_t, uint32_t>>();
            for (int k = 0; k < req_blk.nic_rkeys_size(); ++k) {
                const auto& rk            = req_blk.nic_rkeys(k);
                (*nic_rkeys)[rk.nic_id()] = rk.rkey();
            }
            auto remote =
                std::make_shared<RemoteBuffer>(static_cast<int64_t>(req_blk.addr()), req_blk.len(), nic_rkeys);

            result.emplace_back(local, remote);
        }
    }
    return result;
}

void RdmaKVCacheReceiverService::transfer(::google::protobuf::RpcController*             controller,
                                          const ::transfer::rdma::RdmaTransferRequestPB* request,
                                          ::transfer::rdma::RdmaTransferResponsePB*      response,
                                          ::google::protobuf::Closure*                   done) {
    (void)controller;

    if (!request || !request->has_unique_key() || request->blocks_size() <= 0 || !request->has_rdma_ip()
        || !request->has_rdma_port()) {
        response->set_error_code(::transfer::rdma::INVALID_PARAMS);
        response->set_error_message("invalid request");
        done->Run();
        return;
    }

    auto          collector     = std::make_shared<::rtp_llm::transfer::RecvMetricsCollector>();
    auto          start_time_us = currentTimeUs();
    const int64_t deadline_ms   = request->deadline_ms();

    // real_done captures done and is called asynchronously from the RDMA callback thread.
    auto real_done = [request, response, done, collector, start_time_us, reporter = metrics_reporter_](
                         ::transfer::rdma::RdmaErrorCodePB ec, const std::string& msg) {
        if (reporter) {
            collector->success    = (ec == ::transfer::rdma::ERROR_NONE);
            collector->latency_us = currentTimeUs() - start_time_us;
            reporter->report<::rtp_llm::transfer::TransferMetrics, ::rtp_llm::transfer::RecvMetricsCollector>(
                nullptr, collector.get());
        }
        response->set_error_code(ec);
        response->set_error_message(msg);
        if (ec != ::transfer::rdma::ERROR_NONE) {
            RTP_LLM_LOG_WARNING("RdmaKVCacheReceiverService recv failed, unique_key: %s, "
                                "error_code: %d, error_message: %s",
                                request->unique_key().c_str(),
                                static_cast<int>(ec),
                                msg.c_str());
        }
        done->Run();
    };

    if (!validate(request, collector->block_count, collector->total_block_size)) {
        real_done(::transfer::rdma::INVALID_PARAMS, "invalid request");
        return;
    }

    // Poll for the receive task (same pattern as TCP service).
    RdmaKVCacheRecvTaskPtr task;
    while (currentTimeMs() < deadline_ms) {
        task = task_store_->getTask(request->unique_key());
        if (task) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    collector->wait_task_latency_us = currentTimeUs() - start_time_us;

    if (!task) {
        real_done(::transfer::rdma::NO_RECV_TASK, "timeout waiting for recv task");
        return;
    }

    using RecvErrorCode = ::rtp_llm::transfer::IKVCacheRecvTask::ErrorCode;

    if (!bufferMatch(request, task)) {
        task->markFailed(RecvErrorCode::RECV_FAILED, "buffer layout mismatch");
        real_done(::transfer::rdma::BUFFER_MISMATCH, "buffer layout mismatch");
        return;
    }

    if (!task->markProcessing()) {
        // Task was cancelled before we could start processing.
        real_done(::transfer::rdma::CANCELLED, "task already terminal");
        return;
    }

    auto local_remote = buildLocalRemoteBuffers(request, task);

    auto rdma_conn = rdma_client_->getConnection(request->rdma_ip(), request->rdma_port());
    if (!rdma_conn) {
        task->markFailed(RecvErrorCode::RECV_FAILED, "failed to get RDMA connection");
        real_done(::transfer::rdma::RDMA_FAILED, "failed to get RDMA connection");
        return;
    }

    // Initiate asynchronous RDMA read.  The lambda captures everything needed to finalise
    // the task and send the RPC response; it runs on an RDMA worker thread.
    rdma_conn->read(
        local_remote,
        [task, real_done](bool ok) {
            using RecvEC = ::rtp_llm::transfer::IKVCacheRecvTask::ErrorCode;
            if (ok) {
                task->markSuccess();
            } else {
                task->markFailed(RecvEC::RECV_FAILED, "rdma read failed");
            }
            auto result = task->getResult();
            real_done(result.success ? ::transfer::rdma::ERROR_NONE : ::transfer::rdma::RDMA_FAILED,
                      result.error_message);
        },
        static_cast<uint64_t>(deadline_ms));
    // done is NOT called here; it will be called from the RDMA callback above.
}

}  // namespace rtp_llm::transfer::rdma
