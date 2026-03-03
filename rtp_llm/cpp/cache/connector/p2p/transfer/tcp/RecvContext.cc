#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/RecvContext.h"

#include "rtp_llm/cpp/utils/Logger.h"

#include <cstring>
#include <set>

namespace rtp_llm::transfer::tcp {

RecvContext::RecvContext(const ::transfer::tcp::TransferRequestPB* request,
                         DoneFn                                    done,
                         const std::shared_ptr<CudaCopyUtil>&      cuda_copy_util):
    request_(request), done_(std::move(done)), cuda_copy_util_(cuda_copy_util) {}

bool RecvContext::bufferMatch(const std::shared_ptr<TcpKVCacheRecvTask>& task) const {
    const auto& expected = task->getExpectedBlockInfo();
    if (static_cast<size_t>(request_->blocks_size()) < expected->size()) {
        RTP_LLM_LOG_WARNING("RecvContext bufferMatch failed, cache_key count mismatch, expected: %zu, actual: %zu",
                            expected->size(),
                            request_->blocks_size());
        return false;
    }

    std::set<int64_t> seen_keys;
    for (int i = 0; i < request_->blocks_size(); ++i) {
        const auto&   key_blocks = request_->blocks(i);
        const int64_t key        = key_blocks.key();

        const auto it = expected->find(key);
        if (it == expected->end()) {
            continue;
        }
        seen_keys.insert(key);
        const auto& expected_blocks = it->second;
        if (expected_blocks.size() > static_cast<size_t>(key_blocks.blocks_size())) {
            RTP_LLM_LOG_WARNING("RecvContext bufferMatch failed, block count mismatch, expected: %zu, actual: %zu",
                                expected_blocks.size(),
                                key_blocks.blocks_size());
            return false;
        }
        for (size_t j = 0; j < expected_blocks.size(); ++j) {
            if (expected_blocks[j].size_bytes != key_blocks.blocks(j).len()) {
                RTP_LLM_LOG_WARNING("RecvContext bufferMatch failed, len mismatch, expected: %zu, actual: %u",
                                    expected_blocks[j].size_bytes,
                                    key_blocks.blocks(j).len());
                return false;
            }
        }
    }
    if (seen_keys.size() != expected->size()) {
        RTP_LLM_LOG_WARNING("RecvContext bufferMatch failed, not all expected keys found in request, "
                            "expected key count: %zu, matched: %zu",
                            expected->size(),
                            seen_keys.size());
        return false;
    }
    return true;
}

bool RecvContext::copyBuffer(const std::shared_ptr<TcpKVCacheRecvTask>& task) {
    const auto&           expected = task->getExpectedBlockInfo();
    std::vector<CopyTask> gpu_copy_tasks;

    for (int i = 0; i < request_->blocks_size(); ++i) {
        const auto&   key_blocks = request_->blocks(i);
        const int64_t key        = key_blocks.key();

        const auto it = expected->find(key);
        if (it == expected->end()) {
            continue;
        }

        const auto& expected_blocks = it->second;
        for (size_t j = 0; j < expected_blocks.size(); ++j) {
            const auto&  src_block = key_blocks.blocks(j);
            const auto&  dst_block = expected_blocks[j];
            const void*  src_ptr   = src_block.content().data();
            const size_t size      = static_cast<size_t>(src_block.len());

            if (!dst_block.is_cuda) {
                std::memcpy(dst_block.addr, src_ptr, size);
            } else {
                gpu_copy_tasks.push_back(CopyTask{
                    .src_ptr = const_cast<void*>(src_ptr),
                    .size    = size,
                    .dst_ptr = static_cast<char*>(dst_block.addr),
                });
            }
        }
    }

    if (gpu_copy_tasks.empty()) {
        return true;
    }

    if (!cuda_copy_util_) {
        RTP_LLM_LOG_WARNING("RecvContext copyBuffer failed: cuda_copy_util_ is null but GPU blocks present");
        return false;
    }

    return cuda_copy_util_->batchCopyToDevice(gpu_copy_tasks);
}

void RecvContext::run(const std::shared_ptr<TcpKVCacheRecvTask>& task) {
    if (task == nullptr) {
        done_(::transfer::tcp::NO_RECV_TASK, "recv task is null");
        return;
    }

    if (!bufferMatch(task)) {
        task->markFailed(TcpKVCacheRecvTask::ErrorCode::BUFFER_MISMATCH, "buffer mismatch");
        done_(::transfer::tcp::BUFFER_MISMATCH, "buffer mismatch");
        return;
    }

    if (task->markProcessing()) {
        if (copyBuffer(task)) {
            task->markSuccess();
        } else {
            task->markFailed(TcpKVCacheRecvTask::ErrorCode::RECV_FAILED, "copy buffer failed");
        }
    }

    const auto result = task->getResult();
    if (result.success) {
        done_(::transfer::tcp::ERROR_NONE, "success");
    } else {
        ::transfer::tcp::ErrorCodePB rpc_code = ::transfer::tcp::FAILED;
        if (result.error_code == IKVCacheRecvTask::ErrorCode::CANCELLED) {
            rpc_code = ::transfer::tcp::CANCELLED;
        } else if (result.error_code == IKVCacheRecvTask::ErrorCode::TIMEOUT) {
            rpc_code = ::transfer::tcp::TIMEOUT;
        }
        done_(rpc_code, result.error_message.empty() ? "recv task finished with failure" : result.error_message);
    }
}

}  // namespace rtp_llm::transfer::tcp
