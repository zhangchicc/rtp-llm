#include "rtp_llm/cpp/disaggregate/transfer/CudaCopyUtil.h"
#include "rtp_llm/cpp/utils/Logger.h"

namespace rtp_llm {

CudaCopyUtil::CudaCopyUtil() {
    cudaError_t err = cudaStreamCreate(&stream_);
    if (err != cudaSuccess) {
        RTP_LLM_LOG_WARNING("cudaStreamCreate failed: %s", cudaGetErrorString(err));
        stream_ = nullptr;
    }
}

CudaCopyUtil::~CudaCopyUtil() {
    if (stream_) {
        cudaStreamDestroy(stream_);
        stream_ = nullptr;
    }
}

bool CudaCopyUtil::batchCopyToHost(std::vector<CopyTask>& tasks) {
    if (tasks.empty()) {
        return true;
    }

    if (!stream_) {
        RTP_LLM_LOG_WARNING("CUDA stream is not initialized");
        return false;
    }

    // 1. 提交所有 cudaMemcpyAsync
    for (auto& task : tasks) {
        if (!task.dst_ptr) {
            RTP_LLM_LOG_WARNING("dst_ptr is nullptr, caller must pre-allocate dst_ptr");
            return false;
        }

        cudaError_t err = cudaMemcpyAsync(task.dst_ptr, task.src_ptr, task.size, cudaMemcpyDeviceToHost, stream_);
        if (err != cudaSuccess) {
            RTP_LLM_LOG_WARNING("cudaMemcpyAsync (D2H) failed: %s", cudaGetErrorString(err));
            return false;
        }
    }

    // 2. 同步等待所有拷贝完成
    cudaError_t err = cudaStreamSynchronize(stream_);
    if (err != cudaSuccess) {
        RTP_LLM_LOG_WARNING("cudaStreamSynchronize failed: %s", cudaGetErrorString(err));
        return false;
    }

    return true;
}

bool CudaCopyUtil::batchCopyToDevice(std::vector<CopyTask>& tasks) {
    if (tasks.empty()) {
        return true;
    }

    if (!stream_) {
        RTP_LLM_LOG_WARNING("CUDA stream is not initialized");
        return false;
    }

    // 1. 提交所有 cudaMemcpyAsync
    for (auto& task : tasks) {
        if (!task.dst_ptr) {
            RTP_LLM_LOG_WARNING("dst_ptr is nullptr, caller must pre-allocate dst_ptr");
            return false;
        }

        cudaError_t err = cudaMemcpyAsync(task.dst_ptr, task.src_ptr, task.size, cudaMemcpyHostToDevice, stream_);
        if (err != cudaSuccess) {
            RTP_LLM_LOG_WARNING("cudaMemcpyAsync (H2D) failed: %s", cudaGetErrorString(err));
            return false;
        }
    }

    // 2. 同步等待所有拷贝完成
    cudaError_t err = cudaStreamSynchronize(stream_);
    if (err != cudaSuccess) {
        RTP_LLM_LOG_WARNING("cudaStreamSynchronize failed: %s", cudaGetErrorString(err));
        return false;
    }

    return true;
}

}  // namespace rtp_llm
