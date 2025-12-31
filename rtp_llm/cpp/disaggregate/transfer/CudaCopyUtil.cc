#include "rtp_llm/cpp/disaggregate/transfer/CudaCopyUtil.h"
#include "rtp_llm/cpp/utils/Logger.h"

namespace rtp_llm {

CudaCopyUtil::CudaCopyUtil() {
#if defined(USING_CUDA) && USING_CUDA
    cudaError_t err = cudaStreamCreate(&stream_);
    if (err != cudaSuccess) {
        RTP_LLM_LOG_WARNING("cudaStreamCreate failed: %s", cudaGetErrorString(err));
        stream_ = nullptr;
    }
#elif defined(USING_ROCM) && USING_ROCM
    hipError_t err = hipStreamCreate(&stream_);
    if (err != hipSuccess) {
        RTP_LLM_LOG_WARNING("hipStreamCreate failed: %s", hipGetErrorString(err));
        stream_ = nullptr;
    }
#else
    // 非 GPU 平台，stream_ 保持 nullptr
#endif
}

CudaCopyUtil::~CudaCopyUtil() {
#if defined(USING_CUDA) && USING_CUDA
    if (stream_) {
        cudaStreamDestroy(stream_);
        stream_ = nullptr;
    }
#elif defined(USING_ROCM) && USING_ROCM
    if (stream_) {
        hipStreamDestroy(stream_);
        stream_ = nullptr;
    }
#else
    // 非 GPU 平台，无需清理
#endif
}

bool CudaCopyUtil::batchCopyToHost(std::vector<CopyTask>& tasks) {
    if (tasks.empty()) {
        return true;
    }

#if (defined(USING_CUDA) && USING_CUDA) || (defined(USING_ROCM) && USING_ROCM)
    if (!stream_) {
        RTP_LLM_LOG_WARNING("GPU stream is not initialized");
        return false;
    }

    // 1. 提交所有 MemcpyAsync
    for (auto& task : tasks) {
        if (!task.dst_ptr) {
            RTP_LLM_LOG_WARNING("dst_ptr is nullptr, caller must pre-allocate dst_ptr");
            return false;
        }

#if defined(USING_CUDA) && USING_CUDA
        cudaError_t err = cudaMemcpyAsync(task.dst_ptr, task.src_ptr, task.size, cudaMemcpyDeviceToHost, stream_);
        if (err != cudaSuccess) {
            RTP_LLM_LOG_WARNING("cudaMemcpyAsync (D2H) failed: %s", cudaGetErrorString(err));
            return false;
        }
#elif defined(USING_ROCM) && USING_ROCM
        hipError_t err = hipMemcpyAsync(task.dst_ptr, task.src_ptr, task.size, hipMemcpyDeviceToHost, stream_);
        if (err != hipSuccess) {
            RTP_LLM_LOG_WARNING("hipMemcpyAsync (D2H) failed: %s", hipGetErrorString(err));
            return false;
        }
#endif
    }

    // 2. 同步等待所有拷贝完成
#if defined(USING_CUDA) && USING_CUDA
    cudaError_t err = cudaStreamSynchronize(stream_);
    if (err != cudaSuccess) {
        RTP_LLM_LOG_WARNING("cudaStreamSynchronize failed: %s", cudaGetErrorString(err));
        return false;
    }
#elif defined(USING_ROCM) && USING_ROCM
    hipError_t err = hipStreamSynchronize(stream_);
    if (err != hipSuccess) {
        RTP_LLM_LOG_WARNING("hipStreamSynchronize failed: %s", hipGetErrorString(err));
        return false;
    }
#endif

    return true;
#else
    // 非 GPU 平台不支持此操作
    RTP_LLM_LOG_WARNING("CudaCopyUtil::batchCopyToHost is not supported on non-GPU platforms");
    return false;
#endif
}

bool CudaCopyUtil::batchCopyToDevice(std::vector<CopyTask>& tasks) {
    if (tasks.empty()) {
        return true;
    }

#if (defined(USING_CUDA) && USING_CUDA) || (defined(USING_ROCM) && USING_ROCM)
    if (!stream_) {
        RTP_LLM_LOG_WARNING("GPU stream is not initialized");
        return false;
    }

    // 1. 提交所有 MemcpyAsync
    for (auto& task : tasks) {
        if (!task.dst_ptr) {
            RTP_LLM_LOG_WARNING("dst_ptr is nullptr, caller must pre-allocate dst_ptr");
            return false;
        }

#if defined(USING_CUDA) && USING_CUDA
        cudaError_t err = cudaMemcpyAsync(task.dst_ptr, task.src_ptr, task.size, cudaMemcpyHostToDevice, stream_);
        if (err != cudaSuccess) {
            RTP_LLM_LOG_WARNING("cudaMemcpyAsync (H2D) failed: %s", cudaGetErrorString(err));
            return false;
        }
#elif defined(USING_ROCM) && USING_ROCM
        hipError_t err = hipMemcpyAsync(task.dst_ptr, task.src_ptr, task.size, hipMemcpyHostToDevice, stream_);
        if (err != hipSuccess) {
            RTP_LLM_LOG_WARNING("hipMemcpyAsync (H2D) failed: %s", hipGetErrorString(err));
            return false;
        }
#endif
    }

    // 2. 同步等待所有拷贝完成
#if defined(USING_CUDA) && USING_CUDA
    cudaError_t err = cudaStreamSynchronize(stream_);
    if (err != cudaSuccess) {
        RTP_LLM_LOG_WARNING("cudaStreamSynchronize failed: %s", cudaGetErrorString(err));
        return false;
    }
#elif defined(USING_ROCM) && USING_ROCM
    hipError_t err = hipStreamSynchronize(stream_);
    if (err != hipSuccess) {
        RTP_LLM_LOG_WARNING("hipStreamSynchronize failed: %s", hipGetErrorString(err));
        return false;
    }
#endif

    return true;
#else
    // 非 GPU 平台不支持此操作
    RTP_LLM_LOG_WARNING("CudaCopyUtil::batchCopyToDevice is not supported on non-GPU platforms");
    return false;
#endif
}

}  // namespace rtp_llm
