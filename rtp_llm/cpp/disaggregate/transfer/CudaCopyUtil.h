#pragma once

#if USING_CUDA
#include <cuda_runtime.h>
#elif USING_ROCM
#include <hip/hip_runtime.h>
#endif

#include <vector>
#include <cstddef>

namespace rtp_llm {

/// @brief 待拷贝的 buffer 信息
struct CopyTask {
    void*  src_ptr;  // 源地址
    size_t size;     // 拷贝大小
    char*  dst_ptr;  // 目标地址，必须由调用者预先分配
};

/// @brief GPU 拷贝工具类，封装 memcpyAsync + streamSynchronize 的批量拷贝逻辑
/// 支持 CUDA 和 ROCm 平台
class CudaCopyUtil {
public:
    CudaCopyUtil();
    ~CudaCopyUtil();

    // 禁止拷贝
    CudaCopyUtil(const CudaCopyUtil&)            = delete;
    CudaCopyUtil& operator=(const CudaCopyUtil&) = delete;

    /// @brief 批量执行 GPU -> CPU 拷贝
    /// @param tasks 待拷贝任务列表（dst_ptr 必须由调用者预先分配）
    /// @return 成功返回 true，若 dst_ptr 为 nullptr 则返回 false
    bool batchCopyToHost(std::vector<CopyTask>& tasks);

    /// @brief 批量执行 CPU -> GPU 拷贝
    /// @param tasks 待拷贝任务列表（dst_ptr 必须由调用者预先分配）
    /// @return 成功返回 true，若 dst_ptr 为 nullptr 则返回 false
    bool batchCopyToDevice(std::vector<CopyTask>& tasks);

#if USING_CUDA
    cudaStream_t stream_ = nullptr;
#elif USING_ROCM
    hipStream_t stream_ = nullptr;
#endif
};

}  // namespace rtp_llm
