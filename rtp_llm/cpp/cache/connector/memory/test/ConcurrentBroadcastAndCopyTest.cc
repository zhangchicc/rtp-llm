// Copyright (c) RTP-LLM
//
// This test intentionally simulates a "stuck tpSyncModelInputs broadcast" scenario:
//   1. ranks 0..7 create an 8-rank NCCL TP ProcessGroup on GPUs 0..7.
//   2. all ranks complete a tpSyncModelInputs-style warmup broadcast:
//      small pinned-CPU shape_hints + large pinned-CPU packed buffer.
//   3. in the target round, rank0 only enters the shape_hints broadcast and
//      stays silent for the following packed-buffer broadcast.
//   4. ranks 1..7 enter the packed-buffer c10dBroadcast(TP) and block.
//   5. while rank1 is blocked in that packed broadcast path, two additional
//      threads on GPU1 continuously issue large execNoBlockCopy() workloads:
//      one H2D and one D2H.
//
// Important intent / limitations:
//   - We want to exercise the real business path as closely as possible:
//     c10dBroadcast(TP) with CPU tensors plus execNoBlockCopy() for large copy
//     traffic. We intentionally do not hand-write cudaMemcpyAsync here.
//   - We do not try to prove a specific deadlock mechanism. This UT is mainly
//     a controllable reproducer for "blocked collective + heavy copies +
//     low free memory" on one GPU.
//   - We previously discussed several possible causes for "memcpy looks stuck":
//       * CPU tensor broadcast may stage through GPU memory before / during NCCL.
//       * Larger TP domain (8 ranks) may occupy more NCCL channels / SM / DMA resources.
//       * Very low free memory may change allocator behavior and indirectly affect progress.
//       * Log ordering alone is not enough to prove copies are serialized.
//     The test therefore logs all of these assumptions explicitly.
//
// The goal is not strict data validation. The value of this UT is to make the
// scenario executable and reproducible, so we only assert that:
//   - rank1 really entered the blocked packed-broadcast path; and
//   - the H2D / D2H threads still managed to execute at least one iteration.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/ProcessGroupNCCL.hpp>
#include <torch/csrc/distributed/c10d/ProcessGroup.hpp>
#include <torch/csrc/distributed/c10d/TCPStore.hpp>
#include <c10/cuda/CUDAGuard.h>
#include <cuda_runtime.h>

#include "rtp_llm/cpp/cache/BlockPool.h"
#include "rtp_llm/cpp/cache/BlockPoolConfig.h"
#include "rtp_llm/cpp/cache/MemoryLayoutConfig.h"
#include "rtp_llm/cpp/core/DistributedComm.h"
#include "rtp_llm/cpp/models/ModelTypes.h"
#include "rtp_llm/cpp/testing/TestBase.h"
#include "rtp_llm/models_py/bindings/NoBlockCopy.h"

namespace rtp_llm::test {

namespace {

constexpr int     kWorldSize                  = 8;
constexpr int     kRootRank                   = 0;
constexpr int     kRootDevice                 = 0;
constexpr int     kBlockedRank                = 1;
constexpr int     kBlockedDevice              = 1;
constexpr auto    kTpParallelMode             = rtp_llm::ParallelMode::TP;
constexpr int64_t kTpSyncShapeHintsSize       = static_cast<int64_t>(rtp_llm::GptModelInputIndex::gptModelInputLength);
constexpr int64_t kTpContextBatchSize         = 2048;
constexpr int64_t kTpDecodeBatchSize          = 0;
constexpr int64_t kTpTotalBatchSize           = kTpContextBatchSize + kTpDecodeBatchSize;
constexpr int64_t kTpCurrentTokensSize        = 262144;
constexpr int64_t kTpMaxBlocksPerBatch        = 8192;
constexpr int64_t kTpKernelBlocksPerKvBlock   = 2;
constexpr int64_t kTpMaxKernelBlocksPerBatch  = kTpMaxBlocksPerBatch * kTpKernelBlocksPerKvBlock;
constexpr int64_t kTpKvCacheGroupNum          = 1;
constexpr int64_t kTpKvCacheLayerNum          = 80;
constexpr int64_t kTpKvCacheUpdateCopyNum     = 65536;
constexpr size_t  kCopyThreadsPerDirection    = 3;
constexpr size_t  kBlockSizeBytes             = 1 * 1024 * 1024;
constexpr size_t  kCopyBatchSize              = 8192;
constexpr size_t  kPoolBlockNum               = kCopyBatchSize + 1;  // block 0 is reserved
constexpr auto    kProcessGroupTimeout        = std::chrono::minutes(5);
constexpr auto    kWarmupBroadcastTimeout     = std::chrono::milliseconds(10000);
constexpr auto    kObservationWindow          = std::chrono::minutes(1);
constexpr auto    kRank0KeepAliveSleep        = std::chrono::milliseconds(50);
constexpr auto    kLaunchPollingSleep         = std::chrono::milliseconds(10);
constexpr auto    kBroadcastLaunchGracePeriod = std::chrono::seconds(2);
constexpr auto    kWaitTimeout                = std::chrono::seconds(30);
constexpr int64_t kPackAlignment              = 16;
constexpr size_t  kMemoryPressureMinReserveBytes = 768ull * 1024 * 1024;
constexpr size_t  kMemoryPressureChunkBytes      = 1024ull * 1024 * 1024;
constexpr size_t  kMemoryPressureHeadroomDivisor = 192;

void logScenarioSummary() {
    fprintf(stdout,
            "[Scenario] intent=simulate_tpSyncModelInputs_broadcast_stall "
            "parallel_mode=TP world_size=%d blocked_rank=%d blocked_device=%d root_rank=%d\n",
            kWorldSize,
            kBlockedRank,
            kBlockedDevice,
            kRootRank);
    fprintf(stdout,
            "[Scenario] topology=single_process_multi_thread ranks=0..%d devices=0..%d "
            "broadcast_path=c10dBroadcast copy_path=execNoBlockCopy\n",
            kWorldSize - 1,
            kWorldSize - 1);
    fprintf(stdout,
            "[Scenario] target_pattern=rank0_only_runs_shape_broadcast_then_stays_silent "
            "rank1..%d_enter_packed_broadcast_and_block\n",
            kWorldSize - 1);
    fprintf(stdout,
            "[Scenario] copy_pressure=H2D_threads=%zu D2H_threads=%zu batch_size=%zu "
            "block_size_bytes=%zu total_bytes_per_iteration=%zu observation_seconds=%lld\n",
            kCopyThreadsPerDirection,
            kCopyThreadsPerDirection,
            kCopyBatchSize,
            kBlockSizeBytes,
            kCopyBatchSize * kBlockSizeBytes,
            static_cast<long long>(std::chrono::duration_cast<std::chrono::seconds>(kObservationWindow).count()));
    fflush(stdout);
}

void logPotentialPitfalls() {
    fprintf(stdout,
            "[Concern] #1 c10dBroadcast_receives_CPU_tensors_in_tpSyncModelInputs "
            "so_each_rank_may_do_CPU_to_GPU_staging_before_or_during_NCCL_launch\n");
    fprintf(stdout,
            "[Concern] #2 TP_world_size=%d_may_make_NCCL_broadcast_use_more_channels_or_GPU_resources "
            "than_small_rank_tests_and_change_copy_progress\n",
            kWorldSize);
    fprintf(stdout,
            "[Concern] #3 rank1_adds_memory_pressure_after_blocked_broadcast_launch "
            "to_distinguish_allocator_or_free_memory_effects_from_pure_collective_blocking\n");
    fprintf(stdout,
            "[Concern] #4 copy_threads_share_GPU%d_and_reuse_the_same_source_destination_tensor_sets "
            "so_log_interleaving_can_look_serial_even_when_multiple_threads_are_active\n",
            kBlockedDevice);
    fprintf(stdout,
            "[Concern] #5 this_UT_prioritizes_reproducible_execution_over_data_validation; "
            "success_means_blocked_broadcast_was_entered_and_copy_threads_made_progress\n");
    fflush(stdout);
}

int deviceForRank(int rank) {
    return rank;
}

size_t findFreePort() {
    int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(sockfd, 0);

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = 0;

    EXPECT_EQ(::bind(sockfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)), 0);

    socklen_t len = sizeof(addr);
    EXPECT_EQ(::getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&addr), &len), 0);
    ::close(sockfd);
    return ntohs(addr.sin_port);
}

BlockPoolConfig makeHostPoolConfig() {
    MemoryLayoutConfig layout_cfg;
    layout_cfg.layer_num                = 1;
    layout_cfg.block_num                = static_cast<uint32_t>(kPoolBlockNum);
    layout_cfg.dtype                    = rtp_llm::DataType::TYPE_FP16;
    layout_cfg.kv_block_stride_bytes    = kBlockSizeBytes;
    layout_cfg.kv_scale_stride_bytes    = 0;
    layout_cfg.block_stride_bytes       = kBlockSizeBytes;
    layout_cfg.kv_block_pool_size_bytes = kBlockSizeBytes * kPoolBlockNum;
    layout_cfg.kv_scale_pool_size_bytes = 0;
    layout_cfg.total_size_bytes         = layout_cfg.kv_block_pool_size_bytes;
    layout_cfg.local_head_num_kv        = 1;
    layout_cfg.seq_size_per_block       = 1;

    BlockPoolConfig pool_cfg;
    pool_cfg.block_num        = static_cast<uint32_t>(kPoolBlockNum);
    pool_cfg.total_size_bytes = layout_cfg.total_size_bytes;
    pool_cfg.memory_layouts   = {layout_cfg};
    return pool_cfg;
}

std::vector<torch::Tensor>
makeHostBlockTensors(const std::shared_ptr<BlockPool>& pool, const BlockIndicesType& block_ids, uint8_t base_pattern) {
    std::vector<torch::Tensor> tensors;
    tensors.reserve(block_ids.size());
    for (size_t i = 0; i < block_ids.size(); ++i) {
        auto infos = pool->convertIndexToBuffer(/*layer_id=*/0, block_ids[i]);
        EXPECT_FALSE(infos.empty());
        const auto& info = infos[0];
        EXPECT_NE(info.addr, nullptr);
        EXPECT_EQ(info.size_bytes, kBlockSizeBytes);

        std::memset(info.addr, static_cast<int>(base_pattern + static_cast<uint8_t>(i & 0x3F)), info.size_bytes);
        tensors.push_back(torch::from_blob(
            info.addr, {static_cast<int64_t>(info.size_bytes)}, torch::TensorOptions().dtype(torch::kUInt8)));
    }
    return tensors;
}

std::vector<torch::Tensor> makeDeviceTensors(int device, uint8_t base_pattern) {
    at::cuda::CUDAGuard        guard(device);
    std::vector<torch::Tensor> tensors;
    tensors.reserve(kCopyBatchSize);
    for (size_t i = 0; i < kCopyBatchSize; ++i) {
        auto t = torch::empty({static_cast<int64_t>(kBlockSizeBytes)},
                              torch::TensorOptions().dtype(torch::kUInt8).device(torch::Device(torch::kCUDA, device)));
        t.fill_(static_cast<int64_t>(base_pattern + static_cast<uint8_t>(i & 0x3F)));
        tensors.push_back(std::move(t));
    }
    return tensors;
}

c10::intrusive_ptr<c10d::ProcessGroup> makeNcclPG(c10::intrusive_ptr<c10d::Store> store, int rank, int world_size) {
    auto options      = c10d::ProcessGroupNCCL::Options::create();
    options->timeout  = kProcessGroupTimeout;
    auto nccl_backend = c10::make_intrusive<c10d::ProcessGroupNCCL>(store, rank, world_size, std::move(options));
    auto pg           = c10::make_intrusive<c10d::ProcessGroup>(store, rank, world_size);
    pg->setBackend(c10::DeviceType::CUDA, c10d::ProcessGroup::BackendType::NCCL, nccl_backend);
    return pg;
}

struct MemoryPressureResult {
    std::vector<torch::Tensor> tensors;
    size_t                     free_before{0};
    size_t                     free_after{0};
    size_t                     total_bytes{0};
    size_t                     allocated_bytes{0};
    size_t                     reserve_target_bytes{0};
};

size_t getMemoryPressureReserveTarget(size_t total_bytes) {
    // Keep a small reserve instead of exhausting the device completely.
    // If reserve is too small, NCCL / CUDA may fail immediately on internal
    // allocations and the test degenerates into a pure OOM reproducer.
    return std::max(kMemoryPressureMinReserveBytes, total_bytes / kMemoryPressureHeadroomDivisor);
}

MemoryPressureResult allocateRemainingDeviceMemory(int device) {
    at::cuda::CUDAGuard guard(device);

    MemoryPressureResult result;
    size_t               free_bytes  = 0;
    size_t               total_bytes = 0;
    EXPECT_EQ(cudaMemGetInfo(&free_bytes, &total_bytes), cudaSuccess);
    result.free_before          = free_bytes;
    result.total_bytes          = total_bytes;
    result.reserve_target_bytes = getMemoryPressureReserveTarget(total_bytes);

    fprintf(stdout,
            "[MemoryPressure device=%d] BEGIN free_before=%zu total=%zu reserve_target=%zu chunk=%zu\n",
            device,
            free_bytes,
            total_bytes,
            result.reserve_target_bytes,
            kMemoryPressureChunkBytes);
    fflush(stdout);

    while (free_bytes > result.reserve_target_bytes + (8 * 1024 * 1024)) {
        size_t alloc_bytes = free_bytes - result.reserve_target_bytes;
        alloc_bytes        = std::min(alloc_bytes, kMemoryPressureChunkBytes);
        alloc_bytes        = (alloc_bytes / 256) * 256;
        if (alloc_bytes == 0) {
            break;
        }

        try {
            auto t =
                torch::empty({static_cast<int64_t>(alloc_bytes)},
                             torch::TensorOptions().dtype(torch::kUInt8).device(torch::Device(torch::kCUDA, device)));
            t.fill_(0);
            result.allocated_bytes += alloc_bytes;
            result.tensors.push_back(std::move(t));
            EXPECT_EQ(cudaMemGetInfo(&free_bytes, &total_bytes), cudaSuccess);
            fprintf(stdout,
                    "[MemoryPressure device=%d] ALLOC chunk=%zu allocated_total=%zu free_now=%zu\n",
                    device,
                    alloc_bytes,
                    result.allocated_bytes,
                    free_bytes);
            fflush(stdout);
        } catch (const c10::Error&) {
            alloc_bytes /= 2;
            if (alloc_bytes < (8 * 1024 * 1024)) {
                break;
            }
            try {
                auto t = torch::empty(
                    {static_cast<int64_t>(alloc_bytes)},
                    torch::TensorOptions().dtype(torch::kUInt8).device(torch::Device(torch::kCUDA, device)));
                t.fill_(0);
                result.allocated_bytes += alloc_bytes;
                result.tensors.push_back(std::move(t));
                EXPECT_EQ(cudaMemGetInfo(&free_bytes, &total_bytes), cudaSuccess);
                fprintf(stdout,
                        "[MemoryPressure device=%d] ALLOC_RETRY chunk=%zu allocated_total=%zu free_now=%zu\n",
                        device,
                        alloc_bytes,
                        result.allocated_bytes,
                        free_bytes);
                fflush(stdout);
            } catch (const c10::Error&) {
                break;
            }
        }
    }

    EXPECT_EQ(cudaMemGetInfo(&free_bytes, &total_bytes), cudaSuccess);
    result.free_after = free_bytes;
    fprintf(stdout,
            "[MemoryPressure device=%d] END allocated_total=%zu free_after=%zu tensor_count=%zu\n",
            device,
            result.allocated_bytes,
            result.free_after,
            result.tensors.size());
    fflush(stdout);
    return result;
}

torch::Tensor makePinnedCpuIntTensor(int64_t numel, int32_t fill_value) {
    auto tensor = torch::empty({numel}, torch::TensorOptions().dtype(torch::kInt32)).pin_memory();
    tensor.fill_(fill_value);
    return tensor;
}

torch::Tensor makePinnedCpuByteTensor(int64_t numel, uint8_t fill_value) {
    auto tensor = torch::empty({numel}, torch::TensorOptions().dtype(torch::kUInt8)).pin_memory();
    tensor.fill_(static_cast<int64_t>(fill_value));
    return tensor;
}

struct TpSyncBroadcastBuffers {
    torch::Tensor shape_hints;
    torch::Tensor cpu_packed;
};

int64_t alignUp(int64_t size, int64_t alignment) {
    return (size + alignment - 1) & ~(alignment - 1);
}

void fillInt32Pattern(const torch::Tensor& tensor, int32_t base, int32_t mod) {
    auto* ptr = tensor.data_ptr<int32_t>();
    for (int64_t i = 0; i < tensor.numel(); ++i) {
        ptr[i] = base + static_cast<int32_t>(i % mod);
    }
}

void fillInt64Pattern(const torch::Tensor& tensor, int64_t base) {
    auto* ptr = tensor.data_ptr<int64_t>();
    for (int64_t i = 0; i < tensor.numel(); ++i) {
        ptr[i] = base + i;
    }
}

rtp_llm::GptModelInputs makeMockTpSyncModelInputs(int32_t seed) {
    // Build a synthetic-but-structured GptModelInputs that resembles the CPU
    // tensors packed by tpSyncModelInputs(): token metadata, kv metadata,
    // request metadata and position ids. Values are fake; shapes/byte volume are not.
    static const auto pinned_i32  = torch::TensorOptions(torch::kInt32).pinned_memory(true);
    static const auto pinned_i64  = torch::TensorOptions(torch::kInt64).pinned_memory(true);
    static const auto pinned_bool = torch::TensorOptions(torch::kBool).pinned_memory(true);

    rtp_llm::GptModelInputs inputs;
    inputs.combo_tokens          = torch::empty({kTpCurrentTokensSize}, pinned_i32);
    inputs.input_lengths         = torch::empty({kTpTotalBatchSize}, pinned_i32);
    inputs.sequence_lengths      = torch::empty({kTpDecodeBatchSize}, pinned_i32);
    inputs.lm_output_indexes     = torch::empty({kTpTotalBatchSize}, pinned_i32);
    inputs.lm_output_lengths     = torch::empty({kTpTotalBatchSize}, pinned_i32);
    inputs.prefix_lengths        = torch::empty({kTpContextBatchSize}, pinned_i32);
    inputs.combo_position_ids    = torch::empty({kTpCurrentTokensSize}, pinned_i32);
    inputs.request_id            = torch::empty({kTpContextBatchSize}, pinned_i64);
    inputs.request_pd_separation = torch::empty({kTpContextBatchSize}, pinned_bool);

    inputs.kv_cache_kernel_block_id =
        torch::empty({kTpKvCacheGroupNum, kTpTotalBatchSize, kTpMaxKernelBlocksPerBatch}, pinned_i32);
    inputs.kv_cache_block_id = torch::empty({kTpKvCacheGroupNum, kTpTotalBatchSize, kTpMaxBlocksPerBatch}, pinned_i32);
    inputs.kv_cache_layer_to_group = torch::empty({kTpKvCacheLayerNum}, pinned_i32);
    inputs.kv_cache_group_types    = torch::empty({kTpKvCacheGroupNum}, pinned_i32);
    inputs.kv_cache_update_mapping = torch::empty({kTpKvCacheUpdateCopyNum, 2}, pinned_i32);
    inputs.cache_keys              = torch::empty({kTpContextBatchSize, kTpMaxBlocksPerBatch}, pinned_i64);

    fillInt32Pattern(inputs.combo_tokens, 1000 + seed, 4096);
    auto* input_lengths_ptr     = inputs.input_lengths.data_ptr<int32_t>();
    auto* prefix_lengths_ptr    = inputs.prefix_lengths.data_ptr<int32_t>();
    auto* lm_output_indexes_ptr = inputs.lm_output_indexes.data_ptr<int32_t>();
    auto* lm_output_lengths_ptr = inputs.lm_output_lengths.data_ptr<int32_t>();
    for (int64_t i = 0; i < kTpTotalBatchSize; ++i) {
        input_lengths_ptr[i]     = 96 + static_cast<int32_t>(i % 32);
        lm_output_indexes_ptr[i] = static_cast<int32_t>(i);
        lm_output_lengths_ptr[i] = 1;
    }
    for (int64_t i = 0; i < kTpContextBatchSize; ++i) {
        prefix_lengths_ptr[i] = 64 + static_cast<int32_t>(i % 16);
    }
    fillInt32Pattern(inputs.combo_position_ids, seed * 13, 2048);
    fillInt64Pattern(inputs.request_id, 1000000ll + static_cast<int64_t>(seed) * 10000);
    inputs.request_pd_separation.fill_(true);

    fillInt32Pattern(inputs.kv_cache_kernel_block_id, seed * 7, 32768);
    fillInt32Pattern(inputs.kv_cache_block_id, seed * 11, 16384);
    auto* layer_to_group_ptr = inputs.kv_cache_layer_to_group.data_ptr<int32_t>();
    for (int64_t i = 0; i < kTpKvCacheLayerNum; ++i) {
        layer_to_group_ptr[i] = 0;
    }
    inputs.kv_cache_group_types.fill_(0);
    auto* update_mapping_ptr = inputs.kv_cache_update_mapping.data_ptr<int32_t>();
    for (int64_t i = 0; i < kTpKvCacheUpdateCopyNum; ++i) {
        update_mapping_ptr[2 * i]     = static_cast<int32_t>(i % kTpMaxBlocksPerBatch);
        update_mapping_ptr[2 * i + 1] = static_cast<int32_t>((i * 3) % kTpMaxBlocksPerBatch);
    }
    fillInt64Pattern(inputs.cache_keys, 5000000ll + static_cast<int64_t>(seed) * 100000);

    inputs.pd_separation   = true;
    inputs.need_all_logits = false;
    inputs.skip_run        = false;
    inputs.is_fake_stream  = false;
    return inputs;
}

std::vector<torch::Tensor*> collectTpSyncCpuTensorPointers(rtp_llm::GptModelInputs& inputs) {
    std::vector<torch::Tensor*> tensor_ptrs;
    auto                        collect = [&](torch::Tensor& t) {
        if (t.defined() && t.numel() > 0) {
            tensor_ptrs.push_back(&t);
        }
    };

    collect(inputs.combo_tokens);
    collect(inputs.input_lengths);
    collect(inputs.sequence_lengths);
    collect(inputs.prefix_lengths);
    collect(inputs.kv_cache_kernel_block_id);
    collect(inputs.kv_cache_block_id);
    collect(inputs.kv_cache_layer_to_group);
    collect(inputs.kv_cache_group_types);
    if (inputs.pd_separation) {
        collect(inputs.cache_keys);
    }
    collect(inputs.kv_cache_update_mapping);
    collect(inputs.request_id);
    collect(inputs.request_pd_separation);
    collect(inputs.lm_output_indexes);
    collect(inputs.lm_output_lengths);
    collect(inputs.combo_position_ids);
    // Keep the order aligned with tpSyncModelInputs() packing order so the
    // packed byte layout is closer to the real path.
    return tensor_ptrs;
}

torch::Tensor buildTpSyncShapeHintsFromInputs(const rtp_llm::GptModelInputs& inputs) {
    auto  shape_hints = makePinnedCpuIntTensor(kTpSyncShapeHintsSize, 0);
    auto* shape_ptr   = shape_hints.data_ptr<int32_t>();
    shape_ptr[rtp_llm::GptModelInputIndex::comboTokens] =
        inputs.combo_tokens.defined() ? inputs.combo_tokens.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::inputLengths] =
        inputs.input_lengths.defined() ? inputs.input_lengths.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::sequenceLengths] =
        inputs.sequence_lengths.defined() ? inputs.sequence_lengths.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::prefixLengths] =
        inputs.prefix_lengths.defined() ? inputs.prefix_lengths.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::maxKernelBlocksPerBatch] =
        inputs.kv_cache_kernel_block_id.defined() ? inputs.kv_cache_kernel_block_id.size(2) : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::maxBlocksPerBatch] =
        inputs.kv_cache_block_id.defined() ? inputs.kv_cache_block_id.size(2) : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::kvCacheGroupNum] =
        inputs.kv_cache_kernel_block_id.defined() ?
            inputs.kv_cache_kernel_block_id.size(0) :
            (inputs.kv_cache_block_id.defined() ? inputs.kv_cache_block_id.size(0) : 1);
    shape_ptr[rtp_llm::GptModelInputIndex::kvCacheLayerToGroupLen] =
        inputs.kv_cache_layer_to_group.defined() ? inputs.kv_cache_layer_to_group.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::kvCacheGroupTypesLen] =
        inputs.kv_cache_group_types.defined() ? inputs.kv_cache_group_types.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::kvCacheUpdateCopyNum] =
        inputs.kv_cache_update_mapping.defined() ? inputs.kv_cache_update_mapping.size(0) : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::lmOutputIndexes] =
        inputs.lm_output_indexes.defined() ? inputs.lm_output_indexes.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::lmOutputLengthes] =
        inputs.lm_output_lengths.defined() ? inputs.lm_output_lengths.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::comboPositionIds] =
        inputs.combo_position_ids.defined() ? inputs.combo_position_ids.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::textTokensMask]       = 0;
    shape_ptr[rtp_llm::GptModelInputIndex::mmFeaturesLocs]       = 0;
    shape_ptr[rtp_llm::GptModelInputIndex::mmFeaturesNum]        = 0;
    shape_ptr[rtp_llm::GptModelInputIndex::mmFeaturesSize]       = 0;
    shape_ptr[rtp_llm::GptModelInputIndex::mmFeaturesDtype]      = 0;
    shape_ptr[rtp_llm::GptModelInputIndex::needAllLogits]        = inputs.need_all_logits;
    shape_ptr[rtp_llm::GptModelInputIndex::mtpHiddenStates]      = 0;
    shape_ptr[rtp_llm::GptModelInputIndex::mtpHiddenStatesDtype] = 0;
    shape_ptr[rtp_llm::GptModelInputIndex::skipRun]              = inputs.skip_run;
    shape_ptr[rtp_llm::GptModelInputIndex::gptModelRequestLength] =
        inputs.request_id.defined() ? inputs.request_id.numel() : 0;
    shape_ptr[rtp_llm::GptModelInputIndex::isFakeStream] = inputs.is_fake_stream;
    return shape_hints;
}

torch::Tensor buildTpSyncCpuPackedBuffer(rtp_llm::GptModelInputs& inputs, uint8_t packed_fill_base) {
    auto    tensor_ptrs = collectTpSyncCpuTensorPointers(inputs);
    int64_t total_bytes = 0;
    for (auto* tensor : tensor_ptrs) {
        total_bytes += alignUp(static_cast<int64_t>(tensor->nbytes()), kPackAlignment);
    }

    auto    cpu_packed = makePinnedCpuByteTensor(total_bytes, packed_fill_base);
    auto*   base_ptr   = static_cast<uint8_t*>(cpu_packed.data_ptr());
    int64_t offset     = 0;
    for (auto* tensor : tensor_ptrs) {
        auto contiguous = tensor->contiguous();
        std::memcpy(base_ptr + offset, contiguous.data_ptr(), tensor->nbytes());
        // Match the production alignment rule used when packing cpu buffers.
        offset += alignUp(static_cast<int64_t>(tensor->nbytes()), kPackAlignment);
    }
    return cpu_packed;
}

TpSyncBroadcastBuffers makeTpSyncBroadcastBuffers(int32_t shape_fill_base, uint8_t packed_fill_base) {
    auto inputs = makeMockTpSyncModelInputs(shape_fill_base);

    TpSyncBroadcastBuffers out;
    out.shape_hints = buildTpSyncShapeHintsFromInputs(inputs);
    out.cpu_packed  = buildTpSyncCpuPackedBuffer(inputs, packed_fill_base);
    return out;
}

bool runTpSyncStyleWarmupBroadcast(c10::intrusive_ptr<c10d::ProcessGroup> pg,
                                   int                                    device,
                                   int32_t                                shape_fill_base,
                                   uint8_t                                packed_fill_base) {
    at::cuda::CUDAGuard guard(device);
    auto                buffers = makeTpSyncBroadcastBuffers(shape_fill_base, packed_fill_base);
    // Each rank runs in a separate std::async worker thread inside one process.
    // Thread-local ProcessGroup registration is required so TP lookup does not
    // get overwritten by other ranks in the same process.
    rtp_llm::registerThreadProcessGroup(kTpParallelMode, pg, device);

    try {
        std::vector<torch::Tensor> shape_buffers  = {buffers.shape_hints};
        std::vector<torch::Tensor> packed_buffers = {buffers.cpu_packed};
        rtp_llm::c10dBroadcast({shape_buffers, kRootRank, kTpParallelMode, false});
        rtp_llm::c10dBroadcast({packed_buffers, kRootRank, kTpParallelMode, false});
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

struct CopyThreadResult {
    bool        success{false};
    size_t      iterations{0};
    std::string error;
};

struct BroadcastThreadResult {
    bool        launched_collective{false};
    bool        aborted_cleanly{false};
    std::string detail;
};

CopyThreadResult runCopyThread(const char*                       name,
                               const char*                       direction,
                               const std::vector<torch::Tensor>& src,
                               const std::vector<torch::Tensor>& dst,
                               std::atomic<bool>&                stop_flag) {
    CopyThreadResult result;
    try {
        fprintf(stdout, "[%s] THREAD START direction=%s\n", name, direction);
        fflush(stdout);
        while (!stop_flag.load(std::memory_order_relaxed)) {
            const size_t next_iteration = result.iterations + 1;
            fprintf(stdout,
                    "[%s] COPY BEGIN direction=%s iteration=%zu block_count=%zu block_size_bytes=%zu total_bytes=%zu\n",
                    name,
                    direction,
                    next_iteration,
                    src.size(),
                    kBlockSizeBytes,
                    src.size() * kBlockSizeBytes);
            fflush(stdout);

            rtp_llm::MultiCopyParams copy_params;
            copy_params.multi_dst = dst;
            copy_params.multi_src = src;
            // Keep using the real execNoBlockCopy() path rather than hand-written
            // cudaMemcpyAsync so the observed behavior stays close to production.
            rtp_llm::execNoBlockCopy(copy_params);

            fprintf(stdout, "[%s] COPY END direction=%s iteration=%zu\n", name, direction, next_iteration);
            fflush(stdout);
            ++result.iterations;
        }
        fprintf(stdout, "[%s] THREAD END direction=%s iterations=%zu\n", name, direction, result.iterations);
        fflush(stdout);
        result.success = true;
    } catch (const std::exception& e) {
        result.error = std::string(name) + ": " + e.what();
        fprintf(stderr, "[%s] ERROR direction=%s %s\n", name, direction, result.error.c_str());
        fflush(stderr);
    }
    return result;
}

BroadcastThreadResult runFollowerBroadcastRank(size_t             port,
                                               int                rank,
                                               std::atomic<int>&  ready_count,
                                               std::atomic<bool>& blocked_rank_launched_flag,
                                               std::atomic<bool>& start_flag,
                                               std::atomic<bool>& release_flag) {
    BroadcastThreadResult result;
    try {
        const int           device = deviceForRank(rank);
        at::cuda::CUDAGuard guard(device);
        fprintf(stdout, "[Broadcast] op=broadcast phase=setup_begin rank=%d port=%zu\n", rank, port);
        fflush(stdout);

        c10d::TCPStoreOptions opts;
        opts.port       = static_cast<uint16_t>(port);
        opts.isServer   = false;
        opts.numWorkers = kWorldSize;

        auto store = c10::make_intrusive<c10d::TCPStore>("127.0.0.1", opts);
        auto pg    = makeNcclPG(store, rank, kWorldSize);
        fprintf(stdout, "[Broadcast] op=broadcast phase=warmup_begin rank=%d\n", rank);
        fflush(stdout);
        bool warmup_ok = runTpSyncStyleWarmupBroadcast(
            pg, device, /*shape_fill_base=*/rank, /*packed_fill_base=*/static_cast<uint8_t>(0x10 + rank));
        if (!warmup_ok) {
            result.detail = "warmup broadcast did not complete";
            fprintf(stderr, "[Broadcast] op=broadcast phase=warmup_end rank=%d success=0\n", rank);
            fflush(stderr);
            return result;
        }
        fprintf(stdout, "[Broadcast] op=broadcast phase=warmup_end rank=%d success=1\n", rank);
        fflush(stdout);
        ready_count.fetch_add(1, std::memory_order_acq_rel);

        while (!start_flag.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(kLaunchPollingSleep);
        }

        // This is the target round. Followers prepare tpSyncModelInputs-style
        // CPU buffers and then enter the real c10dBroadcast(TP) path with those tensors.
        auto                       tp_sync_buffers = makeTpSyncBroadcastBuffers(/*shape_fill_base=*/100 + rank,
                                                          /*packed_fill_base=*/static_cast<uint8_t>(0x20 + rank));
        std::vector<torch::Tensor> shape_buffers   = {tp_sync_buffers.shape_hints};
        std::vector<torch::Tensor> packed_buffers  = {tp_sync_buffers.cpu_packed};
        rtp_llm::registerThreadProcessGroup(kTpParallelMode, pg, device);
        fprintf(stdout,
                "[Broadcast] op=broadcast phase=target_round_ready rank=%d device=%d "
                "shape_numel=%ld packed_bytes=%zu note=cpu_tensor_broadcast_matches_tpSyncModelInputs\n",
                rank,
                device,
                tp_sync_buffers.shape_hints.numel(),
                tp_sync_buffers.cpu_packed.nbytes());
        fflush(stdout);

        fprintf(stdout,
                "[Broadcast] op=broadcast phase=shape_begin rank=%d root=%d numel=%ld\n",
                rank,
                kRootRank,
                tp_sync_buffers.shape_hints.numel());
        fflush(stdout);
        rtp_llm::c10dBroadcast({shape_buffers, kRootRank, kTpParallelMode, false});
        fprintf(stdout,
                "[Broadcast] op=broadcast phase=shape_end rank=%d root=%d numel=%ld\n",
                rank,
                kRootRank,
                tp_sync_buffers.shape_hints.numel());
        fflush(stdout);

        fprintf(stdout,
                "[Broadcast] op=broadcast phase=packed_begin rank=%d root=%d numel=%ld bytes=%zu\n",
                rank,
                kRootRank,
                tp_sync_buffers.cpu_packed.numel(),
                tp_sync_buffers.cpu_packed.nbytes());
        fflush(stdout);
        result.launched_collective = true;
        if (rank == kBlockedRank) {
            // The main thread synchronizes on rank1 because rank1 is also where
            // memory pressure and copy pressure are applied.
            blocked_rank_launched_flag.store(true, std::memory_order_release);
        }
        std::thread abort_thread([&]() {
            while (!release_flag.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(kLaunchPollingSleep);
            }
            // Rank0 never joins the packed broadcast in the target round, so we
            // abort after the observation window to keep the test terminable.
            fprintf(stdout, "[Broadcast] op=broadcast phase=abort_begin rank=%d\n", rank);
            fflush(stdout);
            pg->abort();
            fprintf(stdout, "[Broadcast] op=broadcast phase=abort_end rank=%d\n", rank);
            fflush(stdout);
        });
        try {
            rtp_llm::c10dBroadcast({packed_buffers, kRootRank, kTpParallelMode, false});
            result.aborted_cleanly = release_flag.load(std::memory_order_relaxed);
            result.detail          = result.aborted_cleanly ? "packed broadcast returned after pg->abort()" :
                                                              "packed broadcast returned without abort";
            fprintf(stdout,
                    "[Broadcast] op=broadcast phase=packed_end rank=%d aborted=%d message=%s\n",
                    rank,
                    result.aborted_cleanly ? 1 : 0,
                    result.detail.c_str());
            fflush(stdout);
        } catch (const std::exception& e) {
            result.aborted_cleanly = release_flag.load(std::memory_order_relaxed);
            result.detail          = e.what();
            fprintf(stdout,
                    "[Broadcast] op=broadcast phase=packed_end rank=%d aborted=%d message=%s\n",
                    rank,
                    result.aborted_cleanly ? 1 : 0,
                    result.detail.c_str());
            fflush(stdout);
        }
        if (abort_thread.joinable()) {
            abort_thread.join();
        }
    } catch (const std::exception& e) {
        result.detail = e.what();
        fprintf(stderr, "[Broadcast] op=broadcast phase=error rank=%d message=%s\n", rank, result.detail.c_str());
        fflush(stderr);
    }
    return result;
}

bool runSilentRank0(size_t             port,
                    std::atomic<bool>& start_flag,
                    std::atomic<bool>& release_flag,
                    std::atomic<bool>& rank0_ready_flag) {
    at::cuda::CUDAGuard guard(deviceForRank(kRootRank));
    fprintf(stdout, "[Broadcast] op=broadcast phase=silent_setup_begin rank=%d port=%zu\n", kRootRank, port);
    fflush(stdout);

    c10d::TCPStoreOptions opts;
    opts.port       = static_cast<uint16_t>(port);
    opts.isServer   = true;
    opts.numWorkers = kWorldSize;

    auto store = c10::make_intrusive<c10d::TCPStore>("127.0.0.1", opts);
    auto pg    = makeNcclPG(store, kRootRank, kWorldSize);
    fprintf(stdout, "[Broadcast] op=broadcast phase=silent_warmup_begin rank=%d\n", kRootRank);
    fflush(stdout);
    bool warmup_ok =
        runTpSyncStyleWarmupBroadcast(pg, deviceForRank(kRootRank), /*shape_fill_base=*/7, /*packed_fill_base=*/0x77);
    if (!warmup_ok) {
        fprintf(stderr, "[Broadcast] op=broadcast phase=silent_warmup_end rank=%d success=0\n", kRootRank);
        fflush(stderr);
        return false;
    }
    fprintf(stdout, "[Broadcast] op=broadcast phase=silent_warmup_end rank=%d success=1\n", kRootRank);
    fflush(stdout);

    rank0_ready_flag.store(true, std::memory_order_release);
    while (!start_flag.load(std::memory_order_relaxed)) {
        if (release_flag.load(std::memory_order_relaxed)) {
            break;
        }
        std::this_thread::sleep_for(kLaunchPollingSleep);
    }

    if (!release_flag.load(std::memory_order_relaxed)) {
        // Rank0 intentionally participates only in the shape-hints broadcast of
        // the target round. This models "root does not enter packed broadcast"
        // and leaves the followers blocked in the packed c10dBroadcast path.
        auto tp_sync_buffers = makeTpSyncBroadcastBuffers(/*shape_fill_base=*/9, /*packed_fill_base=*/0x99);
        std::vector<torch::Tensor> shape_buffers = {tp_sync_buffers.shape_hints};
        rtp_llm::registerThreadProcessGroup(kTpParallelMode, pg, deviceForRank(kRootRank));
        fprintf(stdout,
                "[Broadcast] op=broadcast phase=silent_shape_begin rank=%d root=%d numel=%ld\n",
                kRootRank,
                kRootRank,
                tp_sync_buffers.shape_hints.numel());
        fflush(stdout);
        rtp_llm::c10dBroadcast({shape_buffers, kRootRank, kTpParallelMode, false});
        fprintf(stdout,
                "[Broadcast] op=broadcast phase=silent_shape_end rank=%d root=%d numel=%ld\n",
                kRootRank,
                kRootRank,
                tp_sync_buffers.shape_hints.numel());
        fflush(stdout);
    }

    while (!release_flag.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(kRank0KeepAliveSleep);
    }
    fprintf(stdout, "[Broadcast] op=broadcast phase=silent_abort_begin rank=%d\n", kRootRank);
    fflush(stdout);
    pg->abort();
    fprintf(stdout, "[Broadcast] op=broadcast phase=silent_abort_end rank=%d\n", kRootRank);
    fflush(stdout);
    return true;
}

class ConcurrentBroadcastAndCopyTest: public ::testing::Test {
protected:
    void SetUp() override {
        rtp_llm::initLogger();
        num_gpus_ = static_cast<int>(torch::cuda::device_count());
    }

    int num_gpus_ = 0;
};

}  // namespace

TEST_F(ConcurrentBroadcastAndCopyTest, testConcurrentBroadcastAndCopy) {
    rtp_llm::clearProcessGroups();
    rtp_llm::clearThreadProcessGroups();
    if (num_gpus_ < kWorldSize) {
        GTEST_SKIP() << "Need at least 8 CUDA devices to simulate an 8-rank TP broadcast domain";
    }

    logScenarioSummary();
    logPotentialPitfalls();

    for (int rank = 0; rank < kWorldSize; ++rank) {
        torch::zeros({1}, torch::TensorOptions().device(torch::Device(torch::kCUDA, deviceForRank(rank))));
    }

    const size_t port = findFreePort();
    fprintf(
        stdout, "[Scenario] rendezvous=tcpstore address=127.0.0.1 port=%zu note=all_8_ranks_share_one_process\n", port);
    fflush(stdout);

    auto host_pool_cfg = makeHostPoolConfig();
    auto h2d_src_pool  = std::make_shared<BlockPool>(host_pool_cfg, AllocationType::HOST);
    auto d2h_dst_pool  = std::make_shared<BlockPool>(host_pool_cfg, AllocationType::HOST);
    ASSERT_TRUE(h2d_src_pool->init());
    ASSERT_TRUE(d2h_dst_pool->init());

    auto h2d_block_ids = h2d_src_pool->malloc(static_cast<int>(kCopyBatchSize));
    auto d2h_block_ids = d2h_dst_pool->malloc(static_cast<int>(kCopyBatchSize));
    ASSERT_EQ(h2d_block_ids.size(), kCopyBatchSize);
    ASSERT_EQ(d2h_block_ids.size(), kCopyBatchSize);

    auto h2d_cpu_srcs = makeHostBlockTensors(h2d_src_pool, h2d_block_ids, /*base_pattern=*/0x11);
    auto d2h_cpu_dsts = makeHostBlockTensors(d2h_dst_pool, d2h_block_ids, /*base_pattern=*/0x00);

    auto h2d_gpu_dsts = makeDeviceTensors(kBlockedDevice, /*base_pattern=*/0x00);
    auto d2h_gpu_srcs = makeDeviceTensors(kBlockedDevice, /*base_pattern=*/0x80);
    fprintf(stdout,
            "[Scenario] copy_buffers_ready blocked_device=%d h2d_blocks=%zu d2h_blocks=%zu "
            "bytes_per_direction=%zu\n",
            kBlockedDevice,
            h2d_cpu_srcs.size(),
            d2h_gpu_srcs.size(),
            kCopyBatchSize * kBlockSizeBytes);
    fflush(stdout);

    std::atomic<bool> rank0_ready{false};
    std::atomic<int>  follower_ready_count{0};
    std::atomic<bool> blocked_rank_launched{false};
    std::atomic<bool> start_blocked_broadcast{false};
    std::atomic<bool> release_rank0{false};
    std::atomic<bool> release_followers{false};
    std::atomic<bool> stop_copies{false};

    auto rank0_future = std::async(std::launch::async, [&]() {
        return runSilentRank0(port, start_blocked_broadcast, release_rank0, rank0_ready);
    });

    std::vector<std::future<BroadcastThreadResult>> follower_futures;
    follower_futures.reserve(kWorldSize - 1);
    for (int rank = 1; rank < kWorldSize; ++rank) {
        follower_futures.push_back(std::async(std::launch::async, [&, rank]() {
            return runFollowerBroadcastRank(
                port, rank, follower_ready_count, blocked_rank_launched, start_blocked_broadcast, release_followers);
        }));
    }

    auto wait_until = [](auto&& predicate) {
        const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
        while (!predicate()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(kLaunchPollingSleep);
        }
        return true;
    };

    ASSERT_TRUE(wait_until([&]() { return rank0_ready.load(std::memory_order_acquire); }))
        << "rank0 failed to create the silent ProcessGroup in time";
    ASSERT_TRUE(wait_until([&]() { return follower_ready_count.load(std::memory_order_acquire) == (kWorldSize - 1); }))
        << "follower ranks failed to create the TP ProcessGroup in time";
    start_blocked_broadcast.store(true, std::memory_order_relaxed);
    fprintf(
        stdout,
        "[Scenario] trigger=target_round_start note=follower_ranks_will_enter_packed_broadcast_while_rank0_stays_silent\n");
    fflush(stdout);
    ASSERT_TRUE(wait_until([&]() { return blocked_rank_launched.load(std::memory_order_acquire); }))
        << "rank1 did not launch the blocked packed broadcast in time";
    fprintf(stdout,
            "[Scenario] blocked_rank_confirmed rank=%d note=rank1_has_entered_the_packed_broadcast_path\n",
            kBlockedRank);
    fflush(stdout);

    std::this_thread::sleep_for(kBroadcastLaunchGracePeriod);
    // Give the blocked broadcast a short head start before squeezing free
    // memory. Without this gap, the test can fail earlier during staging or
    // allocator setup and no longer represent "already entered blocked broadcast".
    auto memory_pressure = allocateRemainingDeviceMemory(kBlockedDevice);
    fprintf(
        stdout,
        "[Scenario] memory_pressure_ready device=%d allocated_bytes=%zu free_before=%zu free_after=%zu reserve_target=%zu\n",
        kBlockedDevice,
        memory_pressure.allocated_bytes,
        memory_pressure.free_before,
        memory_pressure.free_after,
        memory_pressure.reserve_target_bytes);
    fflush(stdout);
    std::vector<std::future<CopyThreadResult>> h2d_futures;
    std::vector<std::future<CopyThreadResult>> d2h_futures;
    h2d_futures.reserve(kCopyThreadsPerDirection);
    d2h_futures.reserve(kCopyThreadsPerDirection);
    fprintf(stdout,
            "[Scenario] copy_threads_launch begin total_threads=%zu note=six_threads_share_gpu%d\n",
            kCopyThreadsPerDirection * 2,
            kBlockedDevice);
    fflush(stdout);
    for (size_t i = 0; i < kCopyThreadsPerDirection; ++i) {
        h2d_futures.push_back(std::async(std::launch::async, [&, i]() {
            const std::string name = "H2D-" + std::to_string(i);
            return runCopyThread(name.c_str(), "H2D", h2d_cpu_srcs, h2d_gpu_dsts, stop_copies);
        }));
        d2h_futures.push_back(std::async(std::launch::async, [&, i]() {
            const std::string name = "D2H-" + std::to_string(i);
            return runCopyThread(name.c_str(), "D2H", d2h_gpu_srcs, d2h_cpu_dsts, stop_copies);
        }));
    }
    // All copy workers intentionally share the same GPU and tensor sets. The goal
    // here is overlap / contention against the blocked broadcast, not per-thread isolation.
    fprintf(stdout,
            "[Scenario] copy_threads_launch end total_threads=%zu observation_window_seconds=%lld\n",
            kCopyThreadsPerDirection * 2,
            static_cast<long long>(std::chrono::duration_cast<std::chrono::seconds>(kObservationWindow).count()));
    fflush(stdout);

    std::this_thread::sleep_for(kObservationWindow);
    release_followers.store(true, std::memory_order_relaxed);
    stop_copies.store(true, std::memory_order_relaxed);
    fprintf(stdout, "[Scenario] observation_complete action=release_followers_and_stop_copies\n");
    fflush(stdout);
    std::vector<BroadcastThreadResult> follower_results;
    follower_results.reserve(kWorldSize - 1);
    BroadcastThreadResult blocked_rank_result;
    for (int rank = 1; rank < kWorldSize; ++rank) {
        auto result = follower_futures[rank - 1].get();
        if (rank == kBlockedRank) {
            blocked_rank_result = result;
        }
        follower_results.push_back(std::move(result));
    }

    std::vector<CopyThreadResult> h2d_results;
    std::vector<CopyThreadResult> d2h_results;
    h2d_results.reserve(kCopyThreadsPerDirection);
    d2h_results.reserve(kCopyThreadsPerDirection);
    for (auto& future : h2d_futures) {
        h2d_results.push_back(future.get());
    }
    for (auto& future : d2h_futures) {
        d2h_results.push_back(future.get());
    }

    release_rank0.store(true, std::memory_order_relaxed);
    EXPECT_TRUE(rank0_future.get());
    fprintf(stdout,
            "[Scenario] result_summary blocked_rank_launched=%d blocked_rank_aborted=%d "
            "memory_pressure_allocated=%zu\n",
            blocked_rank_result.launched_collective ? 1 : 0,
            blocked_rank_result.aborted_cleanly ? 1 : 0,
            memory_pressure.allocated_bytes);
    for (size_t i = 0; i < h2d_results.size(); ++i) {
        fprintf(stdout,
                "[Scenario] result_copy direction=H2D thread=%zu success=%d iterations=%zu\n",
                i,
                h2d_results[i].success ? 1 : 0,
                h2d_results[i].iterations);
    }
    for (size_t i = 0; i < d2h_results.size(); ++i) {
        fprintf(stdout,
                "[Scenario] result_copy direction=D2H thread=%zu success=%d iterations=%zu\n",
                i,
                d2h_results[i].success ? 1 : 0,
                d2h_results[i].iterations);
    }
    fflush(stdout);

    EXPECT_TRUE(blocked_rank_result.launched_collective) << "rank1 never launched the blocked broadcast";
    EXPECT_TRUE(blocked_rank_result.aborted_cleanly)
        << "rank1 blocked broadcast did not abort cleanly: " << blocked_rank_result.detail;
    for (size_t i = 0; i < follower_results.size(); ++i) {
        EXPECT_TRUE(follower_results[i].launched_collective)
            << "follower rank " << (i + 1) << " never launched the blocked broadcast";
        EXPECT_TRUE(follower_results[i].aborted_cleanly)
            << "follower rank " << (i + 1)
            << " blocked broadcast did not abort cleanly: " << follower_results[i].detail;
    }

    for (size_t i = 0; i < h2d_results.size(); ++i) {
        EXPECT_TRUE(h2d_results[i].success) << h2d_results[i].error;
        EXPECT_GT(h2d_results[i].iterations, 0u)
            << "H2D thread " << i << " did not execute any execNoBlockCopy iteration";
    }
    for (size_t i = 0; i < d2h_results.size(); ++i) {
        EXPECT_TRUE(d2h_results[i].success) << d2h_results[i].error;
        EXPECT_GT(d2h_results[i].iterations, 0u)
            << "D2H thread " << i << " did not execute any execNoBlockCopy iteration";
    }
    EXPECT_GT(memory_pressure.allocated_bytes, 0u) << "Failed to allocate device memory pressure tensor(s)";

    for (auto id : h2d_block_ids) {
        h2d_src_pool->requestFree(id);
    }
    for (auto id : d2h_block_ids) {
        d2h_dst_pool->requestFree(id);
    }
    rtp_llm::clearThreadProcessGroups();
    rtp_llm::clearProcessGroups();
}

}  // namespace rtp_llm::test
