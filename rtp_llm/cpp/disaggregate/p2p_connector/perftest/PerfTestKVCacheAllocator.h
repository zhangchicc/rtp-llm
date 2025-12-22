#pragma once

#include <memory>
#include <vector>
#include <mutex>
#include <unordered_map>

#include "rtp_llm/cpp/cache_new/KVCacheAllocator.h"
#include "rtp_llm/cpp/devices/DeviceBase.h"
#include "rtp_llm/cpp/core/Buffer.h"

namespace rtp_llm {

/// @brief Mock KVCacheAllocator for P2P connector performance testing
/// This class provides a simplified mock implementation of KVCacheAllocator
/// that is used by LayerBlockConvertorImpl for the p2p_connector perftest.
class PerfTestKVCacheAllocator: public KVCacheAllocator {
public:
    struct Config {
        int    num_layers  = 32;
        int    num_blocks  = 100;
        size_t block_size  = 1024 * 1024;  // 1MB per block
        int    fill_value  = 0;
        bool   separate_kv = true;  // Whether to have separate K and V buffers
    };

    PerfTestKVCacheAllocator(const Config& config, DeviceBase* device);
    ~PerfTestKVCacheAllocator() override = default;

    /// @brief Initialize the allocator and preallocate buffers
    bool init() override;

    /// @brief Get K/V buffer addresses by layer and block index
    BlockAddrInfo convertIndexToAddr(int layer_id, int block_id) const override;

    /// @brief Get K/V buffer pointers by layer and block index
    BlockBufferPtrInfo convertIndexToBuffer(int layer_id, int block_id) const override;

    /// @brief Get all allocated buffers (used for RDMA registration)
    std::vector<std::pair<BufferPtr, size_t>> getAllBuffers() const override;

    /// @brief Get cache layer layout
    CacheLayerLayout layerCacheBase() const override;

    /// @brief Get KV cache buffer
    KVCacheBuffer kvCacheBuffer() const override;

    // Not implemented in mock (return empty/default values)
    void         free(const FreeInfo& free_info) override {}
    InsertResult insertIntoCache(const InsertInfo& insert_info) override {
        return {false};
    }
    void   regUserMr(size_t model_id) override {}
    size_t freeBlocksNum() const override {
        return config_.num_blocks;
    }
    size_t availableBlocksNum() const override {
        return config_.num_blocks;
    }
    size_t availableTokensNum() const override {
        return config_.num_blocks * 128;
    }
    size_t totalBlocksNum() const override {
        return config_.num_blocks;
    }
    size_t maxSeqLen() const override {
        return config_.num_blocks * 128;
    }

    bool updateKVBlock(const BatchKVCacheResourcePtr& batch_kv_cache_resource,
                       const std::vector<int>&        block_src_batch,
                       bool                           copy_last_block,
                       std::vector<BlockIdPair>&      block_update_mapping) override {
        return false;
    }

    // Config accessors
    int numLayers() const {
        return config_.num_layers;
    }
    int numBlocks() const {
        return config_.num_blocks;
    }
    size_t blockSize() const {
        return config_.block_size;
    }

protected:
    MallocResult incrMalloc(const MallocInfo& malloc_info) override {
        return {false, 0};
    }
    MallocResult initMallocForCommonLen(const MallocInfo& malloc_info) override {
        return {false, 0};
    }

private:
    Config      config_;
    DeviceBase* device_ = nullptr;

    // Storage: layer_id -> block_id -> (k_buffer, v_buffer)
    mutable std::mutex                                                                mutex_;
    std::unordered_map<int, std::unordered_map<int, std::pair<BufferPtr, BufferPtr>>> buffer_map_;

    // Flat list of all buffers for getAllBuffers()
    std::vector<std::pair<BufferPtr, size_t>> all_buffers_;
};

}  // namespace rtp_llm
