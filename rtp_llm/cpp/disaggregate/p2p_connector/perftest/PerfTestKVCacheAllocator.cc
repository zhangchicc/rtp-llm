#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/PerfTestKVCacheAllocator.h"

#include <iostream>

namespace rtp_llm {

PerfTestKVCacheAllocator::PerfTestKVCacheAllocator(const Config& config, DeviceBase* device):
    KVCacheAllocator(CacheConfig{}, device), config_(config), device_(device) {}

bool PerfTestKVCacheAllocator::init() {
    std::cout << "PerfTestKVCacheAllocator: Preallocating " << config_.num_layers << " layers x " << config_.num_blocks
              << " blocks x " << config_.block_size << " bytes"
              << " (fill_value=" << config_.fill_value << ", separate_kv=" << config_.separate_kv << ")" << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);

    for (int layer_id = 0; layer_id < config_.num_layers; ++layer_id) {
        for (int block_id = 0; block_id < config_.num_blocks; ++block_id) {
            // Allocate K buffer
            auto k_buffer = device_->allocateBuffer(
                {DataType::TYPE_UINT8, {static_cast<int64_t>(config_.block_size)}, AllocationType::DEVICE}, {});
            if (!k_buffer) {
                std::cerr << "Failed to allocate K buffer for layer " << layer_id << " block " << block_id << std::endl;
                return false;
            }
            device_->bufMemset(*k_buffer, config_.fill_value);
            all_buffers_.emplace_back(k_buffer, config_.block_size);

            BufferPtr v_buffer = nullptr;
            if (config_.separate_kv) {
                // Allocate V buffer separately
                v_buffer = device_->allocateBuffer(
                    {DataType::TYPE_UINT8, {static_cast<int64_t>(config_.block_size)}, AllocationType::DEVICE}, {});
                if (!v_buffer) {
                    std::cerr << "Failed to allocate V buffer for layer " << layer_id << " block " << block_id
                              << std::endl;
                    return false;
                }
                device_->bufMemset(*v_buffer, config_.fill_value);
                all_buffers_.emplace_back(v_buffer, config_.block_size);
            }

            buffer_map_[layer_id][block_id] = std::make_pair(k_buffer, v_buffer);
        }
    }

    device_->syncAndCheck();

    size_t total_size = all_buffers_.size() * config_.block_size;
    std::cout << "PerfTestKVCacheAllocator: Successfully preallocated " << all_buffers_.size() << " buffers, total "
              << (total_size / 1024.0 / 1024.0) << " MB" << std::endl;

    return true;
}

BlockAddrInfo PerfTestKVCacheAllocator::convertIndexToAddr(int layer_id, int block_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    BlockAddrInfo info{};
    auto          layer_iter = buffer_map_.find(layer_id);
    if (layer_iter == buffer_map_.end()) {
        return info;
    }

    auto block_iter = layer_iter->second.find(block_id);
    if (block_iter == layer_iter->second.end()) {
        return info;
    }

    const auto& [k_buffer, v_buffer] = block_iter->second;
    if (k_buffer) {
        info.k_addr = k_buffer->data();
    }
    if (v_buffer) {
        info.v_addr = v_buffer->data();
    } else if (k_buffer) {
        // If no separate V buffer, K and V share the same buffer
        info.v_addr = k_buffer->data();
    }

    return info;
}

BlockBufferPtrInfo PerfTestKVCacheAllocator::convertIndexToBuffer(int layer_id, int block_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    BlockBufferPtrInfo info{};
    auto               layer_iter = buffer_map_.find(layer_id);
    if (layer_iter == buffer_map_.end()) {
        return info;
    }

    auto block_iter = layer_iter->second.find(block_id);
    if (block_iter == layer_iter->second.end()) {
        return info;
    }

    const auto& [k_buffer, v_buffer] = block_iter->second;
    info.k_addr                      = k_buffer;
    info.v_addr                      = v_buffer ? v_buffer : k_buffer;

    return info;
}

std::vector<std::pair<BufferPtr, size_t>> PerfTestKVCacheAllocator::getAllBuffers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return all_buffers_;
}

CacheLayerLayout PerfTestKVCacheAllocator::layerCacheBase() const {
    return CacheLayerLayout{};
}

KVCacheBuffer PerfTestKVCacheAllocator::kvCacheBuffer() const {
    return KVCacheBuffer{};
}

}  // namespace rtp_llm
