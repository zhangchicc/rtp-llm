#pragma once

#include "rtp_llm/cpp/disaggregate/transfer/LayerBlockConvertor.h"
#include "rtp_llm/cpp/cache/KVCacheAllocator.h"

namespace rtp_llm {

class LayerBlockConvertorImpl: public LayerBlockConvertor {
public:
    LayerBlockConvertorImpl(const std::shared_ptr<KVCacheAllocator>& allocator): allocator_(allocator) {}

    std::vector<BufferPtr>
    convertIndexToBuffer(int layer_id, int block_id, int partition_count, int partition_id) const override {
        // seperate by attention head
        auto                   kv_cache = allocator_->convertIndexToBuffer(layer_id, block_id);
        std::vector<BufferPtr> buffers;
        if (!kv_cache.v_addr || kv_cache.v_addr->size() == 0) {
            // mla, should not seperate by attention head
            buffers.push_back(kv_cache.k_addr);
            return buffers;
        }
        // MHA
        if (kv_cache.k_addr) {  // k buffer always start with head num
            if (partition_count > 1) {
                // clone buffer with offset
                int  size       = kv_cache.k_addr->shape()[0] / partition_count;
                auto new_buffer = kv_cache.k_addr->slice(partition_id * size, size);
                buffers.push_back(new_buffer);
            } else {
                buffers.push_back(kv_cache.k_addr);
            }
        }
        if (kv_cache.v_addr) {
            if (partition_count > 1) {
                int  size       = kv_cache.v_addr->shape()[0] / partition_count;
                auto new_buffer = kv_cache.v_addr->slice(partition_id * size, size);
                buffers.push_back(new_buffer);
            } else {
                buffers.push_back(kv_cache.v_addr);
            }
        }
        return buffers;
    }

private:
    std::shared_ptr<KVCacheAllocator> allocator_;
};

}  // namespace rtp_llm