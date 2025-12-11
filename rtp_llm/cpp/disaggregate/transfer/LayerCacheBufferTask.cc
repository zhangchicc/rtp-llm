#include "rtp_llm/cpp/disaggregate/transfer/LayerCacheBufferTask.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

namespace rtp_llm {

bool LayerCacheBufferTask::success() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return all_success_ && done_layer_ids_.size() == layer_cache_buffers_.size();
}

bool LayerCacheBufferTask::cancelled() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cancelled_;
}

bool LayerCacheBufferTask::hasLoadingLayer() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !loading_layer_ids_.empty();
}

bool LayerCacheBufferTask::isTimeout() const {
    return currentTimeMs() >= deadline_ms_;
}

void LayerCacheBufferTask::setCancelled() {
    std::lock_guard<std::mutex> lock(mutex_);
    cancelled_ = true;
}

void LayerCacheBufferTask::notifyDone(int layer_id, bool success) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (auto iter = layer_cache_buffers_.find(layer_id); iter == layer_cache_buffers_.end()) {
        RTP_LLM_LOG_WARNING("LayerCacheBufferTask notifyDone failed: layer_id not found, layer_id: %d", layer_id);
        return;
    }
    if (!success) {
        all_success_ = false;
    }
    done_layer_ids_.insert(layer_id);
    loading_layer_ids_.erase(layer_id);
    if (done_layer_ids_.size() == layer_cache_buffers_.size()) {
        total_cost_time_us_ = currentTimeUs() - start_time_us_;
    }
}

std::shared_ptr<LayerCacheBuffer> LayerCacheBufferTask::loadingLayerCacheBuffer(int layer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto                        iter = layer_cache_buffers_.find(layer_id);
    if (iter == layer_cache_buffers_.end()) {
        RTP_LLM_LOG_WARNING("LayerCacheBufferTask loadingLayerCacheBuffer failed: layer_id not found, layer_id: %d",
                            layer_id);
        return nullptr;
    }
    if (loading_layer_ids_.find(layer_id) != loading_layer_ids_.end()
        || done_layer_ids_.find(layer_id) != done_layer_ids_.end()) {
        RTP_LLM_LOG_WARNING(
            "LayerCacheBufferTask loadingLayerCacheBuffer failed: layer_id already loading or done, layer_id: %d",
            layer_id);
        return nullptr;
    }
    loading_layer_ids_.insert(layer_id);

    if (first_layer_wait_time_us_ == 0) {
        first_layer_wait_time_us_ = currentTimeUs() - start_time_us_;
    }
    return iter->second;
}

int64_t LayerCacheBufferTask::totalBlockCount() const {
    int64_t                     total_block_count = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [layer_id, layer_cache_buffer] : layer_cache_buffers_) {
        total_block_count += layer_cache_buffer->blockIdMap().size();
    }
    return total_block_count;
}

// ==================== LayerCacheBufferTaskStore ====================

LayerCacheBufferTaskStore::LayerCacheBufferTaskStore() {}

std::shared_ptr<LayerCacheBufferTask>
LayerCacheBufferTaskStore::addTask(const std::string&                                      unique_key,
                                   const std::map<int, std::shared_ptr<LayerCacheBuffer>>& layer_cache_buffers,
                                   int64_t                                                 deadline_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    task_map_[unique_key] = std::make_shared<LayerCacheBufferTask>(layer_cache_buffers, deadline_ms);
    return task_map_[unique_key];
}

std::shared_ptr<LayerCacheBufferTask> LayerCacheBufferTaskStore::getTask(const std::string& unique_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto                        it = task_map_.find(unique_key);
    if (it == task_map_.end()) {
        return nullptr;
    }
    return it->second;
}

std::shared_ptr<LayerCacheBufferTask> LayerCacheBufferTaskStore::stealTask(const std::string& unique_key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto                        it = task_map_.find(unique_key);
    if (it == task_map_.end()) {
        return nullptr;
    }
    auto task = it->second;
    task_map_.erase(it->first);
    return task;
}

int64_t LayerCacheBufferTaskStore::getTaskCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return task_map_.size();
}

}  // namespace rtp_llm