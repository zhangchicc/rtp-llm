#pragma once

#include "rtp_llm/cpp/disaggregate/transfer/LayerCacheBuffer.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

namespace rtp_llm {

class LayerCacheBufferTask {
public:
    LayerCacheBufferTask(const std::map<int, std::shared_ptr<LayerCacheBuffer>>& layer_cache_buffers,
                         int64_t                                                 deadline_ms):
        layer_cache_buffers_(layer_cache_buffers), deadline_ms_(deadline_ms), start_time_us_(currentTimeUs()) {}
    ~LayerCacheBufferTask() = default;

public:
    void                              setCancelled();
    std::shared_ptr<LayerCacheBuffer> loadingLayerCacheBuffer(int layer_id);
    void                              notifyDone(int layer_id, bool success);

    bool success() const;
    bool cancelled() const;
    bool hasLoadingLayer() const;
    bool isTimeout() const;

    int64_t firstLayerWaitTimeUs() const {
        return first_layer_wait_time_us_;
    }
    int64_t totalCostTimeUs() const {
        return total_cost_time_us_;
    }
    int64_t totalBlockCount() const;

private:
    std::map<int, std::shared_ptr<LayerCacheBuffer>> layer_cache_buffers_;
    std::set<int>                                    done_layer_ids_;
    std::set<int>                                    loading_layer_ids_;
    bool                                             all_success_ = true;
    bool                                             cancelled_   = false;
    mutable std::mutex                               mutex_;
    int64_t                                          deadline_ms_;

    // metric
    int64_t start_time_us_            = 0;
    int64_t first_layer_wait_time_us_ = 0;
    int64_t total_cost_time_us_       = 0;
};

class LayerCacheBufferTaskStore {
public:
    LayerCacheBufferTaskStore();
    ~LayerCacheBufferTaskStore() = default;

    std::shared_ptr<LayerCacheBufferTask>
    addTask(const std::string&                                      unique_key,
            const std::map<int, std::shared_ptr<LayerCacheBuffer>>& layer_cache_buffers,
            int64_t                                                 deadline_ms);

    std::shared_ptr<LayerCacheBufferTask> getTask(const std::string& unique_key) const;
    std::shared_ptr<LayerCacheBufferTask> stealTask(const std::string& unique_key);

    int64_t getTaskCount() const;

private:
    mutable std::mutex mutex_;
    // [unique_key, LayerCacheBufferTask]
    std::map<std::string, std::shared_ptr<LayerCacheBufferTask>> task_map_;
};

}  // namespace rtp_llm