#pragma once

#include "kmonitor/client/MetricsReporter.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

namespace rtp_llm {

class P2PConnectorMetrics;

class P2PConnectorDecodeSchedulerMetricsCollector final {
public:
    P2PConnectorDecodeSchedulerMetricsCollector(const std::shared_ptr<kmonitor::MetricsReporter>& metrics_reporter):
        start_time_us(currentTimeUs()), metrics_reporter_(metrics_reporter) {}
    ~P2PConnectorDecodeSchedulerMetricsCollector() {
        if (metrics_reporter_) {
            metrics_reporter_->report<P2PConnectorMetrics, P2PConnectorDecodeSchedulerMetricsCollector>(nullptr, this);
        }
    }

public:
    bool    success                   = true;
    int64_t start_time_us             = 0;
    int64_t prefill_load_cost_time_us = 0;
    int64_t tp_sync_cost_time_us      = 0;
    int64_t total_cost_time_us        = 0;

private:
    std::shared_ptr<kmonitor::MetricsReporter> metrics_reporter_;
};

class P2PConnectorDecodeWorkerMetricsCollector final {
public:
    P2PConnectorDecodeWorkerMetricsCollector()  = default;
    ~P2PConnectorDecodeWorkerMetricsCollector() = default;

public:
    bool    success                  = true;
    int64_t total_block_count        = 0;
    int64_t first_layer_wait_time_us = 0;
    int64_t total_cost_time_us       = 0;
};

class P2PConnectorDecodeSchedulerStatusMetricsCollector final {
public:
    int64_t check_once_cost_time_us = 0;
    int64_t inflight_context_count  = 0;
};

class P2PConnectorPrefillSchedulerMetricsCollector final {
public:
    P2PConnectorPrefillSchedulerMetricsCollector()  = default;
    ~P2PConnectorPrefillSchedulerMetricsCollector() = default;

public:
    bool    success            = true;
    int64_t total_cost_time_us = 0;
};

class P2PConnectorPrefillWorkerStoreMetricsCollector final {
public:
    P2PConnectorPrefillWorkerStoreMetricsCollector(): start_time_us(currentTimeUs()) {}
    ~P2PConnectorPrefillWorkerStoreMetricsCollector() = default;

public:
    bool    success                 = true;
    int64_t total_block_count       = 0;
    int64_t store_wait_done_time_us = 0;
    int64_t start_time_us           = 0;
};

class P2PConnectorPrefillWorkerStatusMetricsCollector final {
public:
    int64_t wait_store_event_count = 0;
    int64_t task_count             = 0;
    int64_t computed_request_count = 0;
};

class P2PConnectorPrefillWorkerWriteMetricsCollector final {
public:
    P2PConnectorPrefillWorkerWriteMetricsCollector()  = default;
    ~P2PConnectorPrefillWorkerWriteMetricsCollector() = default;

public:
    bool    success                  = true;
    int64_t first_layer_wait_time_us = 0;
    int64_t last_layer_wait_time_us  = 0;
    int64_t total_cost_time_us       = 0;
};

class P2PConnectorStreamStoreMetricsCollector1 final {
public:
    int64_t stream_count = 0;
};

class P2PConnectorStreamStoreMetricsCollector2 final {
public:
    bool    timeout             = false;
    int64_t stream_wait_time_us = 0;
};

class P2PConnectorMetrics: public kmonitor::MetricsGroup {
public:
    P2PConnectorMetrics()  = default;
    ~P2PConnectorMetrics() = default;

public:
    bool init(kmonitor::MetricsGroupManager* manager) override;
    void report(const kmonitor::MetricsTags* tags, P2PConnectorDecodeSchedulerMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, P2PConnectorDecodeWorkerMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, P2PConnectorDecodeSchedulerStatusMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, P2PConnectorStreamStoreMetricsCollector1* collector);
    void report(const kmonitor::MetricsTags* tags, P2PConnectorStreamStoreMetricsCollector2* collector);
    void report(const kmonitor::MetricsTags* tags, P2PConnectorPrefillSchedulerMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, P2PConnectorPrefillWorkerWriteMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, P2PConnectorPrefillWorkerStatusMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, P2PConnectorPrefillWorkerStoreMetricsCollector* collector);

private:
    // decode schedule metrics
    kmonitor::MutableMetric* decode_schedule_qps_metric          = nullptr;
    kmonitor::MutableMetric* decode_schedule_failed_qps_metric   = nullptr;
    kmonitor::MutableMetric* decode_schedule_cost_time_us_metric = nullptr;

    // decode worker metrics
    kmonitor::MutableMetric* decode_worker_qps_metric                      = nullptr;
    kmonitor::MutableMetric* decode_worker_failed_qps_metric               = nullptr;
    kmonitor::MutableMetric* decode_worker_total_block_count_metric        = nullptr;
    kmonitor::MutableMetric* decode_worker_first_layer_wait_time_us_metric = nullptr;
    kmonitor::MutableMetric* decode_worker_total_cost_time_us_metric       = nullptr;

    // decode scheduler status metrics
    kmonitor::MutableMetric* decode_scheduler_check_once_cost_time_us_metric = nullptr;
    kmonitor::MutableMetric* decode_scheduler_inflight_context_count_metric  = nullptr;

    // stream store metrics
    kmonitor::MutableMetric* stream_store_stream_count_metric        = nullptr;
    kmonitor::MutableMetric* stream_store_timeout_count_metric       = nullptr;
    kmonitor::MutableMetric* stream_store_stream_wait_time_us_metric = nullptr;

    // prefill scheduler metrics
    kmonitor::MutableMetric* prefill_scheduler_qps_metric                = nullptr;
    kmonitor::MutableMetric* prefill_scheduler_failed_qps_metric         = nullptr;
    kmonitor::MutableMetric* prefill_scheduler_total_cost_time_us_metric = nullptr;

    // prefill worker metrics
    kmonitor::MutableMetric* prefill_worker_store_qps_metric                     = nullptr;
    kmonitor::MutableMetric* prefill_worker_store_failed_qps_metric              = nullptr;
    kmonitor::MutableMetric* prefill_worker_store_total_block_count_metric       = nullptr;
    kmonitor::MutableMetric* prefill_worker_store_store_wait_done_time_us_metric = nullptr;

    // prefill worker write metrics
    kmonitor::MutableMetric* prefill_worker_write_qps_metric                      = nullptr;
    kmonitor::MutableMetric* prefill_worker_write_failed_qps_metric               = nullptr;
    kmonitor::MutableMetric* prefill_worker_write_first_layer_wait_time_us_metric = nullptr;
    kmonitor::MutableMetric* prefill_worker_write_last_layer_wait_time_us_metric  = nullptr;
    kmonitor::MutableMetric* prefill_worker_write_total_cost_time_us_metric       = nullptr;

    // prefill worker status metrics
    kmonitor::MutableMetric* prefill_worker_wait_store_event_count_metric = nullptr;
    kmonitor::MutableMetric* prefill_worker_task_count_metric             = nullptr;
    kmonitor::MutableMetric* prefill_worker_computed_request_count_metric = nullptr;
};

}  // namespace rtp_llm