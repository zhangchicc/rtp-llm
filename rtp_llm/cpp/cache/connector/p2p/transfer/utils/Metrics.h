#pragma once

#include "kmonitor/client/MetricsReporter.h"
#include "rtp_llm/cpp/metrics/RtpLLMMetrics.h"

#include <cstdint>

namespace rtp_llm::transfer {

class SendMetricsCollector final {
public:
    SendMetricsCollector()  = default;
    ~SendMetricsCollector() = default;

public:
    bool    success          = true;
    int64_t block_count      = 0;
    int64_t total_block_size = 0;
    int64_t latency_us       = 0;
};

class RecvMetricsCollector final {
public:
    RecvMetricsCollector()  = default;
    ~RecvMetricsCollector() = default;

public:
    bool    success              = true;
    int64_t block_count          = 0;
    int64_t total_block_size     = 0;
    int64_t latency_us           = 0;
    int64_t wait_task_latency_us = 0;
};

class RecvTaskMetricsCollector final {
public:
    RecvTaskMetricsCollector()  = default;
    ~RecvTaskMetricsCollector() = default;

public:
    bool    success                    = true;
    int64_t block_count                = 0;
    int64_t total_block_size           = 0;
    int     wait_processing_latency_us = 0;
    int64_t latency_us                 = 0;
};

class RecvStoreMetricsCollector final {
public:
    RecvStoreMetricsCollector()  = default;
    ~RecvStoreMetricsCollector() = default;

public:
    int64_t task_count = 0;
};

class TransferMetrics final: public kmonitor::MetricsGroup {
public:
    ~TransferMetrics() override = default;

public:
    bool init(kmonitor::MetricsGroupManager* manager) override;
    void report(const kmonitor::MetricsTags* tags, SendMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, RecvMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, RecvTaskMetricsCollector* collector);
    void report(const kmonitor::MetricsTags* tags, RecvStoreMetricsCollector* collector);

private:
    kmonitor::MutableMetric* transfer_send_qps_metric              = nullptr;
    kmonitor::MutableMetric* transfer_send_error_qps_metric        = nullptr;
    kmonitor::MutableMetric* transfer_send_block_count_metric      = nullptr;
    kmonitor::MutableMetric* transfer_send_total_block_size_metric = nullptr;
    kmonitor::MutableMetric* transfer_send_latency_us_metric       = nullptr;

    kmonitor::MutableMetric* transfer_recv_qps_metric                  = nullptr;
    kmonitor::MutableMetric* transfer_recv_error_qps_metric            = nullptr;
    kmonitor::MutableMetric* transfer_recv_block_count_metric          = nullptr;
    kmonitor::MutableMetric* transfer_recv_total_block_size_metric     = nullptr;
    kmonitor::MutableMetric* transfer_recv_latency_us_metric           = nullptr;
    kmonitor::MutableMetric* transfer_recv_wait_task_latency_us_metric = nullptr;

    kmonitor::MutableMetric* transfer_recv_task_qps_metric              = nullptr;
    kmonitor::MutableMetric* transfer_recv_task_error_qps_metric        = nullptr;
    kmonitor::MutableMetric* transfer_recv_task_block_count_metric      = nullptr;
    kmonitor::MutableMetric* transfer_recv_task_total_block_size_metric = nullptr;
    kmonitor::MutableMetric* transfer_recv_task_latency_us_metric       = nullptr;

    kmonitor::MutableMetric* transfer_recv_store_task_count_metric = nullptr;
};

}  // namespace rtp_llm::transfer