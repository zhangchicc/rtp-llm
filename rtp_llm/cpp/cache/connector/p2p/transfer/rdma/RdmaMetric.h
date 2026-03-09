#pragma once

#include "kmonitor/client/MetricsReporter.h"
#include "rtp_llm/cpp/metrics/RtpLLMMetrics.h"

namespace rtp_llm {

class RdmaMetricsCollector final {
public:
    bool    success              = true;
    int64_t start_time_us        = 0;
    int64_t pending_done_time_us = 0;
    int64_t write_done_time_us   = 0;
};

class RdmaMetric: public kmonitor::MetricsGroup {
public:
    ~RdmaMetric() = default;

public:
    bool init(kmonitor::MetricsGroupManager* manager) override;
    void report(const kmonitor::MetricsTags* tags, RdmaMetricsCollector* collector);

private:
    kmonitor::MutableMetric* rdma_qps_metric                = nullptr;
    kmonitor::MutableMetric* rdma_error_qps_metric          = nullptr;
    kmonitor::MutableMetric* rdma_pending_latency_us_metric = nullptr;
    kmonitor::MutableMetric* rdma_write_latency_us_metric   = nullptr;
};

}  // namespace rtp_llm