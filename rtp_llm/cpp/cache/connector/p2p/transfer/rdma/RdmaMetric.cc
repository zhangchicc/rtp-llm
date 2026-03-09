#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaMetric.h"

namespace rtp_llm {

bool RdmaMetric::init(kmonitor::MetricsGroupManager* manager) {
    REGISTER_QPS_MUTABLE_METRIC(rdma_qps_metric, "rtp_llm.transfer.rdma.qps");
    REGISTER_QPS_MUTABLE_METRIC(rdma_error_qps_metric, "rtp_llm.transfer.rdma.error_qps");
    REGISTER_GAUGE_MUTABLE_METRIC(rdma_pending_latency_us_metric, "rtp_llm.transfer.rdma.pending_latency_us");
    REGISTER_GAUGE_MUTABLE_METRIC(rdma_write_latency_us_metric, "rtp_llm.transfer.rdma.write_latency_us");
    return true;
}

void RdmaMetric::report(const kmonitor::MetricsTags* tags, RdmaMetricsCollector* collector) {
    REPORT_MUTABLE_QPS(rdma_qps_metric);
    if (!collector->success) {
        REPORT_MUTABLE_QPS(rdma_error_qps_metric);
    }
    auto pending_latency_us = collector->pending_done_time_us - collector->start_time_us;
    auto write_latency_us   = collector->write_done_time_us - collector->pending_done_time_us;
    if (pending_latency_us > 0) {
        REPORT_MUTABLE_METRIC(rdma_pending_latency_us_metric, pending_latency_us);
    }
    if (write_latency_us > 0) {
        REPORT_MUTABLE_METRIC(rdma_write_latency_us_metric, write_latency_us);
    }
}

}  // namespace rtp_llm