#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/Metrics.h"

namespace rtp_llm::transfer {

bool TransferMetrics::init(kmonitor::MetricsGroupManager* manager) {
    REGISTER_QPS_MUTABLE_METRIC(transfer_send_qps_metric, "rtp_llm_transfer_send_qps");
    REGISTER_QPS_MUTABLE_METRIC(transfer_send_error_qps_metric, "rtp_llm_transfer_send_error_qps");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_send_block_count_metric, "rtp_llm_transfer_send_block_count");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_send_total_block_size_metric, "rtp_llm_transfer_send_total_block_size");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_send_latency_us_metric, "rtp_llm_transfer_send_latency_us");

    REGISTER_QPS_MUTABLE_METRIC(transfer_recv_qps_metric, "rtp_llm_transfer_recv_qps");
    REGISTER_QPS_MUTABLE_METRIC(transfer_recv_error_qps_metric, "rtp_llm_transfer_recv_error_qps");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_recv_block_count_metric, "rtp_llm_transfer_recv_block_count");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_recv_total_block_size_metric, "rtp_llm_transfer_recv_total_block_size");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_recv_latency_us_metric, "rtp_llm_transfer_recv_latency_us");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_recv_wait_task_latency_us_metric,
                                  "rtp_llm_transfer_recv_wait_task_latency_us");

    REGISTER_QPS_MUTABLE_METRIC(transfer_recv_task_qps_metric, "rtp_llm_transfer_recv_task_qps");
    REGISTER_QPS_MUTABLE_METRIC(transfer_recv_task_error_qps_metric, "rtp_llm_transfer_recv_task_error_qps");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_recv_task_block_count_metric, "rtp_llm_transfer_recv_task_block_count");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_recv_task_total_block_size_metric,
                                  "rtp_llm_transfer_recv_task_total_block_size");
    REGISTER_GAUGE_MUTABLE_METRIC(transfer_recv_task_latency_us_metric, "rtp_llm_transfer_recv_task_latency_us");

    REGISTER_GAUGE_MUTABLE_METRIC(transfer_recv_store_task_count_metric, "rtp_llm_transfer_recv_store_task_count");
    return true;
}

void TransferMetrics::report(const kmonitor::MetricsTags* tags, SendMetricsCollector* collector) {
    REPORT_MUTABLE_QPS(transfer_send_qps_metric);
    if (!collector->success) {
        REPORT_MUTABLE_QPS(transfer_send_error_qps_metric);
    }
    REPORT_MUTABLE_METRIC(transfer_send_block_count_metric, collector->block_count);
    REPORT_MUTABLE_METRIC(transfer_send_total_block_size_metric, collector->total_block_size);
    REPORT_MUTABLE_METRIC(transfer_send_latency_us_metric, collector->latency_us);
}

void TransferMetrics::report(const kmonitor::MetricsTags* tags, RecvMetricsCollector* collector) {
    REPORT_MUTABLE_QPS(transfer_recv_qps_metric);
    if (!collector->success) {
        REPORT_MUTABLE_QPS(transfer_recv_error_qps_metric);
    }
    REPORT_MUTABLE_METRIC(transfer_recv_block_count_metric, collector->block_count);
    REPORT_MUTABLE_METRIC(transfer_recv_total_block_size_metric, collector->total_block_size);
    REPORT_MUTABLE_METRIC(transfer_recv_latency_us_metric, collector->latency_us);
    REPORT_MUTABLE_METRIC(transfer_recv_wait_task_latency_us_metric, collector->wait_task_latency_us);
}

void TransferMetrics::report(const kmonitor::MetricsTags* tags, RecvTaskMetricsCollector* collector) {
    REPORT_MUTABLE_QPS(transfer_recv_task_qps_metric);
    if (!collector->success) {
        REPORT_MUTABLE_QPS(transfer_recv_task_error_qps_metric);
    }
    REPORT_MUTABLE_METRIC(transfer_recv_task_block_count_metric, collector->block_count);
    REPORT_MUTABLE_METRIC(transfer_recv_task_total_block_size_metric, collector->total_block_size);
    REPORT_MUTABLE_METRIC(transfer_recv_task_latency_us_metric, collector->latency_us);
}

void TransferMetrics::report(const kmonitor::MetricsTags* tags, RecvStoreMetricsCollector* collector) {
    REPORT_MUTABLE_METRIC(transfer_recv_store_task_count_metric, collector->task_count);
}

}  // namespace rtp_llm::transfer
