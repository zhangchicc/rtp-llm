#pragma once

#include <cstdint>
#include <sstream>
#include <string>

namespace rtp_llm {

struct TransferConfig {
    // Whether to use RDMA for transfer (otherwise TCP only).
    bool cache_store_rdma_mode = false;

    // Scheduling / misc knobs kept for compatibility with upstream configs.
    int wrr_available_ratio = 80;
    int rank_factor         = 0;
    int thread_count        = 16;

    // RDMA connection / resource settings.
    int rdma_connect_timeout_ms      = 250;
    int rdma_qp_count_per_connection = 2;
    int rdma_io_thread_count         = 4;
    int rdma_worker_thread_count     = 2;

    // TCP (messager/arpc) thread settings.
    int messager_io_thread_count     = 2;
    int messager_worker_thread_count = 16;

    // Max waiting time for an RDMA transfer to finish.
    int64_t rdma_transfer_wait_timeout_ms = 180 * 1000;

    // Max block pairs per RDMA connection. 0 means no limit.
    int rdma_max_block_pairs_per_connection = 0;

    std::string to_string() const {
        std::ostringstream oss;
        oss << "cache_store_rdma_mode: " << cache_store_rdma_mode << "\n"
            << "wrr_available_ratio: " << wrr_available_ratio << "\n"
            << "rank_factor: " << rank_factor << "\n"
            << "thread_count: " << thread_count << "\n"
            << "rdma_connect_timeout_ms: " << rdma_connect_timeout_ms << "\n"
            << "rdma_qp_count_per_connection: " << rdma_qp_count_per_connection << "\n"
            << "rdma_io_thread_count: " << rdma_io_thread_count << "\n"
            << "rdma_worker_thread_count: " << rdma_worker_thread_count << "\n"
            << "messager_io_thread_count: " << messager_io_thread_count << "\n"
            << "messager_worker_thread_count: " << messager_worker_thread_count << "\n"
            << "rdma_transfer_wait_timeout_ms: " << rdma_transfer_wait_timeout_ms << "\n"
            << "rdma_max_block_pairs_per_connection: " << rdma_max_block_pairs_per_connection;
        return oss.str();
    }
};

}  // namespace rtp_llm
