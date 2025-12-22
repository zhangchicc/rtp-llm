#pragma once

#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>

#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/PrefillRpcServer.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorServer.h"
#include "rtp_llm/cpp/cache_new/BatchKVCacheResource.h"
#include "rtp_llm/cpp/normal_engine/NormalGenerateStream.h"

namespace rtp_llm {

/// @brief Configuration for P2PConnectorPerfTestServer
struct P2PConnectorPerfTestServerConfig {
    // TP settings
    int tp_size = 1;  // Number of PrefillRpcServers to create

    // Port settings (for prefill servers)
    uint32_t base_grpc_port     = 50051;  // Base port for prefill gRPC servers
    uint32_t base_transfer_port = 60051;  // Base port for prefill transfer servers

    // Decode server settings (for writeByLayer)
    std::string decode_ip   = "127.0.0.1";
    uint32_t    decode_port = 60151;  // Decode transfer server port (base)

    // KV cache settings
    int    num_layers  = 32;
    int    num_blocks  = 100;          // Number of blocks per request
    size_t block_size  = 1024 * 1024;  // 1MB per block
    bool   separate_kv = true;

    // Transfer settings
    int  tcp_io_thread_count     = 4;
    int  tcp_worker_thread_count = 8;
    bool use_rdma                = false;

    // Test settings
    int qps          = 10;     // Queries per second
    int duration_sec = 60;     // Test duration in seconds
    int deadline_ms  = 30000;  // Request deadline in milliseconds

    // Metrics
    bool enable_metrics = true;
};

/// @brief Statistics for perftest server
struct PerfTestServerStats {
    std::atomic<int64_t> total_requests{0};
    std::atomic<int64_t> successful_requests{0};
    std::atomic<int64_t> failed_requests{0};
    std::atomic<int64_t> total_latency_us{0};
    std::atomic<int64_t> min_latency_us{INT64_MAX};
    std::atomic<int64_t> max_latency_us{0};

    void recordSuccess(int64_t latency_us) {
        total_requests++;
        successful_requests++;
        total_latency_us += latency_us;

        int64_t current_min = min_latency_us.load();
        while (latency_us < current_min && !min_latency_us.compare_exchange_weak(current_min, latency_us)) {}

        int64_t current_max = max_latency_us.load();
        while (latency_us > current_max && !max_latency_us.compare_exchange_weak(current_max, latency_us)) {}
    }

    void recordFailure() {
        total_requests++;
        failed_requests++;
    }

    double avgLatencyMs() const {
        int64_t successful = successful_requests.load();
        if (successful == 0)
            return 0.0;
        return static_cast<double>(total_latency_us.load()) / successful / 1000.0;
    }

    double minLatencyMs() const {
        int64_t min_val = min_latency_us.load();
        return min_val == INT64_MAX ? 0.0 : static_cast<double>(min_val) / 1000.0;
    }

    double maxLatencyMs() const {
        return static_cast<double>(max_latency_us.load()) / 1000.0;
    }

    void print() const;
};

/// @brief P2P Connector Performance Test Server
/// Creates PrefillRpcServers and sends asyncWriteByLayer requests
class P2PConnectorPerfTestServer {
public:
    P2PConnectorPerfTestServer(const P2PConnectorPerfTestServerConfig& config);
    ~P2PConnectorPerfTestServer();

    /// @brief Initialize all prefill servers
    bool init();

    /// @brief Run the perftest (blocking)
    int run(std::atomic<bool>& running);

    /// @brief Stop the test
    void stop();

    /// @brief Print configuration
    void printConfig() const;

    /// @brief Get statistics
    const PerfTestServerStats& stats() const {
        return stats_;
    }

    /// @brief Get all prefill connectors
    std::vector<std::shared_ptr<P2PConnectorServer>> getConnectors() const;

    /// @brief Get prefill server addresses
    std::vector<std::pair<std::string, uint32_t>> getPrefillTransferServerAddrs() const;

private:
    /// @brief Create GrpcServerConfig for a specific TP rank
    GrpcServerConfig createServerConfig(int tp_rank) const;

    /// @brief Initialize device
    bool initDevice();

    /// @brief Initialize all prefill servers
    bool initServers();

    /// @brief Start all servers
    bool startServers();

    /// @brief Create a KVCacheResourceV1 with specified blocks
    std::shared_ptr<KVCacheResourceV1> createTestResource() const;

    /// @brief Create a mock GenerateStream for testing
    GenerateStreamPtr createMockStream(const std::string& unique_key) const;

    /// @brief Get decode transfer server addresses
    std::vector<std::pair<std::string, uint32_t>> getDecodeTransferServerAddrs() const;

    /// @brief Send async write requests at specified QPS
    void runLoadGenerator(std::atomic<bool>& running);

    /// @brief Process a single request (writeByLayer on all ranks + addStream on rank 0)
    void processRequest(int64_t request_id);

private:
    P2PConnectorPerfTestServerConfig config_;
    GptInitParameter                 gpt_params_;
    DeviceBase*                      device_ = nullptr;

    // Prefill RPC servers for each TP rank
    std::vector<std::shared_ptr<PrefillRpcServer>> servers_;

    // Metrics
    kmonitor::MetricsReporterPtr metrics_reporter_;

    // Statistics
    PerfTestServerStats stats_;

    // State
    std::atomic<bool>    initialized_{false};
    std::atomic<bool>    running_{false};
    std::atomic<int64_t> request_id_counter_{0};
};

}  // namespace rtp_llm
