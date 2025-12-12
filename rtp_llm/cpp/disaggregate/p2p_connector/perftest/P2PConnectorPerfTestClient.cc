#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/P2PConnectorPerfTestClient.h"

#include <iostream>
#include <chrono>
#include <iomanip>

#include "rtp_llm/cpp/devices/DeviceFactory.h"

namespace rtp_llm {

void PerfTestStats::print() const {
    std::cout << "\n=== Performance Test Statistics ===" << std::endl;
    std::cout << "Total Requests:      " << total_requests.load() << std::endl;
    std::cout << "Successful Requests: " << successful_requests.load() << std::endl;
    std::cout << "Failed Requests:     " << failed_requests.load() << std::endl;
    std::cout << "Average Latency:     " << std::fixed << std::setprecision(2) << avgLatencyMs() << " ms" << std::endl;
    std::cout << "Min Latency:         " << std::fixed << std::setprecision(2) << minLatencyMs() << " ms" << std::endl;
    std::cout << "Max Latency:         " << std::fixed << std::setprecision(2) << maxLatencyMs() << " ms" << std::endl;

    int64_t successful = successful_requests.load();
    if (successful > 0) {
        double success_rate = 100.0 * successful / total_requests.load();
        std::cout << "Success Rate:        " << std::fixed << std::setprecision(2) << success_rate << "%" << std::endl;
    }
    std::cout << "====================================" << std::endl;
}

P2PConnectorPerfTestClient::P2PConnectorPerfTestClient(const P2PConnectorPerfTestClientConfig& config):
    config_(config) {}

P2PConnectorPerfTestClient::~P2PConnectorPerfTestClient() {
    stop();
}

bool P2PConnectorPerfTestClient::init() {
    if (initialized_) {
        std::cerr << "P2PConnectorPerfTestClient already initialized" << std::endl;
        return false;
    }

    // Initialize device
    if (!initDevice()) {
        return false;
    }

    // Create metrics reporter
    if (config_.enable_metrics) {
        auto kmon_tags = kmonitor::MetricsTags();
        metrics_reporter_.reset(new kmonitor::MetricsReporter("", "", kmon_tags));
    }

    // Initialize all decode servers
    if (!initServers()) {
        return false;
    }

    initialized_ = true;
    std::cout << "P2PConnectorPerfTestClient initialized with " << config_.tp_size << " decode servers" << std::endl;
    return true;
}

bool P2PConnectorPerfTestClient::initDevice() {
    GptInitParameter gpt_init_params;
    gpt_init_params.device_resource_config.device_reserve_memory_bytes = 2L * 1024 * 1024 * 1024;  // 2GB
    gpt_init_params.device_resource_config.host_reserve_memory_bytes   = 1L * 1024 * 1024 * 1024;  // 1GB
    DeviceFactory::initDevices(gpt_init_params);
    device_ = DeviceFactory::getDefaultDevice();
    if (!device_) {
        std::cerr << "Failed to get device" << std::endl;
        return false;
    }
    return true;
}

GrpcServerConfig P2PConnectorPerfTestClient::createServerConfig(int tp_rank) const {
    GrpcServerConfig config;
    config.tp_rank                 = tp_rank;
    config.grpc_port               = config_.base_grpc_port + tp_rank;
    config.transfer_port           = config_.base_transfer_port + tp_rank;
    config.num_layers              = config_.num_layers;
    config.num_blocks              = config_.num_blocks;
    config.block_size              = config_.block_size;
    config.separate_kv             = config_.separate_kv;
    config.tcp_io_thread_count     = config_.tcp_io_thread_count;
    config.tcp_worker_thread_count = config_.tcp_worker_thread_count;
    config.use_rdma                = config_.use_rdma;
    return config;
}

bool P2PConnectorPerfTestClient::initServers() {
    servers_.resize(config_.tp_size);

    for (int tp_rank = 0; tp_rank < config_.tp_size; ++tp_rank) {
        auto server_config = createServerConfig(tp_rank);
        servers_[tp_rank]  = std::make_shared<DecodeRpcServer>(server_config, device_, metrics_reporter_);
    }

    return true;
}

bool P2PConnectorPerfTestClient::startServers() {
    for (auto& server : servers_) {
        if (!server->start()) {
            std::cerr << "Failed to start decode server" << std::endl;
            return false;
        }
    }
    return true;
}

std::shared_ptr<KVCacheResourceV1> P2PConnectorPerfTestClient::createTestResource() const {
    auto resource = std::make_shared<KVCacheResourceV1>();

    // Initialize with 1 group (for simplicity)
    resource->initGroups(1);

    // Fill with block indices from 0 to num_blocks-1
    resource->resizeBlocks(config_.num_blocks, 0);
    auto& blocks = resource->blocks(0);
    for (int i = 0; i < config_.num_blocks; ++i) {
        blocks[i] = i;
    }

    return resource;
}

std::shared_ptr<P2PConnectorClientMeta> P2PConnectorPerfTestClient::createTestMeta(int64_t request_id) const {
    std::string unique_key = "perftest_request_" + std::to_string(request_id);

    return std::make_shared<P2PConnectorClientMeta>(
        request_id, unique_key, config_.prefill_ip, config_.prefill_port, config_.deadline_ms);
}

void P2PConnectorPerfTestClient::processRequest(int64_t request_id) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Get the connector from the first server (TP rank 0 has scheduler)
    auto connector = servers_[0]->getConnector();
    if (!connector) {
        stats_.recordFailure();
        std::cerr << "Failed to get connector for request " << request_id << std::endl;
        return;
    }

    // Create test resource and meta
    auto resource = createTestResource();
    auto meta     = createTestMeta(request_id);

    // Send async read
    auto async_context = connector->asyncRead(resource, meta);
    if (!async_context) {
        stats_.recordFailure();
        std::cerr << "Failed to start asyncRead for request " << request_id << std::endl;
        return;
    }

    // Wait for completion (with timeout)
    auto deadline  = std::chrono::system_clock::now() + std::chrono::milliseconds(config_.deadline_ms);
    bool completed = false;

    while (std::chrono::system_clock::now() < deadline) {
        if (async_context->done()) {
            completed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    auto end_time   = std::chrono::high_resolution_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    if (completed && async_context->success()) {
        stats_.recordSuccess(latency_us);
    } else {
        stats_.recordFailure();
        if (!completed) {
            std::cerr << "Request " << request_id << " timed out" << std::endl;
        } else {
            std::cerr << "Request " << request_id << " failed" << std::endl;
        }
    }
}

void P2PConnectorPerfTestClient::runLoadGenerator(std::atomic<bool>& running) {
    // Calculate interval between requests based on QPS
    auto interval_us = config_.qps > 0 ? 1000000 / config_.qps : 1000000;

    auto test_start = std::chrono::high_resolution_clock::now();
    auto test_end   = test_start + std::chrono::seconds(config_.duration_sec);

    std::cout << "Starting load generator at " << config_.qps << " QPS for " << config_.duration_sec << " seconds..."
              << std::endl;

    while (running && running_ && std::chrono::high_resolution_clock::now() < test_end) {
        auto request_start = std::chrono::high_resolution_clock::now();

        // Generate request ID
        int64_t request_id = request_id_counter_++;

        // Process request in a separate thread to not block the load generator
        std::thread request_thread([this, request_id]() { processRequest(request_id); });
        request_thread.detach();

        // Wait until next request should be sent
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()
                                                                             - request_start)
                           .count();

        if (elapsed < interval_us) {
            std::this_thread::sleep_for(std::chrono::microseconds(interval_us - elapsed));
        }
    }

    // Wait a bit for in-flight requests to complete
    std::cout << "Load generation completed. Waiting for in-flight requests..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
}

int P2PConnectorPerfTestClient::run(std::atomic<bool>& running) {
    if (!initialized_) {
        std::cerr << "P2PConnectorPerfTestClient not initialized" << std::endl;
        return -1;
    }

    running_ = true;

    // Start all decode servers
    if (!startServers()) {
        return -1;
    }

    printConfig();
    std::cout << "\nP2PConnectorPerfTestClient starting..." << std::endl;

    // Run load generator
    runLoadGenerator(running);

    // Print final statistics
    stats_.print();

    stop();
    return 0;
}

void P2PConnectorPerfTestClient::stop() {
    if (!running_) {
        return;
    }
    running_ = false;

    std::cout << "Stopping P2PConnectorPerfTestClient..." << std::endl;

    for (auto& server : servers_) {
        if (server) {
            server->stop();
        }
    }

    std::cout << "P2PConnectorPerfTestClient stopped" << std::endl;
}

void P2PConnectorPerfTestClient::printConfig() const {
    std::cout << "\n=== P2P Connector PerfTest Client Configuration ===" << std::endl;
    std::cout << "TP Size: " << config_.tp_size << std::endl;
    std::cout << "Base gRPC Port: " << config_.base_grpc_port << std::endl;
    std::cout << "Base Transfer Port: " << config_.base_transfer_port << std::endl;
    std::cout << "Prefill Server: " << config_.prefill_ip << ":" << config_.prefill_port << std::endl;
    std::cout << "Num Layers: " << config_.num_layers << std::endl;
    std::cout << "Num Blocks: " << config_.num_blocks << std::endl;
    std::cout << "Block Size: " << config_.block_size << " bytes" << std::endl;
    std::cout << "Separate KV: " << (config_.separate_kv ? "true" : "false") << std::endl;
    std::cout << "Use RDMA: " << (config_.use_rdma ? "true" : "false") << std::endl;
    std::cout << "QPS: " << config_.qps << std::endl;
    std::cout << "Duration: " << config_.duration_sec << " seconds" << std::endl;
    std::cout << "Deadline: " << config_.deadline_ms << " ms" << std::endl;
    std::cout << "\nDecode Servers:" << std::endl;
    for (const auto& server : servers_) {
        std::cout << "  TP Rank " << server->tpRank() << ": gRPC=" << server->grpcPort()
                  << ", Transfer=" << server->transferPort() << std::endl;
    }
    std::cout << "===================================================" << std::endl;
}

std::vector<std::shared_ptr<P2PConnectorClient>> P2PConnectorPerfTestClient::getConnectors() const {
    std::vector<std::shared_ptr<P2PConnectorClient>> connectors;
    for (const auto& server : servers_) {
        auto connector = server->getConnector();
        if (connector) {
            connectors.push_back(connector);
        }
    }
    return connectors;
}

std::vector<std::pair<std::string, uint32_t>> P2PConnectorPerfTestClient::getDecodeTransferServerAddrs() const {
    std::vector<std::pair<std::string, uint32_t>> addrs;
    for (const auto& server : servers_) {
        addrs.emplace_back("127.0.0.1", server->transferPort());
    }
    return addrs;
}

}  // namespace rtp_llm
