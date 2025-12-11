#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/P2PConnectorPerfTestServer.h"

#include <iostream>
#include <chrono>
#include <iomanip>

#include "rtp_llm/cpp/devices/DeviceFactory.h"
#include "rtp_llm/cpp/core/torch_utils/BufferTorchUtils.h"

namespace rtp_llm {

void PerfTestServerStats::print() const {
    std::cout << "\n=== Performance Test Server Statistics ===" << std::endl;
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
    std::cout << "============================================" << std::endl;
}

P2PConnectorPerfTestServer::P2PConnectorPerfTestServer(const P2PConnectorPerfTestServerConfig& config):
    config_(config) {}

P2PConnectorPerfTestServer::~P2PConnectorPerfTestServer() {
    stop();
}

bool P2PConnectorPerfTestServer::init() {
    if (initialized_) {
        std::cerr << "P2PConnectorPerfTestServer already initialized" << std::endl;
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

    // Setup GptInitParameter for mock stream creation
    gpt_params_.num_layers_  = config_.num_layers;
    gpt_params_.max_seq_len_ = config_.num_blocks * 128;  // Estimate
    gpt_params_.vocab_size_  = 32000;

    // Initialize all prefill servers
    if (!initServers()) {
        return false;
    }

    initialized_ = true;
    std::cout << "P2PConnectorPerfTestServer initialized with " << config_.tp_size << " prefill servers" << std::endl;
    return true;
}

bool P2PConnectorPerfTestServer::initDevice() {
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

GrpcServerConfig P2PConnectorPerfTestServer::createServerConfig(int tp_rank) const {
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

bool P2PConnectorPerfTestServer::initServers() {
    servers_.resize(config_.tp_size);

    for (int tp_rank = 0; tp_rank < config_.tp_size; ++tp_rank) {
        auto server_config = createServerConfig(tp_rank);
        servers_[tp_rank]  = std::make_shared<PrefillRpcServer>(server_config, device_, metrics_reporter_);
    }

    return true;
}

bool P2PConnectorPerfTestServer::startServers() {
    for (auto& server : servers_) {
        if (!server->start()) {
            std::cerr << "Failed to start prefill server" << std::endl;
            return false;
        }
    }
    return true;
}

std::shared_ptr<KVCacheResourceV1> P2PConnectorPerfTestServer::createTestResource() const {
    auto resource = std::make_shared<KVCacheResourceV1>();

    // Initialize with 1 group
    resource->initGroups(1);

    // Fill with block indices from 0 to num_blocks-1
    resource->resizeBlocks(config_.num_blocks, 0);
    auto& blocks = resource->blocks(0);
    for (int i = 0; i < config_.num_blocks; ++i) {
        blocks[i] = i;
    }

    // Initialize layer block ids (for writeByLayer)
    auto& layer_block_ids = resource->layerBlockIds();
    layer_block_ids.resize(config_.num_layers);
    for (int layer = 0; layer < config_.num_layers; ++layer) {
        layer_block_ids[layer] = std::make_shared<BlockIds>();
        layer_block_ids[layer]->resize(config_.num_blocks, 0);
        auto& layer_blocks = layer_block_ids[layer]->blocks();
        for (int i = 0; i < config_.num_blocks; ++i) {
            layer_blocks[i] = i;
        }
    }

    return resource;
}

GenerateStreamPtr P2PConnectorPerfTestServer::createMockStream(const std::string& unique_key) const {
    // Create a simple mock GenerateInput
    auto generate_input             = std::make_shared<GenerateInput>();
    auto generate_config            = std::make_shared<GenerateConfig>();
    generate_input->generate_config = generate_config;

    // Create simple input_ids
    std::vector<int> input_ids(config_.num_blocks * 128, 1);  // Dummy input
    generate_input->input_ids = rtp_llm::vector2Buffer(input_ids);

    // Set unique key for P2P separation
    generate_config->pd_sepration_unique_key = unique_key;

    ResourceContext resource_context;

    // Create stream with perf_test flag
    auto stream = std::make_shared<NormalGenerateStream>(generate_input,
                                                         gpt_params_,
                                                         resource_context,
                                                         metrics_reporter_,
                                                         0,    // extra_reserve_token_num
                                                         true  // perf_test
    );

    return stream;
}

std::vector<std::pair<std::string, uint32_t>> P2PConnectorPerfTestServer::getDecodeTransferServerAddrs() const {
    std::vector<std::pair<std::string, uint32_t>> addrs;
    for (int i = 0; i < config_.tp_size; ++i) {
        addrs.emplace_back(config_.decode_ip, config_.decode_port + i);
    }
    return addrs;
}

void P2PConnectorPerfTestServer::processRequest(int64_t request_id) {
    auto start_time = std::chrono::high_resolution_clock::now();

    std::string unique_key = "perftest_request_" + std::to_string(request_id);

    // Create test resource
    auto resource = createTestResource();

    // Get decode transfer server addresses
    auto decode_addrs = getDecodeTransferServerAddrs();

    bool                                                         all_success = true;
    std::vector<std::shared_ptr<KVCacheConnector::AsyncContext>> async_contexts;

    // Call asyncWriteByLayer on all ranks for each layer
    for (int layer = 0; layer < config_.num_layers; ++layer) {
        for (int tp_rank = 0; tp_rank < config_.tp_size; ++tp_rank) {
            auto connector = servers_[tp_rank]->getConnector();
            if (!connector) {
                std::cerr << "Failed to get connector for TP rank " << tp_rank << std::endl;
                all_success = false;
                continue;
            }

            // Create meta for this layer write (using a simple approach)
            // Note: asyncWriteByLayer doesn't need full Meta, we use nullptr or simple impl
            auto async_context = connector->asyncWriteByLayer(layer, resource, nullptr);
            if (async_context) {
                async_contexts.push_back(async_context);
            } else {
                all_success = false;
            }
        }
    }

    // On rank 0, also call addStream (for stream management testing)
    if (config_.tp_size > 0) {
        auto connector = servers_[0]->getConnector();
        if (connector) {
            // Create a mock stream and add it
            auto mock_stream = createMockStream(unique_key);
            if (mock_stream) {
                connector->addStream(unique_key, mock_stream);
            }
        }
    }

    // Wait for all async contexts to complete (with timeout)
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(config_.deadline_ms);

    for (auto& async_context : async_contexts) {
        while (std::chrono::system_clock::now() < deadline) {
            if (async_context->done()) {
                if (!async_context->success()) {
                    all_success = false;
                }
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        if (!async_context->done()) {
            all_success = false;  // Timeout
        }
    }

    auto end_time   = std::chrono::high_resolution_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    if (all_success) {
        stats_.recordSuccess(latency_us);
    } else {
        stats_.recordFailure();
    }
}

void P2PConnectorPerfTestServer::runLoadGenerator(std::atomic<bool>& running) {
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

        // Process request in a separate thread
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

    // Wait for in-flight requests to complete
    std::cout << "Load generation completed. Waiting for in-flight requests..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
}

int P2PConnectorPerfTestServer::run(std::atomic<bool>& running) {
    if (!initialized_) {
        std::cerr << "P2PConnectorPerfTestServer not initialized" << std::endl;
        return -1;
    }

    running_ = true;

    // Start all prefill servers
    if (!startServers()) {
        return -1;
    }

    printConfig();
    std::cout << "\nP2PConnectorPerfTestServer starting..." << std::endl;

    // Run load generator
    runLoadGenerator(running);

    // Print final statistics
    stats_.print();

    stop();
    return 0;
}

void P2PConnectorPerfTestServer::stop() {
    if (!running_) {
        return;
    }
    running_ = false;

    std::cout << "Stopping P2PConnectorPerfTestServer..." << std::endl;

    for (auto& server : servers_) {
        if (server) {
            server->stop();
        }
    }

    std::cout << "P2PConnectorPerfTestServer stopped" << std::endl;
}

void P2PConnectorPerfTestServer::printConfig() const {
    std::cout << "\n=== P2P Connector PerfTest Server Configuration ===" << std::endl;
    std::cout << "TP Size: " << config_.tp_size << std::endl;
    std::cout << "Base gRPC Port: " << config_.base_grpc_port << std::endl;
    std::cout << "Base Transfer Port: " << config_.base_transfer_port << std::endl;
    std::cout << "Decode Server: " << config_.decode_ip << ":" << config_.decode_port << std::endl;
    std::cout << "Num Layers: " << config_.num_layers << std::endl;
    std::cout << "Num Blocks: " << config_.num_blocks << std::endl;
    std::cout << "Block Size: " << config_.block_size << " bytes" << std::endl;
    std::cout << "Separate KV: " << (config_.separate_kv ? "true" : "false") << std::endl;
    std::cout << "Use RDMA: " << (config_.use_rdma ? "true" : "false") << std::endl;
    std::cout << "QPS: " << config_.qps << std::endl;
    std::cout << "Duration: " << config_.duration_sec << " seconds" << std::endl;
    std::cout << "Deadline: " << config_.deadline_ms << " ms" << std::endl;
    std::cout << "\nPrefill Servers:" << std::endl;
    for (const auto& server : servers_) {
        std::cout << "  TP Rank " << server->tpRank() << ": gRPC=" << server->grpcPort()
                  << ", Transfer=" << server->transferPort() << std::endl;
    }
    std::cout << "===================================================" << std::endl;
}

std::vector<std::shared_ptr<P2PConnectorPrefill>> P2PConnectorPerfTestServer::getConnectors() const {
    std::vector<std::shared_ptr<P2PConnectorPrefill>> connectors;
    for (const auto& server : servers_) {
        auto connector = server->getConnector();
        if (connector) {
            connectors.push_back(connector);
        }
    }
    return connectors;
}

std::vector<std::pair<std::string, uint32_t>> P2PConnectorPerfTestServer::getPrefillTransferServerAddrs() const {
    std::vector<std::pair<std::string, uint32_t>> addrs;
    for (const auto& server : servers_) {
        addrs.emplace_back("127.0.0.1", server->transferPort());
    }
    return addrs;
}

}  // namespace rtp_llm
