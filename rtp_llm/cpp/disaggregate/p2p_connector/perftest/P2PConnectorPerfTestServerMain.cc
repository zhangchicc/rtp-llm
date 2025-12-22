#include "rtp_llm/cpp/disaggregate/p2p_connector/perftest/P2PConnectorPerfTestServer.h"

#include <iostream>
#include <csignal>
#include <cstdlib>

using namespace rtp_llm;

static std::atomic<bool> g_running{true};

void signalHandler(int signum) {
    std::cout << "\nReceived signal " << signum << ", shutting down..." << std::endl;
    g_running = false;
}

void printUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --tp_size <num>            Number of TP ranks (prefill servers) [default: 1]" << std::endl;
    std::cout << "  --decode_ip <ip>           Decode server IP [default: 127.0.0.1]" << std::endl;
    std::cout << "  --decode_port <port>       Decode transfer port (base) [default: 60151]" << std::endl;
    std::cout << "  --base_grpc_port <port>    Base gRPC port for prefill servers [default: 50051]" << std::endl;
    std::cout << "  --base_transfer_port <port> Base transfer port for prefill servers [default: 60051]" << std::endl;
    std::cout << "  --num_layers <num>         Number of layers [default: 32]" << std::endl;
    std::cout << "  --num_blocks <num>         Number of blocks per request [default: 100]" << std::endl;
    std::cout << "  --block_size <bytes>       Block size in bytes [default: 1048576]" << std::endl;
    std::cout << "  --qps <num>                Queries per second [default: 10]" << std::endl;
    std::cout << "  --duration <sec>           Test duration in seconds [default: 60]" << std::endl;
    std::cout << "  --deadline_ms <ms>         Request deadline in milliseconds [default: 30000]" << std::endl;
    std::cout << "  --use_rdma                 Use RDMA for transfer [default: false]" << std::endl;
    std::cout << "  --help                     Show this help message" << std::endl;
}

int main(int argc, char* argv[]) {
    // Register signal handlers
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Parse command line arguments
    P2PConnectorPerfTestServerConfig config;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            return 0;
        } else if (arg == "--tp_size" && i + 1 < argc) {
            config.tp_size = std::atoi(argv[++i]);
        } else if (arg == "--decode_ip" && i + 1 < argc) {
            config.decode_ip = argv[++i];
        } else if (arg == "--decode_port" && i + 1 < argc) {
            config.decode_port = std::atoi(argv[++i]);
        } else if (arg == "--base_grpc_port" && i + 1 < argc) {
            config.base_grpc_port = std::atoi(argv[++i]);
        } else if (arg == "--base_transfer_port" && i + 1 < argc) {
            config.base_transfer_port = std::atoi(argv[++i]);
        } else if (arg == "--num_layers" && i + 1 < argc) {
            config.num_layers = std::atoi(argv[++i]);
        } else if (arg == "--num_blocks" && i + 1 < argc) {
            config.num_blocks = std::atoi(argv[++i]);
        } else if (arg == "--block_size" && i + 1 < argc) {
            config.block_size = std::atol(argv[++i]);
        } else if (arg == "--qps" && i + 1 < argc) {
            config.qps = std::atoi(argv[++i]);
        } else if (arg == "--duration" && i + 1 < argc) {
            config.duration_sec = std::atoi(argv[++i]);
        } else if (arg == "--deadline_ms" && i + 1 < argc) {
            config.deadline_ms = std::atoi(argv[++i]);
        } else if (arg == "--use_rdma") {
            config.use_rdma = true;
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            printUsage(argv[0]);
            return 1;
        }
    }

    // Create and run server
    P2PConnectorPerfTestServer server(config);

    if (!server.init()) {
        std::cerr << "Failed to initialize P2PConnectorPerfTestServer" << std::endl;
        return 1;
    }

    return server.run(g_running);
}
