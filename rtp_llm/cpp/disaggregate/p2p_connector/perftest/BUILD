load("//:def.bzl", "copts")
load("//rtp_llm/cpp/devices:device_defs.bzl", "device_impl_target", "device_test_envs")

package(default_visibility = ["//visibility:public"])

perftest_copts = copts()

###############################################################################
# Shared Libraries
###############################################################################

# Mock KVCacheAllocator for perftest (shared by all servers)
cc_library(
    name = "perftest_kv_cache_allocator",
    srcs = ["PerfTestKVCacheAllocator.cc"],
    hdrs = ["PerfTestKVCacheAllocator.h"],
    copts = perftest_copts,
    deps = [
        "//rtp_llm/cpp/cache:cache",
        "//rtp_llm/cpp/devices:devices_base",
        "//rtp_llm/cpp/core:buffer",
        "@local_config_cuda//cuda:nccl",  # dependency from devicebase
    ],
)

# Base GrpcServer class
cc_library(
    name = "grpc_server",
    srcs = ["GrpcServer.cc"],
    hdrs = ["GrpcServer.h"],
    copts = perftest_copts,
    deps = [
        ":perftest_kv_cache_allocator",
        "//rtp_llm/cpp/model_rpc:tp_broadcast_manager",
        "//rtp_llm/cpp/config:gpt_init_params",
        "//rtp_llm/cpp/devices:devices_base",
        "//rtp_llm/cpp/model_rpc/proto:model_rpc_service_cc_proto",
        "@havenask//aios/kmonitor:kmonitor_client_cpp",
    ],
)

###############################################################################
# Prefill RPC Server
###############################################################################

cc_library(
    name = "prefill_rpc_server",
    srcs = ["PrefillRpcServer.cc"],
    hdrs = ["PrefillRpcServer.h"],
    copts = perftest_copts,
    deps = [
        ":grpc_server",
        "//rtp_llm/cpp/disaggregate/p2p_connector:server_connector",
    ],
)

###############################################################################
# Decode RPC Server
###############################################################################

cc_library(
    name = "decode_rpc_server",
    srcs = ["DecodeRpcServer.cc"],
    hdrs = ["DecodeRpcServer.h"],
    copts = perftest_copts,
    deps = [
        ":grpc_server",
        "//rtp_llm/cpp/disaggregate/p2p_connector:client_connector",
    ],
)

###############################################################################
# PerfTest Server
###############################################################################

cc_library(
    name = "perftest_server",
    srcs = ["P2PConnectorPerfTestServer.cc"],
    hdrs = ["P2PConnectorPerfTestServer.h"],
    copts = perftest_copts,
    deps = [
        ":prefill_rpc_server",
        "//rtp_llm/cpp/cache:batch_kv_cache_resource",
        "//rtp_llm/cpp/disaggregate/p2p_connector:server_connector",
        "//rtp_llm/cpp/normal_engine:normal_engine",
    ],
)

cc_binary(
    name = "p2p_connector_perftest_server",
    srcs = ["P2PConnectorPerfTestServerMain.cc"],
    copts = perftest_copts,
    deps = [
        ":perftest_server",
        "//rtp_llm/cpp/metrics:metrics",
        "@havenask//aios/autil:log",
        "@havenask//aios/autil:net",
    ] + device_impl_target(),
)

###############################################################################
# PerfTest Client
###############################################################################

cc_library(
    name = "perftest_client",
    srcs = ["P2PConnectorPerfTestClient.cc"],
    hdrs = ["P2PConnectorPerfTestClient.h"],
    copts = perftest_copts,
    deps = [
        ":decode_rpc_server",
        "//rtp_llm/cpp/cache:batch_kv_cache_resource",
        "//rtp_llm/cpp/disaggregate/p2p_connector:client_connector",
    ],
)

cc_binary(
    name = "p2p_connector_perftest_client",
    srcs = ["P2PConnectorPerfTestClientMain.cc"],
    copts = perftest_copts,
    deps = [
        ":perftest_client",
        "//rtp_llm/cpp/metrics:metrics",
        "@havenask//aios/autil:log",
        "@havenask//aios/autil:net",
    ] + device_impl_target(),
)
