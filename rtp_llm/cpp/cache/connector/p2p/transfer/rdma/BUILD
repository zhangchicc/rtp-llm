package(default_visibility = ["//visibility:public"])

load("//:def.bzl", "copts")

# Core RDMA infrastructure (memory manager, connection pool, client/server).
cc_library(
    name = "rdma_impl",
    srcs = [
        "RdmaInterface.cc",
        "RdmaMemoryManager.cc",
        "RdmaConnection.cc",
        "RdmaHostConnectionPool.cc",
        "RdmaClient.cc",
        "RdmaServer.cc",
        "RdmaMetric.cc",
    ],
    hdrs = [
        "RdmaMemoryManager.h",
        "RdmaConnection.h",
        "RdmaHostConnectionPool.h",
        "RdmaClient.h",
        "RdmaServer.h",
        "RdmaMetric.h",
    ],
    deps = [
        "//rtp_llm/cpp/cache/connector/p2p/transfer:rdma_header",
        "@alibaba_rdma//aios/network/rdma:arpc_rdma",
        "@alibaba_rdma//aios/network/rdma:arpc_kmonitor_metric_reporter",
        "//rtp_llm/cpp/utils:core_utils",
        "//rtp_llm/cpp/utils:time_util",
        "//rtp_llm/cpp/metrics:metrics",
    ],
    alwayslink = True,
)

# RDMA KV cache transfer (sender + receiver built on top of rdma_impl).
cc_library(
    name = "rdma_kvcache",
    hdrs = [
        "RdmaKVCacheSender.h",
        "RdmaKVCacheReceiver.h",
        "RdmaKVCacheReceiverService.h",
        "RdmaKVCacheRecvTask.h",
        "RdmaKVCacheTaskStore.h",
        "RdmaSendClosure.h",
    ],
    srcs = [
        "RdmaKVCacheSender.cc",
        "RdmaKVCacheReceiver.cc",
        "RdmaKVCacheReceiverService.cc",
        "RdmaKVCacheRecvTask.cc",
        "RdmaSendClosure.cc",
    ],
    deps = [
        ":rdma_impl",
        "//rtp_llm/cpp/cache/connector/p2p/transfer:interface",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/common:kvcache_recv_task_base",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/common:kvcache_task_store",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:arpc_dep",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:metrics",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/rdma/proto:service_cc_proto",
        "//rtp_llm/cpp/utils:core_utils",
        "//rtp_llm/cpp/utils:time_util",
    ],
    copts = copts(),
)
