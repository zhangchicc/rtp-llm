package(default_visibility = ["//visibility:public"])

load("//:def.bzl", "copts")

cc_library(
    name = "tcp_kvcache",
    hdrs = [
        "TcpKVCacheSender.h",
        "TcpKVCacheReceiver.h",
        "TcpKVCacheReceiverService.h",
        "TcpKVCacheRecvTask.h",
        "TcpKVCacheTaskStore.h",
        "RecvContext.h",
        "SendClosure.h",
    ],
    srcs = [
        "TcpKVCacheSender.cc",
        "TcpKVCacheReceiver.cc",
        "TcpKVCacheReceiverService.cc",
        "TcpKVCacheRecvTask.cc",
        "RecvContext.cc",
        "SendClosure.cc",
    ],
    deps = [
        "//rtp_llm/cpp/cache/connector/p2p/transfer:interface",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/common:kvcache_recv_task_base",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/common:kvcache_task_store",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:arpc_dep",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:cuda_copy_util",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:metrics",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/tcp/proto:service_cc_proto",
        "//rtp_llm/cpp/utils:core_utils",
        "//rtp_llm/cpp/utils:time_util",
        "@havenask//aios/autil:thread",
    ],
    copts = copts(),
)
