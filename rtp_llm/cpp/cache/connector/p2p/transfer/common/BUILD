package(default_visibility = ["//visibility:public"])

load("//:def.bzl", "copts")

cc_library(
    name = "kvcache_recv_task_base",
    hdrs = ["KVCacheRecvTaskBase.h"],
    srcs = ["KVCacheRecvTaskBase.cc"],
    deps = [
        "//rtp_llm/cpp/cache/connector/p2p/transfer:interface",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:metrics",
        "//rtp_llm/cpp/utils:core_utils",
        "//rtp_llm/cpp/utils:time_util",
    ],
    copts = copts(),
)

cc_library(
    name = "kvcache_task_store",
    hdrs = ["KVCacheTaskStore.h"],
    deps = [
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:metrics",
        "//rtp_llm/cpp/utils:core_utils",
    ],
    copts = copts(),
)
