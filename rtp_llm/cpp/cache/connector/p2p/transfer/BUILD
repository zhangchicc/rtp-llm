package(default_visibility = ["//visibility:public"])

load("//:def.bzl", "copts")

cc_library(
    name = "rdma_header",
    hdrs = ["RdmaInterface.h"],
    srcs = [],
    deps = [
        "//rtp_llm/cpp/core:buffer",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/proto:service_cc_proto",
    ],
    visibility = ['//visibility:public']
)

cc_library(
    name = "no_rdma_impl",
    srcs = ["RdmaInterface.cc"],
    hdrs = [],
    deps = [
        ":rdma_header",
        "//rtp_llm/cpp/utils:core_utils",
    ],
)

# Placeholder for RDMA impl selection. In open-source builds we link to the no-op
# implementation; internal builds can override this target.
cc_library(
    name = "transfer_rdma_impl",
    deps = [":no_rdma_impl"],
)

cc_library(
    name = "basic_types",
    hdrs = [
        "LayerCacheBuffer.h",
        "LayerBlockConvertor.h",
        "LayerCacheBufferUtil.h",
        "TransferTask.h",
    ],
    srcs = [
        "LayerCacheBuffer.cc",
        "LayerCacheBufferUtil.cc",
        "TransferTask.cc",
    ],
    deps = [
        "//rtp_llm/cpp/core:buffer",
        "//rtp_llm/cpp/utils:core_utils",
        "//rtp_llm/cpp/utils:error_code",
        "//rtp_llm/cpp/utils:time_util",
        "//rtp_llm/cpp/cache:batch_kv_cache_resource",
        "@havenask//aios/autil:thread",
    ],
    copts = copts(),
    visibility=['//visibility:public'],
)

cc_library(
    name = "transfer",
    hdrs = ["TransferClient.h", "TransferServer.h", "TransferServerService.h", "TransferTaskContext.h", "TransferMetric.h"],
    srcs = ["TransferClient.cc", "TransferServer.cc", "TransferServerService.cc", "TransferTaskContext.cc", "TransferMetric.cc"],
    deps = [
        ":basic_types",
        ":rdma_header",
        ":transfer_rdma_impl",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:arpc_dep",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/utils:cuda_copy_util",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/proto:service_cc_proto",
        "//rtp_llm/cpp/utils:core_utils",
        "//rtp_llm/cpp/model_rpc:rpc_error_code",
        "//rtp_llm/cpp/cache:cache_core",
        "//rtp_llm/cpp/devices:devices_base",
        "@havenask//aios/autil:net",
        "//rtp_llm/cpp/metrics:metrics",
    ],
    copts = copts(),
    visibility=['//visibility:public'],
)

# Convenience aliases for perftest BUILD files.
cc_library(
    name = "transfer_client",
    deps = [":transfer"],
)

cc_library(
    name = "transfer_server",
    deps = [":transfer"],
)

cc_library(
    name = "interface",
    hdrs = [
        "Types.h",
        "IKVCacheSender.h",
        "IKVCacheReceiver.h",
        "IKVTransferFactory.h",
    ],
    deps = [
        "//rtp_llm/cpp/cache:block_info",
    ],
)