load("//:def.bzl", "copts")
load("//bazel:arch_select.bzl", "torch_deps")

cc_library(
    name = "connector",
    hdrs = glob([
        "*.h",
    ]),
    srcs = glob([
        "*.cc",
    ]),
    deps = [
        "//rtp_llm/cpp/cache:cache_core",
        "//rtp_llm/cpp/cache:cache_types",
        "//rtp_llm/cpp/metrics:metrics",
        "//rtp_llm/cpp/model_rpc/proto:model_rpc_service_cc_proto",
        "@havenask//aios/autil:json",
        "@havenask//aios/autil:thread",
        "//rtp_llm/cpp/disaggregate/p2p_connector:connector",
        "//rtp_llm/cpp/disaggregate/p2p_connector:layer_block_convertor",
    ] + torch_deps(),
    copts = copts(),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "connector_header",
    hdrs = [
        "AsyncContext.h",
        "KVCacheConnector.h",
    ],
    copts = copts(),
    visibility = ["//visibility:public"],
)

