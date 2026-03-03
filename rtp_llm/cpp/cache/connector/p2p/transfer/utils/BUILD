package(default_visibility = ["//visibility:public"])

load("//:def.bzl", "copts")

cc_library(
    name = "arpc_dep",
    srcs = [
        "TcpClient.cc",
        "TcpServer.cc",
    ],
    hdrs = [
        "TcpClient.h",
        "TcpServer.h",
    ],
    deps = [
        "//rtp_llm/cpp/utils:core_utils",
        "@havenask//aios/network/arpc:arpc",
        "@havenask//aios/network/arpc/arpc/metric:kmonitor_anet_metric",
        "@havenask//aios/autil:net",
    ],
    copts = copts(),
)

cc_library(
    name = "cuda_copy_util",
    srcs = ["CudaCopyUtil.cc"],
    hdrs = ["CudaCopyUtil.h"],
    deps = [
        "//rtp_llm/cpp/core:buffer",
        "//rtp_llm/cpp/devices:devices_base",
        "//rtp_llm/cpp/utils:core_utils",
    ],
    copts = copts(),
)

cc_library(
    name = "metrics",
    srcs = ["Metrics.cc"],
    hdrs = ["Metrics.h"],
    deps = [
        "//rtp_llm/cpp/metrics:metrics",
        "@havenask//aios/kmonitor:kmonitor_client_cpp",
    ],
    copts = copts(),
)
