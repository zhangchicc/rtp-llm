package(default_visibility = ["//visibility:public"])

load("//bazel:tf_proto.bzl", "cc_proto")

cc_proto(
    name = "service",
    srcs = [
        "service.proto",
    ],
    deps = [
        "@havenask//aios/network/arpc:proto",
    ],
    visibility = ["//visibility:public"],
)