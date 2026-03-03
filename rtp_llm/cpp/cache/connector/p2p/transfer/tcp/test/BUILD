load("//:def.bzl", "copts")

load("//rtp_llm/cpp/devices:device_defs.bzl", "device_impl_target", "device_test_envs")

test_copts = [
    "-fno-access-control",
] + copts()

cc_test(
    name = "tcp_kvcache_recv_task_test",
    srcs = [
        "TcpKVCacheRecvTaskTest.cc",
    ],
    copts = test_copts,
    deps = [
        "//rtp_llm/cpp/cache/connector/p2p/transfer/tcp:tcp_kvcache",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/test:gtest_main_lib",
        "//rtp_llm/cpp/utils:time_util",
        "@com_google_googletest//:gtest",
    ] + device_impl_target(),
    env = device_test_envs(),
)

cc_test(
    name = "tcp_kvcache_recv_context_test",
    srcs = [
        "RecvContextTest.cc",
    ],
    copts = test_copts,
    deps = [
        "//rtp_llm/cpp/cache/connector/p2p/transfer/tcp:tcp_kvcache",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/test:gtest_main_lib",
        "//rtp_llm/cpp/utils:time_util",
        "//rtp_llm/cpp/config:config_modules",
        "@com_google_googletest//:gtest",
    ] + device_impl_target(),
    env = device_test_envs(),
)

cc_test(
    name = "tcp_kvcache_tcp_test",
    srcs = [
        "TcpKVCacheTcpTest.cc",
    ],
    copts = test_copts,
    deps = [
        "//rtp_llm/cpp/cache/connector/p2p/transfer/tcp:tcp_kvcache",
        "//rtp_llm/cpp/cache/connector/p2p/transfer/test:gtest_main_lib",
        "//rtp_llm/cpp/utils:time_util",
        "@com_google_googletest//:gtest",
        "@havenask//aios/autil:net",
    ] + device_impl_target(),
    env = device_test_envs(),
)
