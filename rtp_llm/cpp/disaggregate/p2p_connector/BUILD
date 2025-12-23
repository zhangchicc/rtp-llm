package(default_visibility = ["//rtp_llm/cpp/disaggregate/p2p_connector:__subpackages__"])

cc_library(
    name = "layer_block_convert",
    srcs = [],
    hdrs = [
        "LayerBlockConvertorImpl.h",
    ],
    deps = [
        "//rtp_llm/cpp/cache:cache",
        "//rtp_llm/cpp/disaggregate/transfer:basic_types",
    ]
)

cc_library(
    name = "p2p_connector_metrics",
    srcs = [
        "P2PConnectorMetrics.cc",
    ],
    hdrs = [
        "P2PConnectorMetrics.h",
    ],
    deps = [
        "//rtp_llm/cpp/metrics:metrics",
        "//rtp_llm/cpp/utils:time_util",
    ]
)

cc_library(
    name = "tp_broadcast",
    srcs = [
        "TPBroadcastClient.cc",
    ],
    hdrs = [
        "TPBroadcastClient.h",
    ],
    deps = [
        "//rtp_llm/cpp/model_rpc:tp_broadcast_manager",
        "//rtp_llm/cpp/disaggregate/transfer:basic_types",
    ]
)

cc_library(
    name = "server_caller",
    srcs = [
        "P2PConnectorServerCaller.cc",
    ],
    hdrs = [
        "P2PConnectorServerCaller.h",
    ],
    deps = [
        "//rtp_llm/cpp/config:gpt_init_params",
        "//rtp_llm/cpp/model_rpc:model_rpc_pool",
        "//rtp_llm/cpp/disaggregate/transfer:basic_types",
    ]
)

cc_library(
    name = "client_connector",
    srcs = [
        "P2PConnectorClient.cc",
        "P2PConnectorClientWorker.cc",
        "P2PConnectorClientScheduler.cc",
    ],
    hdrs = [
        "P2PConnectorClient.h",
        "P2PConnectorClientWorker.h",
        "P2PConnectorClientScheduler.h",
    ],
    deps = [
        ":layer_block_convert",
        ":tp_broadcast",
        ":server_caller",
        ":p2p_connector_metrics",
        "//rtp_llm/cpp/cache:connector_interface",
        "//rtp_llm/cpp/cache:cache",
        "//rtp_llm/cpp/config:gpt_init_params",
        "//rtp_llm/cpp/disaggregate/transfer:basic_types",
        "//rtp_llm/cpp/disaggregate/transfer:transfer_server",
        "//rtp_llm/cpp/model_rpc:model_rpc_pool",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "asymmetric_tp",
    srcs = [
        "AsymmetricTpUtil.cc",
    ],
    hdrs = [
        "AsymmetricTpUtil.h",
    ],
    deps = [
        "//rtp_llm/cpp/config:gpt_init_params",
    ],
)

cc_library(
    name = "prefill_worker_load_context",
    srcs = [
        "PrefillWorkerLoadContext.cc",
    ],
    hdrs = [
        "PrefillWorkerLoadContext.h",
    ],
    deps = [
        ":asymmetric_tp",
        "//rtp_llm/cpp/utils:core_utils",
        "//rtp_llm/cpp/utils:time_util",
    ]
)

cc_library(
    name = "computed_layer_cache_buffer",
    srcs = [
        "ComputedLayerCacheBuffer.cc",
    ],
    hdrs = [
        "ComputedLayerCacheBuffer.h",
    ],
    deps = [
        "//rtp_llm/cpp/disaggregate/transfer:basic_types",
    ],
)

cc_library(
    name = "stream_store",
    srcs = [
        "P2PConnectorStreamStore.cc",
    ],
    hdrs = [
        "P2PConnectorStreamStore.h",
    ],
    deps = [
        ":p2p_connector_metrics",
        "//rtp_llm/cpp/engine_base/stream:stream",
        "//rtp_llm/cpp/utils:time_util",
    ],
)

cc_library(
    name = "server_connector",
    srcs = [
        "P2PConnectorServerWorker.cc",
        "P2PConnectorServerScheduler.cc",
        "P2PConnectorServer.cc",
    ],
    hdrs = [
        "P2PConnectorServerWorker.h",
        "P2PConnectorServerScheduler.h",
        "P2PConnectorServer.h",
    ],
    deps = [
        ":asymmetric_tp",
        ":layer_block_convert",
        ":prefill_worker_load_context",
        ":stream_store",
        ":tp_broadcast",
        ":computed_layer_cache_buffer",
        "//rtp_llm/cpp/cache:connector_interface",
        "//rtp_llm/cpp/cache:cache",
        "//rtp_llm/cpp/config:gpt_init_params",
        "//rtp_llm/cpp/disaggregate/transfer:basic_types",
        "//rtp_llm/cpp/disaggregate/transfer:transfer_client",
        "//rtp_llm/cpp/engine_base/stream:stream",
        "//rtp_llm/cpp/model_rpc:model_rpc_pool",
        "//rtp_llm/cpp/utils:core_utils",
        "//rtp_llm/cpp/utils:time_util",
    ],
    visibility = ["//visibility:public"],
)