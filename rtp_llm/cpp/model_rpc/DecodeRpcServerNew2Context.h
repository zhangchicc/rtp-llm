#pragma once

#include "rtp_llm/cpp/model_rpc/GenerateContext.h"

namespace rtp_llm {

class DecodeRpcServerNew2Context: public GenerateContext {
public:
    DecodeRpcServerNew2Context(grpc::ServerContext*                   server_context,
                               const GenerateInputPB*                 request,
                               grpc::ServerWriter<GenerateOutputsPB>* response_writer,
                               kmonitor::MetricsReporterPtr&          metrics_reporter,
                               std::shared_ptr<RpcServerRuntimeMeta>  meta);
    virtual ~DecodeRpcServerNew2Context();

public:
    void init(const std::shared_ptr<EngineBase>& engine, const std::shared_ptr<MultimodalProcessor>& mm_processor);

public:
    const GenerateInputPB*                 request;
    grpc::ServerWriter<GenerateOutputsPB>* response_writer;

    std::unique_ptr<AtomicGuard>             request_guard;  // for request lifetime control
    std::unique_ptr<lora::LoraResourceGuard> lora_guard;

    int64_t     request_deadline_us = 0;
    std::string unique_key;
    RoleAddr    prefill_role_addr;

    // for metrics
    int64_t prepare_generate_context_done_time_us = 0;
    int64_t call_prefill_done_time_us             = 0;
    int64_t load_cache_from_prefill_done_time_us  = 0;
    int64_t local_generate_done_time_us           = 0;
};

}  // namespace rtp_llm