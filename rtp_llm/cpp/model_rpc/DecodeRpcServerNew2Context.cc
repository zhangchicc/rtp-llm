#include "rtp_llm/cpp/model_rpc/DecodeRpcServerNew2Context.h"

namespace rtp_llm {

DecodeRpcServerNew2Context::DecodeRpcServerNew2Context(grpc::ServerContext*                   server_context,
                                                       const GenerateInputPB*                 request,
                                                       grpc::ServerWriter<GenerateOutputsPB>* response_writer,
                                                       kmonitor::MetricsReporterPtr&          metrics_reporter,
                                                       std::shared_ptr<RpcServerRuntimeMeta>  meta):
    GenerateContext(
        request->request_id(), request->generate_config().timeout_ms(), server_context, metrics_reporter, meta),
    request(request),
    response_writer(response_writer) {
    request_guard = std::make_unique<AtomicGuard>(request_guard);
    RTP_LLM_LOG_DEBUG("receive request %ld", request_id);
}

DecodeRpcServerNew2Context::~DecodeRpcServerNew2Context() {
    // report metrics
    DecodeRpcServerNew2MetricsCollector collector;
    collector.prepare_generate_context_rt_us = prepare_generate_context_done_time_us - request_begin_time_us;
    collector.load_cache_from_prefill_rt_us =
        load_cache_from_prefill_done_time_us - prepare_generate_context_done_time_us;
    collector.call_prefill_rt_us   = call_prefill_done_time_us - prepare_generate_context_done_time_us;
    collector.local_generate_rt_us = local_generate_done_time_us - call_prefill_done_time_us;

    DecodeRpcServerNew2Metrics::report(tags, &collector);
}

void DecodeRpcServerNew2Context::init(const std::shared_ptr<EngineBase>&          engine,
                                      const std::shared_ptr<MultimodalProcessor>& mm_processor) {
    RTP_LLM_LOG_DEBUG("request [%s] start to prepare generate context", request_key.c_str());

    auto input = QueryConverter::transQuery(request);

    // get request deadline
    request_deadline_us = currentTimeUs() + request->generate_config().timeout_ms() * 1000;

    // get unique key
    if (input->generate_config->inter_request_id() >= 0) {
        unique_key = std::to_string(input->generate_config->inter_request_id());
    } else {
        unique_key = autil::NetUtil::getBindIp() + "_" + std::to_string(request->request_id()) + "_"
                     + std::to_string(currentTimeUs());
    }

    // get prefill role addr
    auto role_addrs = QueryConverter::getRoleAddrs(&request->generate_config());
    for (auto& role_addr : role_addrs) {
        if (role_addr.role == RoleType::PREFILL) {
            prefill_role_addr = role_addr;
            break;
        }
    }
    if (prefill_role_addr.ip.empty() || prefill_role_addr.port == 0) {
        RTP_LLM_LOG_WARNING("request [%ld] get prefill role addr failed", request_id);
        error_info   = ErrorInfo(ErrorCode::GET_HOST_FAILED, "get prefill role addr failed");
        error_status = serializeErrorMsg(request_key, error_info);
        return;
    }

    // update multimodal features
    if (mm_processor != nullptr && input->multimodal_inputs) {
        auto mm_res = mm_processor->updateMultimodalFeatures(input);
        if (!mm_res.ok()) {
            RTP_LLM_LOG_WARNING("request [%ld] update multimodal features failed", request_id);
            error_info   = ErrorInfo(ErrorCode::UPDATE_MULTIMODAL_FEATURES_FAILED, mm_res.error_message());
            error_status = serializeErrorMsg(request_key, error_info);
            return;
        }
    }

    // get lora guard
    input->lora_id = engine->getLoraManager()->getLoraId(input->generate_config->adapter_name);
    lora_guard =
        std::make_unique<lora::LoraResourceGuard>(engine->getLoraManager(), input->generate_config->adapter_name);

    // init stream
    stream_ = engine->makeStream(input);
    if (!stream_) {
        RTP_LLM_LOG_WARNING("request [%ld] init stream failed", request_id);
        error_info   = ErrorInfo(ErrorCode::INIT_STREAM_FAILED, "init stream failed");
        error_status = serializeErrorMsg(request_key, error_info);
        return;
    }

    // init KV block
    auto status = stream_->initKVBlock(0);
    if (!status.ok()) {
        RTP_LLM_LOG_WARNING("request [%ld] init KV block failed", request_id);
        error_info   = ErrorInfo(ErrorCode::INIT_KV_BLOCK_FAILED, "init KV block failed");
        error_status = serializeErrorMsg(request_key, error_info);
        return;
    }

    if (stream_->kvCache().batchSize() > 1) {
        RTP_LLM_LOG_WARNING("request [%ld] batch size > 1, not supported", request_id);
        error_info   = ErrorInfo(ErrorCode::INVALID_PARAMS, "batch size > 1 not supported");
        error_status = serializeErrorMsg(request_key, error_info);
        return;
    }

    // set prepare generate context done time
    prepare_generate_context_done_time_us = currentTimeUs();
    RTP_LLM_LOG_DEBUG("request [%ld] enqueue success", request_id);
}

}  // namespace rtp_llm