#include <memory>
#include "rtp_llm/cpp/model_rpc/RemoteRpcServiceImpl.h"
#include "rtp_llm/cpp/model_rpc/PrefillRpcServer.h"
#include "rtp_llm/cpp/model_rpc/DecodeRpcServer.h"
#include "rtp_llm/cpp/model_rpc/PrefillRpcServerNew2.h"
#include "rtp_llm/cpp/model_rpc/DecodeRpcServerNew2.h"

namespace rtp_llm {

grpc::Status RemoteRpcServiceImpl::init(const EngineInitParams&                                maga_init_params,
                                        py::object                                             mm_process_engine,
                                        std::unique_ptr<rtp_llm::ProposeModelEngineInitParams> propose_params) {
    decode_entrance_ = maga_init_params.gpt_init_parameter.decode_entrance_;
    RTP_LLM_LOG_INFO("remote rpc service init, decode_entrance is %d, role_type is %d",
                     decode_entrance_,
                     maga_init_params.gpt_init_parameter.role_type_);

    if (maga_init_params.gpt_init_parameter.role_type_ == RoleType::PREFILL) {
        prefill_server_new2_ = std::make_shared<PrefillRpcServerNew2>();
        local_server_        = prefill_server_new2_;
        RTP_LLM_LOG_INFO("remote rpc service init prefill server new2");
        return prefill_server_new2_->init(maga_init_params, mm_process_engine, std::move(propose_params));
    } else {
        decode_server_new2_ = std::make_shared<DecodeRpcServerNew2>();
        local_server_       = decode_server_new2_;
        RTP_LLM_LOG_INFO("remote rpc service init decode server new2");
        return decode_server_new2_->init(maga_init_params, mm_process_engine, std::move(propose_params));
    }
}

}  // namespace rtp_llm
