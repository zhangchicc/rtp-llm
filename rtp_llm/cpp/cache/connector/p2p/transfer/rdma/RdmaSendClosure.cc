#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaSendClosure.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

#include <algorithm>

namespace rtp_llm::transfer::rdma {

RdmaSendClosure::RdmaSendClosure(
    const ::rtp_llm::transfer::SendRequestPtr&                                              send_request,
    const std::shared_ptr<::transfer::rdma::RdmaTransferRequestPB>&                         transfer_request,
    std::function<void(::rtp_llm::transfer::IKVCacheSender::ErrorCode, const std::string&)> callback):
    send_request_(send_request), transfer_request_(transfer_request), callback_(std::move(callback)) {
    transfer_response_ = std::make_unique<::transfer::rdma::RdmaTransferResponsePB>();
    controller_        = std::make_unique<arpc::ANetRPCController>();

    const auto timeout_ms = std::max<int64_t>(0, send_request_->deadline_ms - currentTimeMs());
    controller_->SetExpireTime(timeout_ms);
}

arpc::ANetRPCController* RdmaSendClosure::getController() const {
    return controller_.get();
}

::transfer::rdma::RdmaTransferResponsePB* RdmaSendClosure::getResponse() const {
    return transfer_response_.get();
}

void RdmaSendClosure::Run() {
    using ErrorCode = ::rtp_llm::transfer::IKVCacheSender::ErrorCode;
    ErrorCode   ec  = ErrorCode::NONE_ERROR;
    std::string msg;

    if (controller_->Failed()) {
        ec  = ErrorCode::FAILED;
        msg = controller_->ErrorText();
    } else {
        const auto resp_code =
            transfer_response_->has_error_code() ? transfer_response_->error_code() : ::transfer::rdma::BUFFER_MISMATCH;
        if (resp_code != ::transfer::rdma::ERROR_NONE) {
            ec  = ErrorCode::RESPONSE_FAILED;
            msg = transfer_response_->has_error_message() ?
                      transfer_response_->error_message() :
                      ("rdma signal response failed, code: " + std::to_string(resp_code));
        }
    }

    if (ec != ErrorCode::NONE_ERROR) {
        RTP_LLM_LOG_WARNING("RdmaKVCacheSender transfer failed, unique_key: %s, ip: %s, port: %u, "
                            "error_code: %d, error_message: %s",
                            send_request_->unique_key.c_str(),
                            send_request_->ip.c_str(),
                            send_request_->port,
                            static_cast<int>(ec),
                            msg.c_str());
    }

    callback_(ec, msg);
    delete this;
}

}  // namespace rtp_llm::transfer::rdma
