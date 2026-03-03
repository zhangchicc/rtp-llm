#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/SendClosure.h"

#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

#include <algorithm>

namespace rtp_llm::transfer::tcp {

SendClosure::SendClosure(const SendRequestPtr&                                              send_request,
                         const std::shared_ptr<::transfer::tcp::TransferRequestPB>&         transfer_request,
                         std::function<void(IKVCacheSender::ErrorCode, const std::string&)> callback):
    send_request_(send_request), transfer_request_(transfer_request), callback_(std::move(callback)) {
    transfer_response_ = std::make_unique<::transfer::tcp::TransferResponsePB>();
    controller_        = std::make_unique<arpc::ANetRPCController>();

    // Caller has already validated deadline; clamp to avoid negative values due to scheduling.
    const auto timeout_ms = std::max<int64_t>(0, send_request_->deadline_ms - currentTimeMs());
    controller_->SetExpireTime(timeout_ms);
}

arpc::ANetRPCController* SendClosure::getController() const {
    return controller_.get();
}

::transfer::tcp::TransferResponsePB* SendClosure::getResponse() const {
    return transfer_response_.get();
}

void SendClosure::Run() {
    IKVCacheSender::ErrorCode ec = IKVCacheSender::ErrorCode::NONE_ERROR;
    std::string               msg;

    if (controller_->Failed()) {
        ec  = IKVCacheSender::ErrorCode::FAILED;
        msg = controller_->ErrorText();
    } else {
        const auto resp_code =
            transfer_response_->has_error_code() ? transfer_response_->error_code() : ::transfer::tcp::BUFFER_MISMATCH;
        if (resp_code != ::transfer::tcp::ERROR_NONE) {
            ec  = IKVCacheSender::ErrorCode::RESPONSE_FAILED;
            msg = transfer_response_->has_error_message() ?
                      transfer_response_->error_message() :
                      ("response failed, resp_code: " + std::to_string(resp_code));
        }
    }

    if (ec != IKVCacheSender::ErrorCode::NONE_ERROR) {
        RTP_LLM_LOG_WARNING("TcpKVCacheSender transfer failed, unique_key: %s, ip: %s, port: %u, "
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

}  // namespace rtp_llm::transfer::tcp
