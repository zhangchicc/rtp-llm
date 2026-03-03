#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheSender.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/proto/service.pb.h"

#include "aios/network/arpc/arpc/ANetRPCController.h"

#include <functional>
#include <memory>
#include <string>

namespace rtp_llm::transfer::tcp {

/// One-shot Closure that owns the RPC response buffer and fires the user callback on completion.
/// Heap-allocated; deletes itself inside Run().
class SendClosure final: public ::google::protobuf::Closure {
public:
    SendClosure(const SendRequestPtr&                                              send_request,
                const std::shared_ptr<::transfer::tcp::TransferRequestPB>&         transfer_request,
                std::function<void(IKVCacheSender::ErrorCode, const std::string&)> callback);
    ~SendClosure() override = default;

    arpc::ANetRPCController*             getController() const;
    ::transfer::tcp::TransferResponsePB* getResponse() const;

    void Run() override;

private:
    SendRequestPtr                                       send_request_;
    std::shared_ptr<::transfer::tcp::TransferRequestPB>  transfer_request_;  // keep alive until RPC completes
    std::unique_ptr<::transfer::tcp::TransferResponsePB> transfer_response_;
    std::unique_ptr<arpc::ANetRPCController>             controller_;
    std::function<void(IKVCacheSender::ErrorCode, const std::string&)> callback_;
};

}  // namespace rtp_llm::transfer::tcp
