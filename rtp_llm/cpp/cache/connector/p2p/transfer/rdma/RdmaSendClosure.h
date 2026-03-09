#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheSender.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/proto/service.pb.h"

#include "aios/network/arpc/arpc/ANetRPCController.h"

#include <functional>
#include <memory>
#include <string>

namespace rtp_llm::transfer::rdma {

/// One-shot Closure that owns the RDMA signal RPC response buffer and fires the
/// user callback on completion.  Heap-allocated; deletes itself inside Run().
class RdmaSendClosure final: public ::google::protobuf::Closure {
public:
    RdmaSendClosure(const ::rtp_llm::transfer::SendRequestPtr&                      send_request,
                    const std::shared_ptr<::transfer::rdma::RdmaTransferRequestPB>& transfer_request,
                    std::function<void(::rtp_llm::transfer::IKVCacheSender::ErrorCode, const std::string&)> callback);
    ~RdmaSendClosure() override = default;

    arpc::ANetRPCController*                  getController() const;
    ::transfer::rdma::RdmaTransferResponsePB* getResponse() const;

    void Run() override;

private:
    ::rtp_llm::transfer::SendRequestPtr                                                     send_request_;
    std::shared_ptr<::transfer::rdma::RdmaTransferRequestPB>                                transfer_request_;
    std::unique_ptr<::transfer::rdma::RdmaTransferResponsePB>                               transfer_response_;
    std::unique_ptr<arpc::ANetRPCController>                                                controller_;
    std::function<void(::rtp_llm::transfer::IKVCacheSender::ErrorCode, const std::string&)> callback_;
};

}  // namespace rtp_llm::transfer::rdma
