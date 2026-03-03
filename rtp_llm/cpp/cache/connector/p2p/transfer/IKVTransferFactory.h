#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheReceiver.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheSender.h"

namespace rtp_llm {
namespace transfer {

enum class TransferType {
    TCP  = 0,
    RDMA = 1,
};

class IKVTransferFactory {
public:
    virtual ~IKVTransferFactory() = default;

    /// Create transport-specific sender instance.
    /// Returns nullptr when the transport is unsupported or initialization fails.
    virtual IKVCacheSenderPtr createSender(TransferType transfer_type, TransferConfig transfer_config) = 0;

    /// Create transport-specific receiver instance.
    /// Returns nullptr when the transport is unsupported or initialization fails.
    virtual IKVCacheReceiverPtr createReceiver(TransferType transfer_type, TransferConfig transfer_config) = 0;
};

}  // namespace transfer
}  // namespace rtp_llm
