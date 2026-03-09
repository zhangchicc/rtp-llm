#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/common/KVCacheTaskStore.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheRecvTask.h"

namespace rtp_llm::transfer::tcp {

using TcpKVCacheTaskStore = ::rtp_llm::transfer::KVCacheTaskStore<TcpKVCacheRecvTask>;

}  // namespace rtp_llm::transfer::tcp
