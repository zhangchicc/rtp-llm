#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/common/KVCacheTaskStore.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaKVCacheRecvTask.h"

namespace rtp_llm::transfer::rdma {

using RdmaKVCacheTaskStore = ::rtp_llm::transfer::KVCacheTaskStore<RdmaKVCacheRecvTask>;

}  // namespace rtp_llm::transfer::rdma
