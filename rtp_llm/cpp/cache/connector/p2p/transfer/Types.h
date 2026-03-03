#pragma once

#include "rtp_llm/cpp/cache/BlockInfo.h"

#include <map>
#include <memory>
#include <vector>

namespace rtp_llm {
namespace transfer {

/// BlockInfo describes local process memory only (addr/size/device metadata).
/// It is not a cross-process wire address.
///
/// Contract:
/// 1) For each cache_key, block order in vector<BlockInfo> must be deterministic
///    and identical between sender and receiver for the same model layout.
/// 2) All BlockInfo.addr in requests must remain valid until transfer completion
///    callback/task completion (or cancellation).
using KeyBlockInfos    = std::map<int64_t, std::vector<BlockInfo>>;
using KeyBlockInfosPtr = std::shared_ptr<KeyBlockInfos>;

}  // namespace transfer
}  // namespace rtp_llm
