#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaMemoryManager.h"

#include "aios/network/accl-barex/src/barex/impl/xsimple_mempool_impl.h"
#include "aios/network/accl-barex/src/barex/env.h"
#include "autil/EnvUtil.h"

#include "rtp_llm/cpp/utils/Logger.h"

#include <algorithm>

namespace rtp_llm {
namespace transfer {

RdmaMemoryManager::RdmaMemoryManager() {
    arpc::RdmaMempoolFactory::Init();
    rdma_mempool_ = arpc::RdmaMempoolFactory::GetMempool();
    if (!rdma_mempool_) {
        RTP_LLM_LOG_ERROR("rdma memory manager init failed, get rdma mempool failed");
    }

#ifdef ACCL_USE_EIC  // eic current only support less than 64MB
    max_reg_mem_size_ = 64L * 1024 * 1024;
#endif
    max_reg_mem_size_ = autil::EnvUtil::getEnv("CACHE_STORE_MAX_REG_MEM_SIZE", max_reg_mem_size_);
    min_reg_mem_size_ = autil::EnvUtil::getEnv("CACHE_STORE_MIN_REG_MEM_SIZE", min_reg_mem_size_);
    RTP_LLM_LOG_INFO(
        "rdma memory manager max reg mem size is %lu, min reg mem size is %lu", max_reg_mem_size_, min_reg_mem_size_);
}

RdmaMemoryManager::~RdmaMemoryManager() {
    if (!rdma_mempool_) {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(mr_mutex_);
    size_t                              mr_count = mr_info_list_.size();
    for (auto& mr_info : mr_info_list_) {
        bool is_gpu = (mr_info.mem_info_.d_type == ::accl::barex::GPU);
        rdma_mempool_->DeregUserMr(mr_info.begin_, is_gpu ? arpc::RdmaMempool::GPU : arpc::RdmaMempool::CPU);
        RTP_LLM_LOG_INFO("auto dereg user mr in dtor, base: %p, buf: %p", mr_info.base_, mr_info.begin_);
    }
    mr_info_list_.clear();
    if (mr_count > 0) {
        RTP_LLM_LOG_INFO("rdma memory manager dtor, deregistered %lu MRs", mr_count);
    }

    arpc::RdmaMempoolFactory::Shutdown();
}

// IRdmaMemoryManager implementation – raw pointer interface.

bool RdmaMemoryManager::regUserMr(void* addr, uint64_t size, bool is_gpu, uint64_t aligned_size) {
    if (!addr) {
        RTP_LLM_LOG_WARNING("reg user mr failed, addr is nullptr");
        return false;
    }
    RTP_LLM_LOG_INFO("reg user mr, addr: %p, size: %lu, aligned_size: %lu", addr, size, aligned_size);
    bool ret = regUserMrInternal(addr, size, is_gpu, aligned_size);
    if (!ret) {
        RTP_LLM_LOG_ERROR("reg user mr failed, addr: %p, size: %lu", addr, size);
    }
    return ret;
}

bool RdmaMemoryManager::deregUserMr(void* addr, bool is_gpu) {
    if (!addr) {
        RTP_LLM_LOG_WARNING("dereg user mr failed, addr is nullptr");
        return false;
    }
    return deregUserMrInternal(addr, is_gpu);
}

std::shared_ptr<RemoteBuffer> RdmaMemoryManager::findMemoryMr(void* addr, uint64_t size, bool is_gpu) {
    if (!addr) {
        return nullptr;
    }
    std::shared_lock<std::shared_mutex> lock(mr_mutex_);
    const mr_info*                      mr = findMemoryMrInternal(addr, size, is_gpu);
    if (!mr) {
        return nullptr;
    }
    return std::make_shared<RemoteBuffer>(reinterpret_cast<int64_t>(addr), static_cast<uint32_t>(size), mr->nic_rkeys_);
}

// Lower-level lookup for RdmaConnection: fills memp_t in addition to returning success.
bool RdmaMemoryManager::findMemoryMr(::accl::barex::memp_t& mem_info, void* buf, uint64_t size, bool gpu) {
    const mr_info* mr = findMemoryMrInternal(buf, size, gpu);
    if (!mr) {
        return false;
    }
    mem_info         = mr->mem_info_;
    mem_info.buf     = static_cast<char*>(buf);
    mem_info.buf_len = size;
    return true;
}

// Private helpers (unchanged from original implementation).

bool RdmaMemoryManager::regUserMrInternal(void* buf, uint64_t size, bool gpu, uint64_t aligned_size) {
    if (!rdma_mempool_) {
        RTP_LLM_LOG_WARNING("reg user mr failed, rdma mempool is nullptr");
        return false;
    }
    if (aligned_size == 0) {
        aligned_size = min_reg_mem_size_;
    } else if (size % aligned_size != 0) {
        RTP_LLM_LOG_WARNING("reg user mr failed, size %lu not aligned to %lu", size, aligned_size);
        return false;
    } else if (size < aligned_size || max_reg_mem_size_ < aligned_size) {
        RTP_LLM_LOG_WARNING(
            "reg user mr failed, size %lu, max reg size %lu, align size %lu", size, max_reg_mem_size_, aligned_size);
        return false;
    }

    auto get_align_size = [](uint64_t s, uint64_t a) { return s % a == 0 ? s : s - s % a; };

    uint64_t max_reg_mem_size = get_align_size(max_reg_mem_size_, aligned_size);
    if (max_reg_mem_size == 0) {
        RTP_LLM_LOG_WARNING(
            "reg user mr failed, max reg mem size is %lu after align %lu", max_reg_mem_size, aligned_size);
        return false;
    }

    void*                reg_buf     = buf;
    uint64_t             to_reg_size = size;
    std::vector<mr_info> mr_infos;
    while (to_reg_size > 0) {
        uint64_t            reg_size = std::min(to_reg_size, max_reg_mem_size);
        accl::barex::memp_t mr_mem;
        bool                success = false;
        do {
            reg_size = get_align_size(reg_size, aligned_size);
            if (reg_size < min_reg_mem_size_) {
                RTP_LLM_LOG_WARNING("reg user mr failed, after align reg size %lu is too small, align size %lu",
                                    reg_size,
                                    aligned_size);
                break;
            }
            if (rdma_mempool_->RegUserMr(
                    mr_mem, reg_buf, reg_size, gpu ? arpc::RdmaMempool::GPU : arpc::RdmaMempool::CPU)) {
                success = true;
                break;
            }
            RTP_LLM_LOG_INFO("reg user mr backward, buf: %p, size: %lu, dec to %lu", reg_buf, reg_size, reg_size / 2);
            reg_size = reg_size / 2;
        } while (reg_size >= min_reg_mem_size_);

        if (!success) {
            RTP_LLM_LOG_WARNING("reg user mr failed, buf: %p. size: %lu not work", reg_buf, reg_size);
            break;
        }
        auto nic_rkeys = std::make_shared<std::map<uint32_t, uint32_t>>();
        for (auto& [nicid, ibv_mr] : mr_mem.mrs) {
            nic_rkeys->insert({nicid, ibv_mr->rkey});
        }
        mr_infos.push_back(mr_info(buf, reg_buf, (int8_t*)reg_buf + reg_size, mr_mem, nic_rkeys));
        reg_buf = (void*)((uint64_t)reg_buf + reg_size);
        to_reg_size -= reg_size;
        RTP_LLM_LOG_INFO(
            "reg user mr success, base: %p, buf: %p, size: %lu, align size %lu", buf, reg_buf, reg_size, aligned_size);
    }

    if (to_reg_size > 0) {
        for (auto& mr : mr_infos) {
            rdma_mempool_->DeregUserMr(mr.begin_, gpu ? arpc::RdmaMempool::GPU : arpc::RdmaMempool::CPU);
        }
        return false;
    }

    RTP_LLM_LOG_INFO("reg user mr success, base: %p, size: %lu, mr count %lu, align size %lu",
                     buf,
                     size,
                     mr_infos.size(),
                     aligned_size);

    std::unique_lock<std::shared_mutex> lock(mr_mutex_);
    mr_info_list_.insert(mr_info_list_.end(), mr_infos.begin(), mr_infos.end());
    std::sort(mr_info_list_.begin(), mr_info_list_.end(), [](const mr_info& a, const mr_info& b) {
        return a.begin_ < b.begin_;
    });
    return true;
}

bool RdmaMemoryManager::deregUserMrInternal(void* buf, bool gpu) {
    if (!rdma_mempool_) {
        RTP_LLM_LOG_WARNING("dereg user mr failed, rdma mempool is nullptr");
        return false;
    }
    std::unique_lock<std::shared_mutex> lock(mr_mutex_);
    for (auto iter = mr_info_list_.begin(); iter != mr_info_list_.end();) {
        if (buf == iter->base_) {
            rdma_mempool_->DeregUserMr(iter->begin_, gpu ? arpc::RdmaMempool::GPU : arpc::RdmaMempool::CPU);
            RTP_LLM_LOG_INFO("dereg user mr success, base: %p, buf: %p", buf, iter->begin_);
            iter = mr_info_list_.erase(iter);
        } else {
            ++iter;
        }
    }
    return true;
}

const RdmaMemoryManager::mr_info* RdmaMemoryManager::findMemoryMrInternal(void* buf, uint64_t size, bool gpu) {
    ::accl::barex::device_type          d_type = gpu ? ::accl::barex::GPU : ::accl::barex::CPU;
    std::shared_lock<std::shared_mutex> lock(mr_mutex_);

    auto it = std::upper_bound(
        mr_info_list_.begin(), mr_info_list_.end(), buf, [](void* b, const mr_info& info) { return b < info.begin_; });

    if (it == mr_info_list_.begin()) {
        RTP_LLM_LOG_WARNING("find memory mr from mr_info_list failed, buf: %p, gpu: %d", buf, gpu);
        return nullptr;
    }
    --it;
    if (buf < it->begin_ || ((int64_t)buf + (int64_t)size) > (int64_t)(it->end_) || it->mem_info_.d_type != d_type) {
        RTP_LLM_LOG_WARNING("find memory mr from mr_info_list failed, buf: %p, size: %lu, begin: %p, end: %p, gpu: %d",
                            buf,
                            size,
                            it->begin_,
                            it->end_,
                            gpu);
        return nullptr;
    }
    return &(*it);
}

}  // namespace transfer
}  // namespace rtp_llm
