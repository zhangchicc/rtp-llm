#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/RdmaInterface.h"
#include "aios/network/rdma/accl/RdmaMempool.h"
#include "aios/network/rdma/accl/RdmaMempoolFactory.h"
#include "aios/network/accl-barex/src/barex/impl/xsimple_mempool_impl.h"
#include <memory>
#include <shared_mutex>
#include <vector>

namespace rtp_llm {
namespace transfer {

/// RDMA Memory Region (MR) manager.
/// Registers, deregisters and looks up RDMA MRs keyed by raw memory addresses.
class RdmaMemoryManager: public IRdmaMemoryManager {
public:
    RdmaMemoryManager();
    ~RdmaMemoryManager();

public:
    // IRdmaMemoryManager – raw pointer interface (no Buffer dependency).
    bool                          regUserMr(void* addr, uint64_t size, bool is_gpu, uint64_t aligned_size = 0) override;
    bool                          deregUserMr(void* addr, bool is_gpu) override;
    std::shared_ptr<RemoteBuffer> findMemoryMr(void* addr, uint64_t size, bool is_gpu) override;

    /// Lower-level lookup used by RdmaConnection to obtain the memp_t needed
    /// to build RDMA read descriptors.
    bool findMemoryMr(::accl::barex::memp_t& mem_info, void* buf, uint64_t size, bool gpu);

    std::shared_ptr<::accl::barex::XSimpleMempool> getXMempool() {
        if (!rdma_mempool_) {
            return nullptr;
        }
        return rdma_mempool_->GetXMempool();
    }

private:
    struct mr_info {
        void*                                         base_{nullptr};
        void*                                         begin_{nullptr};
        void*                                         end_{nullptr};
        accl::barex::memp_t                           mem_info_;
        std::shared_ptr<std::map<uint32_t, uint32_t>> nic_rkeys_;
        mr_info(void*                                                base,
                void*                                                begin,
                void*                                                end,
                accl::barex::memp_t                                  mem_info,
                const std::shared_ptr<std::map<uint32_t, uint32_t>>& nic_rkeys):
            base_(base), begin_(begin), end_(end), mem_info_(mem_info), nic_rkeys_(nic_rkeys) {}
    };

    const mr_info* findMemoryMrInternal(void* buf, uint64_t size, bool gpu);
    bool           regUserMrInternal(void* buf, uint64_t size, bool gpu, uint64_t aligned_size = 0);
    bool           deregUserMrInternal(void* buf, bool gpu);

    void setMaxRegMemSize(uint64_t max_size) {
        max_reg_mem_size_ = max_size;
    }
    void setMinRegMemSize(uint64_t min_size) {
        min_reg_mem_size_ = min_size;
    }
    uint64_t getMaxRegMemSize() const {
        return max_reg_mem_size_;
    }
    uint64_t getMinRegMemSize() const {
        return min_reg_mem_size_;
    }

private:
    std::shared_ptr<arpc::RdmaMempool> rdma_mempool_;
    std::shared_mutex                  mr_mutex_;
    std::vector<mr_info>               mr_info_list_;
    uint64_t                           max_reg_mem_size_ = std::numeric_limits<uint64_t>::max();
    uint64_t                           min_reg_mem_size_ = 1L * 1024;  // 1 KB
};

}  // namespace transfer
}  // namespace rtp_llm
