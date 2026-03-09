#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace rtp_llm {

/// Describes the remote end of an RDMA buffer (addr + per-NIC rkey map).
struct RemoteBuffer {
    int64_t                                       addr;
    uint32_t                                      len;
    std::shared_ptr<std::map<uint32_t, uint32_t>> nic_rkeys;

    RemoteBuffer(int64_t addr, uint32_t len, const std::shared_ptr<std::map<uint32_t, uint32_t>>& nic_rkeys):
        addr(addr), len(len), nic_rkeys(nic_rkeys) {}
};

/// Describes the local end of an RDMA read target (raw pointer, size, GPU flag).
struct LocalBuffer {
    void*    addr   = nullptr;
    uint64_t size   = 0;
    bool     is_gpu = false;
};

/// RDMA memory manager interface.
/// All methods operate on raw pointers to avoid Buffer abstraction dependency.
class IRdmaMemoryManager {
public:
    virtual ~IRdmaMemoryManager() = default;

    /// Register a memory region as an RDMA MR.
    /// @param addr        Base address of the memory block.
    /// @param size        Size in bytes.
    /// @param is_gpu      True for GPU memory, false for host memory.
    /// @param aligned_size Optional alignment hint (0 = use implementation default).
    virtual bool regUserMr(void* addr, uint64_t size, bool is_gpu, uint64_t aligned_size = 0) = 0;

    /// Deregister a previously registered RDMA MR.
    virtual bool deregUserMr(void* addr, bool is_gpu) = 0;

    /// Look up the remote buffer descriptor for a registered memory region.
    /// Returns nullptr if the address has not been registered.
    virtual std::shared_ptr<RemoteBuffer> findMemoryMr(void* addr, uint64_t size, bool is_gpu) = 0;
};

/// RDMA connection interface (single bidirectional RDMA channel).
class IRdmaConnection {
public:
    virtual ~IRdmaConnection() = default;

    /// Asynchronously RDMA-read each remote buffer into the corresponding local buffer.
    ///
    /// Contract:
    /// 1) callback is invoked exactly once on completion (success or failure).
    /// 2) callback may run on an RDMA worker thread.
    /// 3) deadline_ms is an absolute unix timestamp in milliseconds; the implementation
    ///    should abort and callback(false) if the deadline is exceeded.
    virtual void read(const std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>& pairs,
                      std::function<void(bool)>                                                 callback,
                      uint64_t                                                                  deadline_ms) = 0;
};

/// RDMA client interface – manages outgoing connections (one pool per remote host:port).
class IRdmaClient {
public:
    virtual ~IRdmaClient() = default;

    /// Return a (possibly reused) connection to ip:port.
    /// Returns nullptr on failure.
    virtual std::shared_ptr<IRdmaConnection> getConnection(const std::string& ip, uint32_t port) = 0;
};

/// RDMA server interface – listens for incoming RDMA connections from remote clients.
class IRdmaServer {
public:
    virtual ~IRdmaServer() = default;

    virtual uint32_t getListenPort() const = 0;
};

std::shared_ptr<IRdmaMemoryManager> createRdmaMemoryManager();

std::shared_ptr<IRdmaClient> createRdmaClient(const std::shared_ptr<IRdmaMemoryManager>& memory_manager,
                                              int                                        io_thread_count,
                                              int                                        worker_thread_count       = 0,
                                              uint32_t                                   rdma_connections_per_host = 8,
                                              int                                        connect_timeout_ms = 250);

std::shared_ptr<IRdmaServer> createRdmaServer(const std::shared_ptr<IRdmaMemoryManager>& memory_manager,
                                              uint32_t                                   listen_port,
                                              int                                        io_thread_count,
                                              int                                        worker_thread_count = 0);

}  // namespace rtp_llm
