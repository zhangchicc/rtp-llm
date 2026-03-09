#include "rtp_llm/cpp/cache/connector/p2p/transfer/rdma/RdmaConnection.h"

#include "aios/network/accl-barex/include/accl/barex/xchannel.h"
#include "aios/network/accl-barex/src/barex/impl/xsimple_mempool_impl.h"
#include "rtp_llm/cpp/utils/Logger.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

#include <algorithm>

namespace rtp_llm {
namespace transfer {

RdmaConnection::RdmaConnection(const std::shared_ptr<RdmaMemoryManager>&         memory_manager,
                               const std::string&                                ip,
                               uint32_t                                          port,
                               const std::shared_ptr<kmonitor::MetricsReporter>& metrics_reporter):
    memory_manager_(memory_manager),
    ip_(ip),
    port_(port),
    metrics_reporter_(metrics_reporter),
    state_(RdmaConnectionState::CONNECTING) {}

RdmaConnection::~RdmaConnection() {
    auto pending = drainPendingRequests();
    for (auto& req : pending) {
        req.callback(false);
    }
}

RdmaConnectionState RdmaConnection::getState() const {
    return state_.load();
}

void RdmaConnection::onConnectSuccess(const std::shared_ptr<::accl::barex::XConnection>& connection) {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    if (state_.load() != RdmaConnectionState::CONNECTING) {
        RTP_LLM_LOG_WARNING(
            "rdma connection to %s:%u onConnectSuccess called but state is not CONNECTING", ip_.c_str(), port_);
        return;
    }
    connection_ = connection;
    state_.store(RdmaConnectionState::CONNECTED);
    RTP_LLM_LOG_INFO("rdma connection to %s:%u connect success", ip_.c_str(), port_);
    processPendingRequests();
}

void RdmaConnection::onConnectFailed(const std::shared_ptr<::accl::barex::XConnection>& connection) {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    state_.store(RdmaConnectionState::FAILED);
    RTP_LLM_LOG_WARNING("rdma connection to %s:%u connect failed", ip_.c_str(), port_);
    auto requests = drainPendingRequests();
    for (auto& req : requests) {
        req.callback(false);
    }
}

void RdmaConnection::read(const std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>& pairs,
                          std::function<void(bool)>                                                 callback,
                          uint64_t                                                                  deadline_ms) {
    auto collector           = std::make_shared<RdmaMetricsCollector>();
    collector->start_time_us = currentTimeUs();

    auto wrapped = [collector, callback, reporter = metrics_reporter_](bool success) {
        if (reporter) {
            collector->success = success;
            reporter->report<RdmaMetric, RdmaMetricsCollector>(nullptr, collector.get());
        }
        callback(success);
    };

    if (state_.load() == RdmaConnectionState::FAILED) {
        wrapped(false);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_requests_.push({pairs, wrapped, collector, deadline_ms});
    }

    if (state_.load() == RdmaConnectionState::CONNECTED) {
        processPendingRequests();
    }
}

std::vector<RdmaConnection::PendingReadRequest> RdmaConnection::drainPendingRequests() {
    std::lock_guard<std::mutex>     lock(pending_mutex_);
    std::vector<PendingReadRequest> out;
    while (!pending_requests_.empty()) {
        out.push_back(std::move(pending_requests_.front()));
        pending_requests_.pop();
    }
    return out;
}

void RdmaConnection::processPendingRequests() {
    if (state_.load() != RdmaConnectionState::CONNECTED) {
        return;
    }
    if (!connection_) {
        RTP_LLM_LOG_WARNING(
            "rdma connection to %s:%u processPendingRequests but connection is nullptr", ip_.c_str(), port_);
        return;
    }
    auto requests = drainPendingRequests();
    for (auto& req : requests) {
        sendReadRequest(req.pairs, req.collector, req.callback, req.deadline_ms);
    }
}

bool RdmaConnection::validateAndBuildRwMemps(
    const std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>& pairs,
    std::vector<::accl::barex::rw_memp_t>&                                    mems,
    uint64_t                                                                  deadline_ms) {
    mems.clear();
    for (const auto& [local, remote] : pairs) {
        if (!local.addr) {
            RTP_LLM_LOG_WARNING("rdma connection read failed, local addr is nullptr");
            return false;
        }
        if (!remote) {
            RTP_LLM_LOG_WARNING("rdma connection read failed, remote buffer is nullptr");
            return false;
        }
        if (local.size != remote->len) {
            RTP_LLM_LOG_WARNING(
                "rdma connection read failed, size mismatch: local %lu, remote %u", local.size, remote->len);
            return false;
        }

        ::accl::barex::memp_t mem;
        if (!memory_manager_->findMemoryMr(mem, local.addr, local.size, local.is_gpu)) {
            RTP_LLM_LOG_WARNING("rdma connection read failed, find local MR failed for addr %p", local.addr);
            return false;
        }

        uint32_t nic_id = connection_->GetChannel()->GetPeerNicId();
        auto     it     = remote->nic_rkeys->find(nic_id);
        if (it == remote->nic_rkeys->end()) {
            RTP_LLM_LOG_WARNING("rdma connection read failed, no rkey for nic %u", nic_id);
            return false;
        }

        ::accl::barex::rw_memp_t rw;
        rw.data     = mem;
        rw.r_addr   = static_cast<uint64_t>(remote->addr);
        rw.r_key    = it->second;
        rw.r_ttl_ms = deadline_ms;
        mems.push_back(rw);
    }
    return true;
}

void RdmaConnection::sendReadRequest(const std::vector<std::pair<LocalBuffer, std::shared_ptr<RemoteBuffer>>>& pairs,
                                     const std::shared_ptr<RdmaMetricsCollector>& collector,
                                     std::function<void(bool)>                    callback,
                                     uint64_t                                     deadline_ms) {
    collector->pending_done_time_us = currentTimeUs();
    if (!connection_) {
        callback(false);
        return;
    }

    std::vector<::accl::barex::rw_memp_t> mems;
    if (!validateAndBuildRwMemps(pairs, mems, deadline_ms)) {
        callback(false);
        return;
    }

    auto rdma_cb = [callback, collector](::accl::barex::Status status) {
        collector->write_done_time_us = currentTimeUs();
        if (!status.IsOk()) {
            RTP_LLM_LOG_WARNING("rdma read batch failed: %s", status.ErrMsg().c_str());
            callback(false);
            return;
        }
        callback(true);
    };
    connection_->GetChannel()->ReadBatch(mems, rdma_cb, false);
}

}  // namespace transfer
}  // namespace rtp_llm
