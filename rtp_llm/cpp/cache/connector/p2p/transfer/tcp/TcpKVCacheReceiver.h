#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheReceiver.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheTaskStore.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheReceiverService.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/TcpServer.h"

#include "kmonitor/client/MetricsReporter.h"

#include <memory>
#include <string>

namespace rtp_llm::transfer::tcp {

class TcpKVCacheReceiver final: public IKVCacheReceiver {
public:
    explicit TcpKVCacheReceiver(kmonitor::MetricsReporterPtr metrics_reporter = nullptr);
    ~TcpKVCacheReceiver() override;

public:
    bool init(uint32_t listen_port, int io_thread_count, int worker_thread_count, bool enable_arpc_metric = true);

    std::string ip() const;
    uint32_t    port() const;

public:
    bool                regMem(const BlockInfo& block_info, uint64_t aligned_size = 0) override;
    IKVCacheRecvTaskPtr recv(const RecvRequestPtr& request) override;

private:
    std::shared_ptr<transfer::TcpServer>       server_;
    std::shared_ptr<TcpKVCacheTaskStore>       task_store_;
    std::shared_ptr<TcpKVCacheReceiverService> service_;
    kmonitor::MetricsReporterPtr               metrics_reporter_;
};

}  // namespace rtp_llm::transfer::tcp
