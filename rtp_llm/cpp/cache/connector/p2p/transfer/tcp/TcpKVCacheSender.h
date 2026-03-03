#pragma once

#include "rtp_llm/cpp/cache/connector/p2p/transfer/IKVCacheSender.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/proto/service.pb.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/CudaCopyUtil.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/utils/TcpClient.h"

#include "kmonitor/client/MetricsReporter.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace rtp_llm::transfer::tcp {

class TcpKVCacheSender final: public IKVCacheSender {
public:
    explicit TcpKVCacheSender(kmonitor::MetricsReporterPtr metrics_reporter = nullptr);
    ~TcpKVCacheSender() override = default;

public:
    bool init(int io_thread_count);

public:
    bool regMem(const BlockInfo& block_info, uint64_t aligned_size = 0) override;
    void send(const SendRequestPtr& request, std::function<void(ErrorCode, const std::string&)> callback) override;

private:
    std::shared_ptr<::transfer::tcp::TransferRequestPB> makeTransferRequest(const SendRequestPtr& request);

    std::pair<ErrorCode, std::string>
    validate(const SendRequestPtr& request, int64_t& block_count, int64_t& total_block_size);

    void callSend(const SendRequestPtr& request, std::function<void(ErrorCode, const std::string&)> callback);

private:
    std::shared_ptr<TcpClient>    tcp_client_;
    kmonitor::MetricsReporterPtr  metrics_reporter_;
    std::unique_ptr<CudaCopyUtil> cuda_copy_util_;
};

}  // namespace rtp_llm::transfer::tcp
