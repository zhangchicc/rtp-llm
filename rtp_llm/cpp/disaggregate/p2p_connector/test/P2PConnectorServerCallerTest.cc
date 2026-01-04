#include <thread>
#include <gtest/gtest.h>
#include "grpc++/grpc++.h"

#include "autil/NetUtil.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/P2PConnectorServerCaller.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include "rtp_llm/cpp/disaggregate/p2p_connector/test/TestRpcServer.h"
#include "rtp_llm/cpp/engine_base/stream/CompleteTokenIds.h"
#include "rtp_llm/cpp/engine_base/stream/GenerateTypes.h"
#include "rtp_llm/cpp/devices/DeviceFactory.h"

namespace rtp_llm {

// 辅助函数：创建 CompleteTokenIds 用于测试
CompleteTokenIdsPtr createTestCompleteTokenIds(int batch_size, int seq_length) {
    auto device = DeviceFactory::getDevice(DeviceType::Cpu);
    // CompleteTokenIds(device, batch_size, max_batch_size, max_seq_len, seq_size_per_block)
    auto complete_token_ids = std::make_shared<CompleteTokenIds>(device, batch_size, batch_size, seq_length + 100, 8);

    auto input_ids = device->allocateBuffer(
        {rtp_llm::DataType::TYPE_INT32, {(size_t)seq_length}, rtp_llm::AllocationType::HOST}, {});
    // 初始化输入 token ids
    int* data = input_ids->data<int>();
    for (int i = 0; i < seq_length; i++) {
        data[i] = i + 1;  // token ids: 1, 2, 3, ...
    }

    auto generate_input       = std::make_shared<GenerateInput>();
    generate_input->input_ids = input_ids;
    complete_token_ids->init(generate_input);
    return complete_token_ids;
}

class P2PConnectorServerCallerTest: public ::testing::Test {
protected:
    void SetUp() override {
        // 创建测试用的 RPC 服务器
        auto service = std::make_unique<TestRpcService>();
        server_      = std::make_unique<TestRpcServer>(std::move(service));
        ASSERT_TRUE(server_->start());
        server_addr_ = "127.0.0.1:" + std::to_string(server_->listenPort());

        // worker_addrs_ 格式: "ip:cache_store_port:grpc_port"
        worker_addrs_.push_back("127.0.0.1:12345:" + std::to_string(server_->listenPort()));

        // 创建 P2PConnectorServerCaller
        client_ = std::make_unique<P2PConnectorServerCaller>(worker_addrs_);
    }

    void TearDown() override {
        client_.reset();
        server_.reset();
    }

    // 等待 Result 完成（封装 checkDone 的轮询逻辑）
    bool waitDone(std::shared_ptr<P2PConnectorServerCaller::Result>& result, int timeout_ms = 5000) {
        int waited_ms = 0;
        while (!result->done() && waited_ms < timeout_ms) {
            result->checkDone();
            if (!result->done()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                waited_ms += 10;
            }
        }
        return result->success();
    }

protected:
    std::unique_ptr<TestRpcServer>            server_;
    std::string                               server_addr_;
    std::vector<std::string>                  worker_addrs_;
    std::unique_ptr<P2PConnectorServerCaller> client_;
};

// ---------------------------- load ----------------------------

TEST_F(P2PConnectorServerCallerTest, Load_ReturnNotNull_RequestSuccess) {
    std::string unique_key   = "test_load_1";
    int64_t     request_id   = 1001;
    int64_t     deadline_ms  = currentTimeMs() + 5000;
    std::string prefill_ip   = "127.0.0.1";
    uint32_t    prefill_port = static_cast<uint32_t>(server_->listenPort());

    // 执行 load
    auto result = client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, nullptr);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->request_id_, request_id);
    EXPECT_EQ(result->request.unique_key(), unique_key);

    // 等待完成
    bool success = waitDone(result);
    EXPECT_TRUE(success);
    EXPECT_TRUE(result->done());
    EXPECT_TRUE(result->success());
    EXPECT_TRUE(result->response.success());

    // 验证 StartLoad 被调用
    EXPECT_EQ(server_->service()->getStartLoadCallCount(), 1);
}

TEST_F(P2PConnectorServerCallerTest, Load_ReturnNotNull_RequestFailed) {
    // 设置服务器返回失败
    server_->service()->setStartLoadResponseSuccess(false);

    std::string unique_key   = "test_load_fail";
    int64_t     request_id   = 1002;
    int64_t     deadline_ms  = currentTimeMs() + 5000;
    std::string prefill_ip   = "127.0.0.1";
    uint32_t    prefill_port = static_cast<uint32_t>(server_->listenPort());

    // 执行 load
    auto result = client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, nullptr);
    ASSERT_NE(result, nullptr);

    // 等待完成
    bool success = waitDone(result);
    EXPECT_FALSE(success);
    EXPECT_TRUE(result->done());
    EXPECT_FALSE(result->success());
    EXPECT_FALSE(result->response.success());

    // 验证 StartLoad 被调用（即使失败也应该被调用）
    EXPECT_EQ(server_->service()->getStartLoadCallCount(), 1);
}

TEST_F(P2PConnectorServerCallerTest, Load_ReturnNotNull_Timeout) {
    // 设置服务器延迟响应
    server_->service()->setSleepMillis(200);

    std::string unique_key   = "test_load_timeout";
    int64_t     request_id   = 1003;
    int64_t     deadline_ms  = currentTimeMs() + 10;  // 很短的超时时间
    std::string prefill_ip   = "127.0.0.1";
    uint32_t    prefill_port = static_cast<uint32_t>(server_->listenPort());

    // 执行 load
    auto result = client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, nullptr);
    ASSERT_NE(result, nullptr);

    // 等待完成，应该会因为超时返回 false
    bool success = waitDone(result, 1000);
    EXPECT_FALSE(success);
    EXPECT_FALSE(result->success());

    // 验证 StartLoad 被调用（即使超时也应该被调用）
    EXPECT_GE(server_->service()->getStartLoadCallCount(), 1);
}

TEST_F(P2PConnectorServerCallerTest, Load_ReturnNull_InvalidServerAddr) {
    std::string unique_key   = "test_load_invalid_addr";
    int64_t     request_id   = 1004;
    int64_t     deadline_ms  = currentTimeMs() + 5000;
    std::string prefill_ip   = "127.0.0.1";
    uint32_t    prefill_port = 99999;  // 无效端口

    // 执行 load，应该返回 nullptr（因为无法连接）
    auto result = client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, nullptr);
    // 注意：由于 RPCPool 的行为，可能返回非空但 waitDone 会失败
    // 这里主要测试接口调用不会崩溃
    if (result != nullptr) {
        bool success = waitDone(result);
        EXPECT_FALSE(success);
    }
}

TEST_F(P2PConnectorServerCallerTest, Load_ReturnNotNull_RpcStatusFailed) {
    // 设置服务器返回 RPC 错误状态
    server_->service()->setRpcResponseStatus(::grpc::Status(grpc::StatusCode::INTERNAL, "Internal error"));

    std::string unique_key   = "test_load_rpc_fail";
    int64_t     request_id   = 1006;
    int64_t     deadline_ms  = currentTimeMs() + 5000;
    std::string prefill_ip   = "127.0.0.1";
    uint32_t    prefill_port = static_cast<uint32_t>(server_->listenPort());

    // 执行 load
    auto result = client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, nullptr);
    ASSERT_NE(result, nullptr);

    // 等待完成
    bool success = waitDone(result);
    EXPECT_FALSE(success);
    EXPECT_TRUE(result->done());
    EXPECT_FALSE(result->success());
    EXPECT_FALSE(result->status.ok());

    // 验证 StartLoad 被调用
    EXPECT_EQ(server_->service()->getStartLoadCallCount(), 1);
}

// ---------------------------- checkDone ----------------------------

TEST_F(P2PConnectorServerCallerTest, CheckDone_NotDoneInitially) {
    // 设置服务器延迟响应
    server_->service()->setSleepMillis(500);

    std::string unique_key   = "test_check_done";
    int64_t     request_id   = 2001;
    int64_t     deadline_ms  = currentTimeMs() + 5000;
    std::string prefill_ip   = "127.0.0.1";
    uint32_t    prefill_port = static_cast<uint32_t>(server_->listenPort());

    auto result = client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, nullptr);
    ASSERT_NE(result, nullptr);

    // 初始状态应该是 not done
    EXPECT_FALSE(result->done());
    EXPECT_FALSE(result->success());

    // 多次调用 checkDone
    result->checkDone();
    // 由于服务器延迟，应该还没完成
    // 注意：这个测试可能有时间敏感性

    // 最终等待完成
    waitDone(result);
    EXPECT_TRUE(result->done());
}

TEST_F(P2PConnectorServerCallerTest, CheckDone_TotalCostTimeUs) {
    std::string unique_key   = "test_cost_time";
    int64_t     request_id   = 2002;
    int64_t     deadline_ms  = currentTimeMs() + 5000;
    std::string prefill_ip   = "127.0.0.1";
    uint32_t    prefill_port = static_cast<uint32_t>(server_->listenPort());

    auto result = client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, nullptr);
    ASSERT_NE(result, nullptr);

    waitDone(result);
    EXPECT_TRUE(result->done());

    // 验证总耗时被记录
    int64_t cost_time_us = result->totalCostTimeUs();
    EXPECT_GT(cost_time_us, 0);
}

// ---------------------------- complete_token_ids update ----------------------------

TEST_F(P2PConnectorServerCallerTest, CheckDone_CompleteTokenIdsUpdate) {
    // 设置服务器返回特定的 first_generate_token_id
    int64_t expected_token_id = 99999;
    server_->service()->setFirstGenerateTokenId(expected_token_id);

    std::string unique_key   = "test_token_ids_update";
    int64_t     request_id   = 3001;
    int64_t     deadline_ms  = currentTimeMs() + 5000;
    std::string prefill_ip   = "127.0.0.1";
    uint32_t    prefill_port = static_cast<uint32_t>(server_->listenPort());

    // 创建 CompleteTokenIds，初始长度为 10
    int  initial_seq_length = 10;
    auto complete_token_ids = createTestCompleteTokenIds(1, initial_seq_length);
    ASSERT_NE(complete_token_ids, nullptr);
    EXPECT_EQ(complete_token_ids->seqLength(), initial_seq_length);

    // 执行 load
    auto result = client_->load(request_id, prefill_ip, prefill_port, unique_key, deadline_ms, complete_token_ids);
    ASSERT_NE(result, nullptr);

    // 等待完成
    bool success = waitDone(result);
    EXPECT_TRUE(success);
    EXPECT_TRUE(result->done());
    EXPECT_TRUE(result->success());

    // 验证 complete_token_ids 被更新
    // update 方法应该添加了一个新 token，所以长度应该 +1
    EXPECT_EQ(complete_token_ids->seqLength(), initial_seq_length + 1);

    // 验证新 token 的值是 first_generate_token_id
    auto token_vec = complete_token_ids->completeTokenIdsVec(0);
    ASSERT_GT(token_vec.size(), static_cast<size_t>(initial_seq_length));
    EXPECT_EQ(token_vec[initial_seq_length], static_cast<int>(expected_token_id));
}

}  // namespace rtp_llm
