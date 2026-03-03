#include "gtest/gtest.h"

#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheReceiver.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheRecvTask.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheSender.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

#include "autil/NetUtil.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

namespace rtp_llm::transfer::tcp {

namespace {

BlockInfo makeCpuBlock(void* addr, size_t size_bytes) {
    BlockInfo b;
    b.is_cuda      = false;
    b.device_index = 0;
    b.scalar_type  = 0;
    b.addr         = addr;
    b.size_bytes   = size_bytes;
    return b;
}

bool waitFlag(std::atomic<bool>& flag, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!flag.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return flag.load();
}

std::shared_ptr<RecvRequest> makeRecvReq(const std::string& key, KeyBlockInfosPtr blocks, int64_t deadline) {
    auto req         = std::make_shared<RecvRequest>();
    req->unique_key  = key;
    req->block_info  = std::move(blocks);
    req->deadline_ms = deadline;
    return req;
}

std::shared_ptr<SendRequest>
makeSendReq(const std::string& ip, uint32_t port, const std::string& key, KeyBlockInfosPtr blocks, int64_t deadline) {
    auto req         = std::make_shared<SendRequest>();
    req->ip          = ip;
    req->port        = port;
    req->unique_key  = key;
    req->block_info  = std::move(blocks);
    req->deadline_ms = deadline;
    return req;
}

}  // namespace

// =========================================================================
// Test fixture: receiver 和 sender 在整个 suite 内共享，只初始化一次，
// 避免每个用例重复创建 TCP server/client 导致资源耗尽。
// =========================================================================

class TcpKVCacheTcpTest: public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        port_     = autil::NetUtil::randomPort();
        receiver_ = std::make_unique<TcpKVCacheReceiver>();
        ASSERT_TRUE(receiver_->init(port_, /*io*/ 2, /*worker*/ 4));
        sender_ = std::make_unique<TcpKVCacheSender>();
        ASSERT_TRUE(sender_->init(/*io*/ 2));
    }

    static void TearDownTestSuite() {
        sender_.reset();
        receiver_.reset();
    }

    static uint32_t                            port_;
    static std::unique_ptr<TcpKVCacheReceiver> receiver_;
    static std::unique_ptr<TcpKVCacheSender>   sender_;
};

uint32_t                            TcpKVCacheTcpTest::port_     = 0;
std::unique_ptr<TcpKVCacheReceiver> TcpKVCacheTcpTest::receiver_ = nullptr;
std::unique_ptr<TcpKVCacheSender>   TcpKVCacheTcpTest::sender_   = nullptr;

// 多 key 多块端到端成功：sender 发送两个 cache key（key=101 两块，key=202 一块）
// 期望：sender NONE_ERROR，task success，dst 数据与 src 完全一致
TEST_F(TcpKVCacheTcpTest, FanoutMultiKeySuccess) {
    const std::string unique_key  = "uk_fanout_ok";
    const int64_t     deadline_ms = currentTimeMs() + 5000;

    std::vector<char> src_a(64, 'A');
    std::vector<char> src_b(32, 'B');
    std::vector<char> src_c(16, 'C');

    std::vector<char> dst_a(64, 0);
    std::vector<char> dst_b(32, 0);
    std::vector<char> dst_c(16, 0);

    auto send_blocks    = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[101] = {makeCpuBlock(src_a.data(), src_a.size()), makeCpuBlock(src_b.data(), src_b.size())};
    (*send_blocks)[202] = {makeCpuBlock(src_c.data(), src_c.size())};

    auto recv_blocks    = std::make_shared<KeyBlockInfos>();
    (*recv_blocks)[101] = {makeCpuBlock(dst_a.data(), dst_a.size()), makeCpuBlock(dst_b.data(), dst_b.size())};
    (*recv_blocks)[202] = {makeCpuBlock(dst_c.data(), dst_c.size())};

    auto task = receiver_->recv(makeRecvReq(unique_key, recv_blocks, deadline_ms));
    ASSERT_NE(task, nullptr);

    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::FAILED;
    std::string               cb_msg;
    sender_->send(makeSendReq("127.0.0.1", receiver_->port(), unique_key, send_blocks, deadline_ms),
                  [&cb_done, &cb_ec, &cb_msg](IKVCacheSender::ErrorCode ec, const std::string& msg) {
                      cb_ec   = ec;
                      cb_msg  = msg;
                      cb_done = true;
                  });

    ASSERT_TRUE(waitFlag(cb_done, 5000));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::NONE_ERROR) << cb_msg;

    ASSERT_TRUE(task->waitUntil(deadline_ms));
    auto result = task->getResult();
    EXPECT_TRUE(result.success) << result.error_message;

    EXPECT_EQ(std::memcmp(dst_a.data(), src_a.data(), src_a.size()), 0);
    EXPECT_EQ(std::memcmp(dst_b.data(), src_b.data(), src_b.size()), 0);
    EXPECT_EQ(std::memcmp(dst_c.data(), src_c.data(), src_c.size()), 0);
}

// receiver 未注册任何 recv task，server 轮询至 deadline 后返回 NO_RECV_TASK
// 期望：sender RESPONSE_FAILED
TEST_F(TcpKVCacheTcpTest, NoRecvTask) {
    const std::string unique_key  = "uk_no_task";
    const int64_t     deadline_ms = currentTimeMs() + 3000;

    std::vector<char> src(8, 'X');
    auto              send_blocks = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[1]             = {makeCpuBlock(src.data(), src.size())};

    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::NONE_ERROR;
    std::string               cb_msg;
    sender_->send(makeSendReq("127.0.0.1", receiver_->port(), unique_key, send_blocks, deadline_ms),
                  [&cb_done, &cb_ec, &cb_msg](IKVCacheSender::ErrorCode ec, const std::string& msg) {
                      cb_ec   = ec;
                      cb_msg  = msg;
                      cb_done = true;
                  });
    // waitFlag timeout must be larger than the send deadline so the callback is
    // guaranteed to fire before the test exits (server times out at deadline_ms).
    ASSERT_TRUE(waitFlag(cb_done, 5000));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::RESPONSE_FAILED);
    EXPECT_NE(cb_msg.find("timeout waiting recv task"), std::string::npos) << cb_msg;
}

// sender 发送 1 块，recv 期望 2 块（receiver 期望多于 sender 发送），bufferMatch 失败
// 期望：sender RESPONSE_FAILED，task BUFFER_MISMATCH
TEST_F(TcpKVCacheTcpTest, BufferMismatch) {
    const std::string unique_key  = "uk_mismatch";
    const int64_t     deadline_ms = currentTimeMs() + 5000;

    std::vector<char> src_a(16, 'A');
    std::vector<char> dst_a(16, 0);
    std::vector<char> dst_b(16, 0);

    auto send_blocks  = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[7] = {makeCpuBlock(src_a.data(), src_a.size())};  // sender sends 1 block

    auto recv_blocks  = std::make_shared<KeyBlockInfos>();
    (*recv_blocks)[7] = {makeCpuBlock(dst_a.data(), dst_a.size()),
                         makeCpuBlock(dst_b.data(), dst_b.size())};  // expect 2 blocks, but sender sends 1

    auto task = receiver_->recv(makeRecvReq(unique_key, recv_blocks, deadline_ms));
    ASSERT_NE(task, nullptr);

    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::NONE_ERROR;
    std::string               cb_msg;
    sender_->send(makeSendReq("127.0.0.1", receiver_->port(), unique_key, send_blocks, deadline_ms),
                  [&cb_done, &cb_ec, &cb_msg](IKVCacheSender::ErrorCode ec, const std::string& msg) {
                      cb_ec   = ec;
                      cb_msg  = msg;
                      cb_done = true;
                  });
    ASSERT_TRUE(waitFlag(cb_done, 5000));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::RESPONSE_FAILED);
    EXPECT_NE(cb_msg.find("buffer mismatch"), std::string::npos) << cb_msg;

    ASSERT_TRUE(task->waitUntil(deadline_ms));
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, IKVCacheRecvTask::ErrorCode::BUFFER_MISMATCH);
}

// sender deadline 在调用 send 前已过期，validate() 同步检测，不发起 RPC
// 期望：sender TIMEOUT
TEST_F(TcpKVCacheTcpTest, SenderDeadlineTimeout) {
    std::vector<char> src(4, 'Z');
    auto              send_blocks = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[1]             = {makeCpuBlock(src.data(), src.size())};

    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::NONE_ERROR;
    sender_->send(makeSendReq("127.0.0.1", 1, "uk_timeout", send_blocks, currentTimeMs() - 1),
                  [&cb_done, &cb_ec](IKVCacheSender::ErrorCode ec, const std::string&) {
                      cb_ec   = ec;
                      cb_done = true;
                  });
    ASSERT_TRUE(waitFlag(cb_done, 1000));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::TIMEOUT);
}

// IT5: 完整 cancel 生命周期：注册 → waitUntil 超时 → stealTask → cancel → sender 发送
// 期望：sender RESPONSE_FAILED（服务端 NO_RECV_TASK），task CANCELLED
TEST_F(TcpKVCacheTcpTest, RecvTask_WaitTimeout_Steal_Cancel_Then_Send) {
    const std::string unique_key    = "uk_steal_cancel";
    const int64_t     task_deadline = currentTimeMs() + 5000;

    std::vector<char> dst(16, 0);
    auto              recv_blocks = std::make_shared<KeyBlockInfos>();
    (*recv_blocks)[1]             = {makeCpuBlock(dst.data(), dst.size())};

    auto task = receiver_->recv(makeRecvReq(unique_key, recv_blocks, task_deadline));
    ASSERT_NE(task, nullptr);

    // Step 1: waitUntil 短超时 → 返回 false（无 sender）
    EXPECT_FALSE(task->waitUntil(currentTimeMs() + 50));

    // Step 2: 从 store 中移除 task，server 此后找不到该 key
    auto stolen = receiver_->task_store_->stealTask(unique_key);
    ASSERT_NE(stolen, nullptr);

    // Step 3: cancel task
    task->cancel("wait_timeout");

    // Step 4: sender 以短 deadline 发送，server 轮询 300ms 找不到 task → NO_RECV_TASK
    std::vector<char> src(16, 'S');
    auto              send_blocks = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[1]             = {makeCpuBlock(src.data(), src.size())};

    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::NONE_ERROR;
    std::string               cb_msg;
    sender_->send(makeSendReq("127.0.0.1", receiver_->port(), unique_key, send_blocks, currentTimeMs() + 300),
                  [&cb_done, &cb_ec, &cb_msg](IKVCacheSender::ErrorCode ec, const std::string& msg) {
                      cb_ec   = ec;
                      cb_msg  = msg;
                      cb_done = true;
                  });

    ASSERT_TRUE(waitFlag(cb_done, 500));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::RESPONSE_FAILED);
    EXPECT_NE(cb_msg.find("timeout waiting recv task"), std::string::npos) << cb_msg;

    // task 已被 cancel，应为 CANCELLED 终态
    ASSERT_TRUE(task->waitUntil(currentTimeMs() + 100));
    auto result = task->getResult();
    EXPECT_EQ(result.error_code, IKVCacheRecvTask::ErrorCode::CANCELLED);
}

// IT6: sender 向未监听的端口发送，RPC 连接失败
// 期望：sender FAILED
TEST_F(TcpKVCacheTcpTest, Sender_InvalidPort) {
    uint32_t dead_port = autil::NetUtil::randomPort();  // 无 receiver 监听

    std::vector<char> src(8, 'Y');
    auto              send_blocks = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[1]             = {makeCpuBlock(src.data(), src.size())};

    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::NONE_ERROR;
    std::string               cb_msg;
    sender_->send(makeSendReq("127.0.0.1", dead_port, "uk_invalid_port", send_blocks, currentTimeMs() + 2000),
                  [&cb_done, &cb_ec, &cb_msg](IKVCacheSender::ErrorCode ec, const std::string& msg) {
                      cb_ec   = ec;
                      cb_msg  = msg;
                      cb_done = true;
                  });

    ASSERT_TRUE(waitFlag(cb_done, 5000));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::FAILED);
    EXPECT_FALSE(cb_msg.empty());
}

// IT7: recv task deadline 极短，sender 在 deadline 过后才发送
// markSuccess() 检查 deadline → TIMEOUT；server 返回 TIMEOUT
// 期望：sender RESPONSE_FAILED + msg 含 "recv task timeout"，task TIMEOUT
TEST_F(TcpKVCacheTcpTest, RecvTask_Deadline_Short) {
    const std::string unique_key = "uk_deadline_short";

    std::vector<char> src(16, 'T');
    std::vector<char> dst(16, 0);
    auto              send_blocks = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[1]             = {makeCpuBlock(src.data(), src.size())};
    auto recv_blocks              = std::make_shared<KeyBlockInfos>();
    (*recv_blocks)[1]             = {makeCpuBlock(dst.data(), dst.size())};

    // task deadline 极短：50ms 后过期
    auto task = receiver_->recv(makeRecvReq(unique_key, recv_blocks, currentTimeMs() + 50));
    ASSERT_NE(task, nullptr);

    // 等待 task deadline 过期
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // sender 以长 deadline 发送，server 找到 task 但 markSuccess() 检查 deadline → TIMEOUT
    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::NONE_ERROR;
    std::string               cb_msg;
    sender_->send(makeSendReq("127.0.0.1", receiver_->port(), unique_key, send_blocks, currentTimeMs() + 5000),
                  [&cb_done, &cb_ec, &cb_msg](IKVCacheSender::ErrorCode ec, const std::string& msg) {
                      cb_ec   = ec;
                      cb_msg  = msg;
                      cb_done = true;
                  });

    ASSERT_TRUE(waitFlag(cb_done, 5000));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::RESPONSE_FAILED);
    EXPECT_NE(cb_msg.find("recv task timeout"), std::string::npos) << cb_msg;

    ASSERT_TRUE(task->waitUntil(currentTimeMs() + 1000));
    auto result = task->getResult();
    EXPECT_EQ(result.error_code, IKVCacheRecvTask::ErrorCode::TIMEOUT);
}

// IT8: 同 key 两次 recv，第二次应返回 nullptr
TEST_F(TcpKVCacheTcpTest, DuplicateKey_Second_Recv_Null) {
    const std::string unique_key  = "uk_dup_key";
    const int64_t     deadline_ms = currentTimeMs() + 5000;

    std::vector<char> dst_a(16, 0);
    std::vector<char> dst_b(16, 0);

    auto recv_blocks_a  = std::make_shared<KeyBlockInfos>();
    (*recv_blocks_a)[1] = {makeCpuBlock(dst_a.data(), dst_a.size())};

    auto recv_blocks_b  = std::make_shared<KeyBlockInfos>();
    (*recv_blocks_b)[1] = {makeCpuBlock(dst_b.data(), dst_b.size())};

    auto task_first  = receiver_->recv(makeRecvReq(unique_key, recv_blocks_a, deadline_ms));
    auto task_second = receiver_->recv(makeRecvReq(unique_key, recv_blocks_b, deadline_ms));

    EXPECT_NE(task_first, nullptr);
    EXPECT_EQ(task_second, nullptr);
}

// IT10: sender 块大小（32B）与 recv 期望（16B）不匹配，块数相同
// bufferMatch 失败 → task BUFFER_MISMATCH；server 返回 BUFFER_MISMATCH
// 期望：sender RESPONSE_FAILED + msg 含 "buffer mismatch"，task BUFFER_MISMATCH
TEST_F(TcpKVCacheTcpTest, Sender_BlockSizeMismatch) {
    const std::string unique_key  = "uk_size_mismatch";
    const int64_t     deadline_ms = currentTimeMs() + 5000;

    std::vector<char> src(32, 'M');  // sender：32B
    std::vector<char> dst(16, 0);    // recv 期望：16B

    auto send_blocks  = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[1] = {makeCpuBlock(src.data(), src.size())};

    auto recv_blocks  = std::make_shared<KeyBlockInfos>();
    (*recv_blocks)[1] = {makeCpuBlock(dst.data(), dst.size())};

    auto task = receiver_->recv(makeRecvReq(unique_key, recv_blocks, deadline_ms));
    ASSERT_NE(task, nullptr);

    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::NONE_ERROR;
    std::string               cb_msg;
    sender_->send(makeSendReq("127.0.0.1", receiver_->port(), unique_key, send_blocks, deadline_ms),
                  [&cb_done, &cb_ec, &cb_msg](IKVCacheSender::ErrorCode ec, const std::string& msg) {
                      cb_ec   = ec;
                      cb_msg  = msg;
                      cb_done = true;
                  });

    ASSERT_TRUE(waitFlag(cb_done, 5000));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::RESPONSE_FAILED);
    EXPECT_NE(cb_msg.find("buffer mismatch"), std::string::npos) << cb_msg;

    ASSERT_TRUE(task->waitUntil(deadline_ms));
    auto result = task->getResult();
    EXPECT_EQ(result.error_code, IKVCacheRecvTask::ErrorCode::BUFFER_MISMATCH);
}

// Extra2: 另一个线程 cancel task，主线程 waitUntil 获取 CANCELLED 结果
TEST_F(TcpKVCacheTcpTest, RecvTask_CancelInAnotherThread) {
    const std::string unique_key  = "uk_cancel_thread";
    const int64_t     deadline_ms = currentTimeMs() + 5000;

    std::vector<char> dst(16, 0);
    auto              recv_blocks = std::make_shared<KeyBlockInfos>();
    (*recv_blocks)[1]             = {makeCpuBlock(dst.data(), dst.size())};

    auto task = receiver_->recv(makeRecvReq(unique_key, recv_blocks, deadline_ms));
    ASSERT_NE(task, nullptr);

    // 另一个线程在 50ms 后 cancel
    std::thread cancel_thread([&task]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        task->cancel("from_thread");
    });

    // 主线程等待 task 完成
    ASSERT_TRUE(task->waitUntil(currentTimeMs() + 2000));
    cancel_thread.join();

    auto result = task->getResult();
    EXPECT_EQ(result.error_code, IKVCacheRecvTask::ErrorCode::CANCELLED);
}

// Extra3: sender 先发送，receiver 稍后注册 task（竞态：server 轮询等待）
// 期望：sender NONE_ERROR，task success，数据正确拷贝
TEST_F(TcpKVCacheTcpTest, Sender_Before_Recv) {
    const std::string unique_key  = "uk_sender_first";
    const int64_t     deadline_ms = currentTimeMs() + 5000;

    std::vector<char> src(32, 'R');
    std::vector<char> dst(32, 0);
    auto              send_blocks = std::make_shared<KeyBlockInfos>();
    (*send_blocks)[1]             = {makeCpuBlock(src.data(), src.size())};

    // sender 先发（task 尚未注册，server 开始轮询）
    std::atomic<bool>         cb_done(false);
    IKVCacheSender::ErrorCode cb_ec = IKVCacheSender::ErrorCode::FAILED;
    std::string               cb_msg;
    sender_->send(makeSendReq("127.0.0.1", receiver_->port(), unique_key, send_blocks, deadline_ms),
                  [&cb_done, &cb_ec, &cb_msg](IKVCacheSender::ErrorCode ec, const std::string& msg) {
                      cb_ec   = ec;
                      cb_msg  = msg;
                      cb_done = true;
                  });

    // 50ms 后 receiver 注册 task，server 轮询到 task 后完成传输
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto recv_blocks  = std::make_shared<KeyBlockInfos>();
    (*recv_blocks)[1] = {makeCpuBlock(dst.data(), dst.size())};
    auto task         = receiver_->recv(makeRecvReq(unique_key, recv_blocks, deadline_ms));
    ASSERT_NE(task, nullptr);

    ASSERT_TRUE(waitFlag(cb_done, 5000));
    EXPECT_EQ(cb_ec, IKVCacheSender::ErrorCode::NONE_ERROR) << cb_msg;

    ASSERT_TRUE(task->waitUntil(deadline_ms));
    auto result = task->getResult();
    EXPECT_TRUE(result.success) << result.error_message;
    EXPECT_EQ(std::memcmp(dst.data(), src.data(), src.size()), 0);
}

}  // namespace rtp_llm::transfer::tcp
