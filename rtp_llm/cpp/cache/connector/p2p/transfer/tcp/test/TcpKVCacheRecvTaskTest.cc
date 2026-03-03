#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheRecvTask.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"

namespace rtp_llm::transfer::tcp {

using EC = IKVCacheRecvTask::ErrorCode;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static char g_dummy_buf[1024];

static KeyBlockInfosPtr makeKeyBlockInfos(int64_t key = 1, size_t size_bytes = 64) {
    auto      info = std::make_shared<KeyBlockInfos>();
    BlockInfo b;
    b.is_cuda      = false;
    b.device_index = 0;
    b.scalar_type  = 0;
    b.addr         = g_dummy_buf;
    b.size_bytes   = size_bytes;
    (*info)[key]   = {b};
    return info;
}

// Create a task whose own deadline is `deadline_offset_ms` from now.
static std::shared_ptr<TcpKVCacheRecvTask> makeTask(int64_t          deadline_offset_ms = 5000,
                                                    KeyBlockInfosPtr block_info         = nullptr) {
    if (!block_info) {
        block_info = makeKeyBlockInfos();
    }
    return std::make_shared<TcpKVCacheRecvTask>("test_key", block_info, currentTimeMs() + deadline_offset_ms);
}

// Non-blocking terminal check: replaces the removed isTerminal().
// waitUntil(0) returns true immediately if terminal, false otherwise
// (because deadline_ms=0 hits the "deadline_ms<=0 → false" path when not terminal).
static bool isTerminalNonBlocking(const std::shared_ptr<TcpKVCacheRecvTask>& task) {
    return task->waitUntil(0);
}

// =====================================================================
// Group A: Constructor / Initial State
// =====================================================================

// A1: valid construction — initial state is non-terminal.
TEST(TcpKVCacheRecvTaskTest, A1_ConstructorValid) {
    auto blocks = makeKeyBlockInfos(42, 128);
    auto task   = std::make_shared<TcpKVCacheRecvTask>("uk_a1", blocks, currentTimeMs() + 5000);

    EXPECT_FALSE(isTerminalNonBlocking(task));
    EXPECT_FALSE(task->getResult().success);
    EXPECT_EQ(task->getExpectedBlockInfo(), blocks);
    EXPECT_EQ(task->getExpectedBlockInfo()->count(42), 1u);

    // Caller deadline already past → false immediately (task not yet terminal)
    EXPECT_FALSE(task->waitUntil(currentTimeMs() - 1));
}

// A2: nullptr block_info → task is immediately terminal with RECV_FAILED.
TEST(TcpKVCacheRecvTaskTest, A2_ConstructorNullBlockInfo) {
    auto task = std::make_shared<TcpKVCacheRecvTask>("uk_a2", nullptr, currentTimeMs() + 5000);

    EXPECT_TRUE(isTerminalNonBlocking(task));
    EXPECT_TRUE(task->waitUntil(currentTimeMs() + 5000));

    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::RECV_FAILED);
    EXPECT_FALSE(result.error_message.empty());
}

// =====================================================================
// Group B: waitUntil + markSuccess
// =====================================================================

// B1: markSuccess happens before waitUntil is called.
TEST(TcpKVCacheRecvTaskTest, B1_MarkSuccess_Then_WaitUntil) {
    auto task = makeTask();

    EXPECT_TRUE(task->markProcessing());
    task->markSuccess();

    EXPECT_TRUE(task->waitUntil(currentTimeMs() + 5000));
    auto result = task->getResult();
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.error_code, EC::NONE_ERROR);
    EXPECT_TRUE(result.error_message.empty());
}

// B2: waitUntil blocks first; markSuccess arrives ~100 ms later.
TEST(TcpKVCacheRecvTaskTest, B2_WaitUntil_Then_MarkSuccess) {
    auto task = makeTask();

    auto worker = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        task->markProcessing();
        task->markSuccess();
    });

    auto t0      = std::chrono::steady_clock::now();
    bool waited  = task->waitUntil(currentTimeMs() + 5000);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    worker.join();

    EXPECT_TRUE(waited);
    EXPECT_GE(elapsed, 80);
    EXPECT_LE(elapsed, 500);
    EXPECT_TRUE(task->getResult().success);
}

// B3: waitUntil blocks first; markSuccess arrives after the task's own deadline has passed.
//     markSuccess detects now > deadline_ms_ and yields TIMEOUT instead of SUCCESS.
TEST(TcpKVCacheRecvTaskTest, B3_WaitUntil_Then_MarkSuccess_AlreadyTimeout) {
    // Task deadline = now + 50 ms (very short).
    auto task = makeTask(50);

    auto worker = std::thread([&]() {
        task->markProcessing();
        // Sleep past the task deadline before calling markSuccess.
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        // deadline_ms_ is now in the past → markSuccess yields TIMEOUT
        task->markSuccess();
    });

    bool waited = task->waitUntil(currentTimeMs() + 5000);
    worker.join();

    EXPECT_TRUE(waited);
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::TIMEOUT);
}

// =====================================================================
// Group C: waitUntil + markFailed
// =====================================================================

// C1: markFailed happens before waitUntil is called.
TEST(TcpKVCacheRecvTaskTest, C1_MarkFailed_Then_WaitUntil) {
    auto task = makeTask();

    task->markProcessing();
    task->markFailed(EC::RECV_FAILED, "err");

    EXPECT_TRUE(task->waitUntil(currentTimeMs() + 5000));
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::RECV_FAILED);
    EXPECT_EQ(result.error_message, "err");
}

// C2: waitUntil blocks first; markFailed arrives ~100 ms later.
TEST(TcpKVCacheRecvTaskTest, C2_WaitUntil_Then_MarkFailed) {
    auto task = makeTask();

    auto worker = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        task->markProcessing();
        task->markFailed(EC::BUFFER_MISMATCH, "mismatch");
    });

    bool waited = task->waitUntil(currentTimeMs() + 5000);
    worker.join();

    EXPECT_TRUE(waited);
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::BUFFER_MISMATCH);
    EXPECT_EQ(result.error_message, "mismatch");
}

// =====================================================================
// Group D: waitUntil + cancel (not yet in processing state)
// =====================================================================

// D1: cancel happens before waitUntil is called.
TEST(TcpKVCacheRecvTaskTest, D1_Cancel_Then_WaitUntil) {
    auto task = makeTask();

    task->cancel("reason");

    EXPECT_TRUE(task->waitUntil(currentTimeMs() + 5000));
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::CANCELLED);
    EXPECT_EQ(result.error_message, "reason");
}

// D2: waitUntil blocks first; cancel arrives ~100 ms later.
TEST(TcpKVCacheRecvTaskTest, D2_WaitUntil_Then_Cancel) {
    auto task = makeTask();

    auto canceller = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        task->cancel("reason");
    });

    auto t0      = std::chrono::steady_clock::now();
    bool waited  = task->waitUntil(currentTimeMs() + 5000);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    canceller.join();

    EXPECT_TRUE(waited);
    EXPECT_GE(elapsed, 80);
    EXPECT_LE(elapsed, 500);
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::CANCELLED);
    EXPECT_EQ(result.error_message, "reason");
}

// =====================================================================
// Group E: waitUntil + cancel (while processing) + markSuccess
// cancel sets cancel_requested_; markSuccess detects it and yields CANCELLED.
// =====================================================================

// E1: sequence markProcessing → cancel → markSuccess all happen before waitUntil.
//     cancel is prioritised over markSuccess.
TEST(TcpKVCacheRecvTaskTest, E1_Processing_Cancel_MarkSuccess_Then_WaitUntil) {
    auto task = makeTask();

    task->markProcessing();
    task->cancel("c");
    // cancel while processing: sets flag only; task is still non-terminal
    EXPECT_FALSE(isTerminalNonBlocking(task));

    task->markSuccess();  // cancel_requested_ → CANCELLED

    EXPECT_TRUE(task->waitUntil(currentTimeMs() + 5000));
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::CANCELLED);
    EXPECT_EQ(result.error_message, "c");
}

// E2: markProcessing → cancel; then waitUntil blocks (task not yet terminal);
//     markSuccess arrives later → terminal with CANCELLED.
TEST(TcpKVCacheRecvTaskTest, E2_Processing_Cancel_WaitUntil_Then_MarkSuccess) {
    auto task = makeTask();

    task->markProcessing();
    task->cancel("c");
    // cancel while processing: task is still non-terminal
    EXPECT_FALSE(isTerminalNonBlocking(task));

    auto worker = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        task->markSuccess();  // cancel_requested_ → CANCELLED
    });

    bool waited = task->waitUntil(currentTimeMs() + 5000);
    worker.join();

    EXPECT_TRUE(waited);
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::CANCELLED);
    EXPECT_EQ(result.error_message, "c");
}

// E3: markProcessing → markSuccess → cancel; cancel arrives too late → SUCCESS.
TEST(TcpKVCacheRecvTaskTest, E3_Processing_MarkSuccess_Then_Cancel) {
    auto task = makeTask();

    task->markProcessing();
    task->markSuccess();
    task->cancel("too_late");  // already terminal → no effect

    EXPECT_TRUE(task->waitUntil(currentTimeMs() + 5000));
    auto result = task->getResult();
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.error_code, EC::NONE_ERROR);
}

// =====================================================================
// Group F: waitUntil + cancel (while processing) + markFailed
// =====================================================================

// F1: sequence markProcessing → cancel → markFailed all happen before waitUntil.
//     cancel is prioritised over markFailed.
TEST(TcpKVCacheRecvTaskTest, F1_Processing_Cancel_MarkFailed_Then_WaitUntil) {
    auto task = makeTask();

    task->markProcessing();
    task->cancel("c");
    EXPECT_FALSE(isTerminalNonBlocking(task));

    task->markFailed(EC::RECV_FAILED, "f");  // cancel_requested_ → CANCELLED

    EXPECT_TRUE(task->waitUntil(currentTimeMs() + 5000));
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::CANCELLED);
    EXPECT_EQ(result.error_message, "c");
}

// F2: markProcessing → cancel; then waitUntil blocks;
//     markFailed arrives later → terminal with CANCELLED.
TEST(TcpKVCacheRecvTaskTest, F2_Processing_Cancel_WaitUntil_Then_MarkFailed) {
    auto task = makeTask();

    task->markProcessing();
    task->cancel("c");
    EXPECT_FALSE(isTerminalNonBlocking(task));

    auto worker = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        task->markFailed(EC::RECV_FAILED, "f");  // cancel_requested_ → CANCELLED
    });

    bool waited = task->waitUntil(currentTimeMs() + 5000);
    worker.join();

    EXPECT_TRUE(waited);
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::CANCELLED);
    EXPECT_EQ(result.error_message, "c");
}

// F3: markProcessing → markFailed → cancel; cancel too late → FAILED.
TEST(TcpKVCacheRecvTaskTest, F3_Processing_MarkFailed_Then_Cancel) {
    auto task = makeTask();

    task->markProcessing();
    task->markFailed(EC::RECV_FAILED, "f");
    task->cancel("too_late");  // already terminal → no effect

    EXPECT_TRUE(task->waitUntil(currentTimeMs() + 5000));
    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::RECV_FAILED);
    EXPECT_EQ(result.error_message, "f");
}

// =====================================================================
// Group G: waitUntil's own caller-deadline expires (no external signal)
// =====================================================================

// G1: Task deadline is far in the future; waitUntil's own deadline is short.
//     waitUntil returns false; the task itself remains non-terminal.
TEST(TcpKVCacheRecvTaskTest, G1_WaitUntil_SelfDeadlineExpires) {
    auto task = makeTask(10000);  // task deadline = now + 10 s

    auto t0      = std::chrono::steady_clock::now();
    bool waited  = task->waitUntil(currentTimeMs() + 100);  // caller deadline = now + 100 ms
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

    EXPECT_FALSE(waited);
    EXPECT_FALSE(isTerminalNonBlocking(task));
    EXPECT_GE(elapsed, 80);
    EXPECT_LE(elapsed, 500);
}

// =====================================================================
// Group H: Idempotency and boundary checks
// =====================================================================

// H1: markProcessing on an already-terminal task must return false.
TEST(TcpKVCacheRecvTaskTest, H1_MarkProcessing_AfterTerminal) {
    auto task = makeTask();
    task->markProcessing();
    task->markFailed(EC::RECV_FAILED, "f");
    EXPECT_TRUE(isTerminalNonBlocking(task));
    EXPECT_FALSE(task->markProcessing());
}

// H2: repeated markSuccess calls do not crash and do not change the result.
TEST(TcpKVCacheRecvTaskTest, H2_MarkSuccess_Idempotent) {
    auto task = makeTask();
    task->markProcessing();
    task->markSuccess();
    auto first = task->getResult();

    task->markSuccess();
    auto second = task->getResult();

    EXPECT_EQ(first.success, second.success);
    EXPECT_EQ(first.error_code, second.error_code);
    EXPECT_EQ(first.error_message, second.error_message);
}

// H3: repeated markFailed calls do not crash and do not change the result.
TEST(TcpKVCacheRecvTaskTest, H3_MarkFailed_Idempotent) {
    auto task = makeTask();
    task->markProcessing();
    task->markFailed(EC::RECV_FAILED, "first");
    auto first = task->getResult();

    task->markFailed(EC::BUFFER_MISMATCH, "second");
    auto second = task->getResult();

    EXPECT_EQ(first.error_code, second.error_code);
    EXPECT_EQ(first.error_message, second.error_message);
}

// H4: repeated cancel calls do not crash and do not change the result.
TEST(TcpKVCacheRecvTaskTest, H4_Cancel_Idempotent) {
    auto task = makeTask();
    task->cancel("first");
    auto first = task->getResult();

    task->cancel("second");
    task->cancel("third");
    auto third = task->getResult();

    EXPECT_EQ(first.error_code, third.error_code);
    EXPECT_EQ(first.error_message, third.error_message);
}

}  // namespace rtp_llm::transfer::tcp
