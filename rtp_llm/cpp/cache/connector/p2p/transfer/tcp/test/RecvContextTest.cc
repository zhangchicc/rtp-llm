#include <gtest/gtest.h>

#include <cstring>
#include <functional>
#include <memory>
#include <string>

#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/RecvContext.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/TcpKVCacheRecvTask.h"
#include "rtp_llm/cpp/cache/connector/p2p/transfer/tcp/proto/service.pb.h"
#include "rtp_llm/cpp/utils/TimeUtil.h"
#include "rtp_llm/cpp/config/ConfigModules.h"
#include "rtp_llm/cpp/devices/DeviceFactory.h"

namespace rtp_llm::transfer::tcp {

using EC  = IKVCacheRecvTask::ErrorCode;
using RPC = ::transfer::tcp::ErrorCodePB;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Captures the done callback result for assertion.
struct DoneResult {
    RPC         code = ::transfer::tcp::ERROR_NONE;
    std::string msg;
    bool        called = false;
};

static RecvContext::DoneFn captureDone(DoneResult& out) {
    return [&out](RPC code, std::string msg) {
        out.code   = code;
        out.msg    = std::move(msg);
        out.called = true;
    };
}

// Block specification for building a TransferRequestPB: {len, fill_byte}.
struct BlockSpec {
    uint32_t len;
    char     fill;
};

// Build a TransferRequestPB whose block content is filled with the given byte.
static ::transfer::tcp::TransferRequestPB makeRequest(const std::string&                               unique_key,
                                                      int64_t                                          deadline_ms,
                                                      const std::map<int64_t, std::vector<BlockSpec>>& blocks_map) {
    ::transfer::tcp::TransferRequestPB req;
    req.set_unique_key(unique_key);
    req.set_deadline_ms(deadline_ms);
    for (const auto& [key, specs] : blocks_map) {
        auto* kb = req.add_blocks();
        kb->set_key(key);
        for (const auto& s : specs) {
            auto* b = kb->add_blocks();
            b->set_len(s.len);
            b->set_content(std::string(s.len, s.fill));
        }
    }
    return req;
}

static BlockInfo makeCpuBlock(void* addr, size_t size_bytes) {
    BlockInfo b;
    b.is_cuda      = false;
    b.device_index = 0;
    b.scalar_type  = 0;
    b.addr         = addr;
    b.size_bytes   = size_bytes;
    return b;
}

static BlockInfo makeGpuBlock(void* addr, size_t size_bytes) {
    BlockInfo b;
    b.is_cuda      = true;
    b.device_index = 0;
    b.scalar_type  = 0;
    b.addr         = addr;
    b.size_bytes   = size_bytes;
    return b;
}

static std::shared_ptr<TcpKVCacheRecvTask> makeTask(KeyBlockInfosPtr block_info, int64_t deadline_offset_ms = 5000) {
    return std::make_shared<TcpKVCacheRecvTask>("test_key", block_info, currentTimeMs() + deadline_offset_ms);
}

// =====================================================================
// Group A: bufferMatch
// Private method, accessible via -fno-access-control.
// =====================================================================

// A1: Single key, single block, exact size match → true.
TEST(RecvContextTest, A1_BufferMatch_SingleKey_SingleBlock_Match) {
    char dst[64];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 64)};
    auto task      = makeTask(expected);

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{64, 'A'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_TRUE(ctx.bufferMatch(task));
}

// A2: Multi-key, multi-block, all sizes match → true.
TEST(RecvContextTest, A2_BufferMatch_MultiKey_Match) {
    char dst_a[32], dst_b[16], dst_c[8];
    auto expected   = std::make_shared<KeyBlockInfos>();
    (*expected)[10] = {makeCpuBlock(dst_a, 32), makeCpuBlock(dst_b, 16)};
    (*expected)[20] = {makeCpuBlock(dst_c, 8)};
    auto task       = makeTask(expected);

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{10, {{32, 'X'}, {16, 'Y'}}}, {20, {{8, 'Z'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_TRUE(ctx.bufferMatch(task));
}

// A3: Request has MORE keys than expected, all expected keys are covered → true.
// Sender may attach extra cache keys; receiver only pulls what it expects.
TEST(RecvContextTest, A3_BufferMatch_RequestMoreKeys_AllExpectedCovered) {
    char dst[32];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 32)};
    auto task      = makeTask(expected);

    // request has key=1 (in expected) and key=2 (extra, not in expected)
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'A'}}}, {2, {{32, 'B'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_TRUE(ctx.bufferMatch(task));
}

// A4: Key count mismatch — request has 1 key, expected has 2 → false.
TEST(RecvContextTest, A4_BufferMatch_KeyCount_MoreInExpected) {
    char dst_a[32], dst_b[32];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst_a, 32)};
    (*expected)[2] = {makeCpuBlock(dst_b, 32)};
    auto task      = makeTask(expected);

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'A'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_FALSE(ctx.bufferMatch(task));
}

// A5: Request has only keys NOT in expected — expected key=1 is never matched.
// seen_keys remains empty; seen_keys.size() (0) != expected->size() (1) → false.
TEST(RecvContextTest, A5_BufferMatch_ExpectedKey_MissingFromRequest) {
    char dst[32];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 32)};  // key=1 expected
    auto task      = makeTask(expected);

    // request only has key=99; expected key=1 is absent
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{99, {{32, 'A'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_FALSE(ctx.bufferMatch(task));
}

// A6: Request provides fewer blocks than expected for a given key → false.
TEST(RecvContextTest, A6_BufferMatch_RequestBlockCount_LessThanExpected) {
    char dst_a[32], dst_b[32];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst_a, 32), makeCpuBlock(dst_b, 32)};  // expect 2 blocks
    auto task      = makeTask(expected);

    // request only sends 1 block for key=1
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'A'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_FALSE(ctx.bufferMatch(task));
}

// A7: Block size in request != expected size → false.
TEST(RecvContextTest, A7_BufferMatch_BlockSize_Mismatch) {
    char dst[32];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 32)};  // expect 32 bytes
    auto task      = makeTask(expected);

    // request sends 16 bytes
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{16, 'A'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_FALSE(ctx.bufferMatch(task));
}

// A8: Request sends MORE blocks than expected for a key — extra blocks are ignored → true.
// Sender is allowed to send a superset; receiver only pulls what it expects.
TEST(RecvContextTest, A8_BufferMatch_RequestBlockCount_MoreThanExpected) {
    char dst[32];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 32)};  // expect 1 block (32 bytes)
    auto task      = makeTask(expected);

    // request sends 3 blocks; only first is checked against expected
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'A'}, {64, 'B'}, {128, 'C'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_TRUE(ctx.bufferMatch(task));
}

// A9: Request has extra keys AND is missing one expected key → false.
// Even though request key count > expected, the missing expected key means not all
// expected keys are covered; seen_keys.size() != expected->size() → false.
TEST(RecvContextTest, A9_BufferMatch_RequestMoreKeys_ButMissingOneExpectedKey) {
    char dst_a[32], dst_b[32];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst_a, 32)};  // expected key=1
    (*expected)[2] = {makeCpuBlock(dst_b, 32)};  // expected key=2
    auto task      = makeTask(expected);

    // request has key=1 and key=99 (extra), but NOT key=2
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'A'}}}, {99, {{32, 'X'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_FALSE(ctx.bufferMatch(task));
}

// =====================================================================
// GPU test fixture
// Initializes the device once per suite via SetUpTestSuite.
// =====================================================================

class RecvContextGpuTest: public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        rtp_llm::ParallelismConfig    parallelism_config;
        rtp_llm::ModelConfig          model_config;
        rtp_llm::EPLBConfig           eplb_config;
        rtp_llm::FMHAConfig           fmha_config;
        rtp_llm::DeviceResourceConfig device_resource_config;
        device_resource_config.device_reserve_memory_bytes = 2048000000;
        device_resource_config.host_reserve_memory_bytes   = 2048000000;
        rtp_llm::MoeConfig                   moe_config;
        rtp_llm::SpeculativeExecutionConfig  sp_config;
        rtp_llm::MiscellaneousConfig         misc_config;
        rtp_llm::ProfilingDebugLoggingConfig profiling_debug_logging_config;
        rtp_llm::HWKernelConfig              hw_kernel_config;
        rtp_llm::ConcurrencyConfig           concurrency_config;
        rtp_llm::FfnDisAggregateConfig       ffn_disaggregate_config;
        rtp_llm::RuntimeConfig               runtime_config;
        rtp_llm::ModelSpecificConfig         model_specific_config;

        rtp_llm::DeviceFactory::initDevices(parallelism_config,
                                            model_config,
                                            eplb_config,
                                            fmha_config,
                                            device_resource_config,
                                            moe_config,
                                            sp_config,
                                            misc_config,
                                            profiling_debug_logging_config,
                                            hw_kernel_config,
                                            concurrency_config,
                                            ffn_disaggregate_config,
                                            runtime_config,
                                            model_specific_config,
                                            rtp_llm::NcclCommConfig{});
        device_ = rtp_llm::DeviceFactory::getDefaultDevice();
    }

    // Allocate a TYPE_UINT8 device buffer of given byte count.
    rtp_llm::BufferPtr allocGpu(size_t bytes) {
        return device_->allocateBuffer({rtp_llm::DataType::TYPE_UINT8, {bytes}, rtp_llm::AllocationType::DEVICE}, {});
    }

    // Copy GPU buffer contents to a host vector for byte-level assertion.
    std::vector<char> readGpu(const rtp_llm::BufferPtr& gpu_buf) {
        const size_t bytes = gpu_buf->size();
        auto         host_buf =
            device_->allocateBuffer({rtp_llm::DataType::TYPE_UINT8, {bytes}, rtp_llm::AllocationType::HOST}, {});
        device_->copy({*host_buf, *gpu_buf});
        device_->syncAndCheck();
        return std::vector<char>(static_cast<const char*>(host_buf->data()),
                                 static_cast<const char*>(host_buf->data()) + bytes);
    }

    static rtp_llm::DeviceBase* device_;
};

rtp_llm::DeviceBase* RecvContextGpuTest::device_ = nullptr;

// =====================================================================
// Group B: copyBuffer
// Private method, accessible via -fno-access-control.
// =====================================================================

// B1: Single CPU block — data is memcpy'd into the destination buffer.
TEST(RecvContextTest, B1_CopyBuffer_CPU_SingleBlock_DataVerified) {
    char dst[64];
    memset(dst, 0, sizeof(dst));

    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 64)};
    auto task      = makeTask(expected);

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{64, 'P'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    EXPECT_TRUE(ctx.copyBuffer(task));

    for (size_t i = 0; i < 64; ++i) {
        EXPECT_EQ(dst[i], 'P') << "byte " << i << " mismatch";
    }
}

// B2: Multi-key, multi-block GPU copy via CudaCopyUtil::batchCopyToDevice.
// Source data lives in the TransferRequestPB content (CPU); destinations are GPU buffers.
// After copyBuffer, GPU data is read back to host for verification.
TEST_F(RecvContextGpuTest, B2_CopyBuffer_GPU_MultiKey_MultiBlock_DataVerified) {
    // key=10: 2 GPU blocks (32B filled 'X', 16B filled 'Y')
    // key=20: 1 GPU block  (8B  filled 'Z')
    auto gpu_a = allocGpu(32);
    auto gpu_b = allocGpu(16);
    auto gpu_c = allocGpu(8);

    auto expected   = std::make_shared<KeyBlockInfos>();
    (*expected)[10] = {makeGpuBlock(gpu_a->data(), 32), makeGpuBlock(gpu_b->data(), 16)};
    (*expected)[20] = {makeGpuBlock(gpu_c->data(), 8)};
    auto task       = makeTask(expected);

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{10, {{32, 'X'}, {16, 'Y'}}}, {20, {{8, 'Z'}}}});
    DoneResult  done;
    auto        cuda_copy_util = std::make_shared<CudaCopyUtil>();
    RecvContext ctx(&req, captureDone(done), cuda_copy_util);

    EXPECT_TRUE(ctx.copyBuffer(task));

    EXPECT_EQ(readGpu(gpu_a), std::vector<char>(32, 'X'));
    EXPECT_EQ(readGpu(gpu_b), std::vector<char>(16, 'Y'));
    EXPECT_EQ(readGpu(gpu_c), std::vector<char>(8, 'Z'));
}

// =====================================================================
// Group C: run()
// Tests the full public path through bufferMatch → copyBuffer → task state.
// =====================================================================

// C1: null task → done called with NO_RECV_TASK.
TEST(RecvContextTest, C1_Run_NullTask_Done_NoRecvTask) {
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{8, 'A'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    ctx.run(nullptr);

    EXPECT_TRUE(done.called);
    EXPECT_EQ(done.code, RPC::NO_RECV_TASK);
}

// C2: Buffer mismatch — task is marked BUFFER_MISMATCH and done(BUFFER_MISMATCH).
TEST(RecvContextTest, C2_Run_BufferMismatch_TaskFailed) {
    char dst[16];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 16)};  // expect 16 bytes
    auto task      = makeTask(expected);

    // request sends 32 bytes → size mismatch
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'A'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    ctx.run(task);

    EXPECT_TRUE(done.called);
    EXPECT_EQ(done.code, RPC::BUFFER_MISMATCH);

    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::BUFFER_MISMATCH);
}

// C3: CPU copy succeeds — done(ERROR_NONE), task SUCCESS, data copied into dst buffer.
TEST(RecvContextTest, C3_Run_CpuCopy_Success_DataVerified) {
    char dst[64];
    memset(dst, 0, sizeof(dst));

    auto expected   = std::make_shared<KeyBlockInfos>();
    (*expected)[42] = {makeCpuBlock(dst, 64)};
    auto task       = makeTask(expected);

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{42, {{64, 'Q'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    ctx.run(task);

    EXPECT_TRUE(done.called);
    EXPECT_EQ(done.code, RPC::ERROR_NONE);

    auto result = task->getResult();
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.error_code, EC::NONE_ERROR);

    EXPECT_EQ(std::string(dst, 64), std::string(64, 'Q'));
}

// C4: GPU block + null cuda_copy_util → copyBuffer fails → task RECV_FAILED, done(FAILED).
TEST(RecvContextTest, C4_Run_GpuBlock_NullCudaCopyUtil_TaskFailed) {
    char dst[64];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeGpuBlock(dst, 64)};
    auto task      = makeTask(expected);

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{64, 'G'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);  // nullptr cuda_copy_util

    ctx.run(task);

    EXPECT_TRUE(done.called);
    EXPECT_EQ(done.code, RPC::FAILED);

    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::RECV_FAILED);
}

// C5: Task pre-cancelled (terminal before run is called).
// markProcessing returns false; run reads terminal result and done(CANCELLED).
TEST(RecvContextTest, C5_Run_PreCancelledTask_Done_Cancelled) {
    char dst[32];
    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 32)};
    auto task      = makeTask(expected);
    task->cancel("pre-cancel");  // task becomes terminal

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'A'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    ctx.run(task);

    EXPECT_TRUE(done.called);
    EXPECT_EQ(done.code, RPC::CANCELLED);

    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::CANCELLED);
    EXPECT_EQ(result.error_message, "pre-cancel");
}

// C6: Task deadline already past when markSuccess is called inside run.
// copyBuffer succeeds, but markSuccess detects currentTimeMs() > deadline_ms_ → TIMEOUT.
// done(TIMEOUT) and task result is TIMEOUT.
TEST(RecvContextTest, C6_Run_MarkSuccess_DetectsTaskTimeout) {
    char dst[32];
    memset(dst, 0, sizeof(dst));

    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 32)};
    // Task deadline is already in the past.
    auto task = std::make_shared<TcpKVCacheRecvTask>("test_key", expected, currentTimeMs() - 1);

    // Context deadline is far in the future so isTimeout() won't interfere.
    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'T'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    ctx.run(task);

    EXPECT_TRUE(done.called);
    EXPECT_EQ(done.code, RPC::TIMEOUT);

    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::TIMEOUT);
}

// C7: Task has cancel_requested flag set while in processing state.
// Sequence: markProcessing() → cancel() (sets flag, not terminal) → run() →
//   run calls markProcessing() again (not terminal, returns true) →
//   copyBuffer succeeds → markSuccess detects cancel_requested_ → CANCELLED.
TEST(RecvContextTest, C7_Run_CancelWhileProcessing_MarkSuccess_Done_Cancelled) {
    char dst[32];
    memset(dst, 0, sizeof(dst));

    auto expected  = std::make_shared<KeyBlockInfos>();
    (*expected)[1] = {makeCpuBlock(dst, 32)};
    auto task      = makeTask(expected);

    // Put task into processing state, then cancel (sets flag without terminating).
    task->markProcessing();
    task->cancel("cancel-while-processing");
    // Task is NOT terminal yet (cancel while processing only sets the flag).
    EXPECT_FALSE(task->waitUntil(0));

    auto        req = makeRequest("k", currentTimeMs() + 5000, {{1, {{32, 'C'}}}});
    DoneResult  done;
    RecvContext ctx(&req, captureDone(done), nullptr);

    // run() calls markProcessing again (processing_=true but not terminal → returns true),
    // copyBuffer succeeds, markSuccess sees cancel_requested_ → CANCELLED.
    ctx.run(task);

    EXPECT_TRUE(done.called);
    EXPECT_EQ(done.code, RPC::CANCELLED);

    auto result = task->getResult();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_code, EC::CANCELLED);
    EXPECT_EQ(result.error_message, "cancel-while-processing");
}

}  // namespace rtp_llm::transfer::tcp
