package com.github.feng111627.asyncingest.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.feng111627.asyncingest.config.IngestProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 熔断冷却恢复、运行指标统计与空闲键回收的场景测试。
 * 每个用例打印 [SCENARIO]/[PASS] 日志便于核对覆盖情况。
 */
class IngestServiceRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(IngestServiceRecoveryTest.class);

    private InMemoryDownstreamStore store;
    private IngestService service;

    private IngestService newService(IngestProperties props) {
        store = new InMemoryDownstreamStore();
        service = new IngestService(props, store);
        return service;
    }

    private IngestProperties baseProps() {
        IngestProperties props = new IngestProperties();
        props.setMaxPending(100);
        props.setMaxRetries(1); // 共2次尝试，缩短用例耗时
        props.setRetryBackoff(Duration.ofMillis(10));
        props.setWorkerThreads(8);
        props.setShutdownTimeout(Duration.ofSeconds(5));
        props.setCircuitCooldown(Duration.ofMillis(200));
        return props;
    }

    @AfterEach
    void tearDown() {
        if (service != null) {
            service.shutdown();
        }
    }

    private static WriteRequest req(String key, int seq) {
        return new WriteRequest(key + "-" + seq, key, "payload-" + seq);
    }

    private static WriteOutcome await(SubmitResult result) {
        SubmitResult.Accepted accepted = assertInstanceOf(SubmitResult.Accepted.class, result);
        try {
            return accepted.outcome().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("outcome not completed in time", e);
        }
    }

    private static void waitFor(BooleanSupplier cond, String what) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for " + what);
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted waiting for " + what);
            }
        }
    }

    @Test
    @DisplayName("场景10: 熔断冷却到期后半开试探，试探成功则该键恢复，后续请求照常处理")
    void probeSuccessAfterCooldownRecoversKey() throws Exception {
        log.info("[SCENARIO] 熔断 -> 冷却200ms -> 半开只放一条试探(并发的第二条仍被拒) -> 试探成功 -> 键恢复");
        newService(baseProps());
        store.failNextTimes(2); // 首次请求 2 次尝试全部失败

        WriteOutcome failed = await(service.submit(req("recover-key", 0)));
        assertEquals(OutcomeStatus.FAILED_RETRY_EXHAUSTED, failed.status());

        // 熔断中：快速失败，且键状态可查
        KeyStatus blocked = service.keyStatus("recover-key").orElseThrow();
        assertTrue(blocked.circuitBlocked(), "熔断后键应被挡住");
        assertFalse(blocked.circuitHalfOpen());
        assertTrue(blocked.cooldownRemainingMillis() > 0, "应报告冷却剩余时间");
        assertEquals(0, blocked.successCount());
        SubmitResult.Rejected rejected = assertInstanceOf(SubmitResult.Rejected.class,
                service.submit(req("recover-key", 1)));
        assertEquals(RejectReason.KEY_CIRCUIT_OPEN, rejected.reason());
        assertEquals(2, store.writeLog().size(), "冷却期间不应再调用下游");

        // 冷却到期：下一条成为试探；试探在途期间其它提交仍快速失败（只放一条）
        Thread.sleep(250);
        store.setLatencyMillis(300); // 让试探在途一段时间，便于验证半开只放一条
        SubmitResult probe = service.submit(req("recover-key", 2));
        assertInstanceOf(SubmitResult.Accepted.class, probe);
        SubmitResult.Rejected duringProbe = assertInstanceOf(SubmitResult.Rejected.class,
                service.submit(req("recover-key", 3)));
        assertEquals(RejectReason.KEY_CIRCUIT_OPEN, duringProbe.reason(),
                "半开试探在途时其它提交应继续快速失败");

        WriteOutcome probeOutcome = await(probe);
        assertEquals(OutcomeStatus.SUCCESS, probeOutcome.status());
        store.setLatencyMillis(0);

        // 试探成功：键恢复，后续请求照常处理，顺序不乱
        KeyStatus recovered = service.keyStatus("recover-key").orElseThrow();
        assertFalse(recovered.circuitBlocked(), "试探成功后键应恢复");
        assertEquals(0, recovered.cooldownRemainingMillis());
        assertEquals(1, recovered.successCount());
        for (int i = 4; i < 7; i++) {
            assertEquals(OutcomeStatus.SUCCESS, await(service.submit(req("recover-key", i))).status());
        }
        assertEquals(4, service.keyStatus("recover-key").orElseThrow().successCount());
        log.info("[PASS] 冷却恢复: 半开试探成功, 键恢复正常, successCount=4, 全局统计={}", service.globalStats());
    }

    @Test
    @DisplayName("场景11: 半开试探仍失败则重新熔断继续挡，不堆更多失败请求；再次冷却后仍可恢复")
    void probeFailureReopensCircuitAndRecoversLater() throws Exception {
        log.info("[SCENARIO] 熔断 -> 冷却后试探仍失败 -> 重新熔断且下游调用不增加 -> 再次冷却后试探成功恢复");
        newService(baseProps());
        store.failNextTimes(2);
        assertEquals(OutcomeStatus.FAILED_RETRY_EXHAUSTED, await(service.submit(req("flaky-key", 0))).status());
        assertTrue(service.keyStatus("flaky-key").orElseThrow().circuitBlocked());

        // 冷却到期，但下游还没恢复：试探再失败 2 次，重新熔断
        store.failNextTimes(2);
        Thread.sleep(250);
        WriteOutcome probeFailed = await(service.submit(req("flaky-key", 1)));
        assertEquals(OutcomeStatus.FAILED_RETRY_EXHAUSTED, probeFailed.status());
        KeyStatus reopened = service.keyStatus("flaky-key").orElseThrow();
        assertTrue(reopened.circuitBlocked(), "试探失败后应重新挡住");
        assertFalse(reopened.circuitHalfOpen());
        assertTrue(reopened.cooldownRemainingMillis() > 0, "冷却应重新计时");
        assertEquals(4, store.writeLog().size(), "试探只放一条, 不应往下游堆更多失败请求");

        // 重新熔断期间继续快速失败，下游调用数不变
        SubmitResult.Rejected rejected = assertInstanceOf(SubmitResult.Rejected.class,
                service.submit(req("flaky-key", 2)));
        assertEquals(RejectReason.KEY_CIRCUIT_OPEN, rejected.reason());
        assertEquals(4, store.writeLog().size());

        // 再次冷却到期，下游已恢复：试探成功，键恢复
        Thread.sleep(250);
        assertEquals(OutcomeStatus.SUCCESS, await(service.submit(req("flaky-key", 3))).status());
        assertFalse(service.keyStatus("flaky-key").orElseThrow().circuitBlocked());
        assertEquals(OutcomeStatus.SUCCESS, await(service.submit(req("flaky-key", 4))).status());
        assertEquals(6, store.writeLog().size(), "恢复后正常调用下游");
        log.info("[PASS] 试探失败重挡: 重新熔断期间下游0调用, 再次冷却后恢复, 全局统计={}", service.globalStats());
    }

    @Test
    @DisplayName("场景12: 并发提交下全局与每键统计准确：接收+拒绝=总提交，成功=接收，拒绝按原因区分")
    void concurrentSubmitStatsAreAccurate() throws Exception {
        log.info("[SCENARIO] 8线程x50条并发提交+下游慢+maxPending=20 -> 接收/成功/拒绝计数必须精确对上");
        IngestProperties props = baseProps();
        props.setMaxPending(20);
        props.setWorkerThreads(4);
        newService(props);
        store.setLatencyMillis(20);

        int threads = 8;
        int perThread = 50;
        AtomicInteger acceptedCount = new AtomicInteger();
        AtomicInteger rejectedCount = new AtomicInteger();
        List<Thread> producers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            String key = "stats-key-" + t;
            producers.add(Thread.startVirtualThread(() -> {
                for (int i = 0; i < perThread; i++) {
                    SubmitResult result = service.submit(req(key, i));
                    if (result instanceof SubmitResult.Accepted) {
                        acceptedCount.incrementAndGet();
                    } else {
                        rejectedCount.incrementAndGet();
                    }
                }
            }));
        }
        for (Thread p : producers) {
            p.join();
        }

        IngestMetrics.GlobalStats midFlight = service.globalStats();
        assertEquals(threads * perThread, midFlight.received() + midFlight.rejectedTotal(),
                "每条提交必须且只能计入接收或拒绝之一");
        assertEquals(acceptedCount.get(), midFlight.received(), "接收计数不能漏计或多计");
        assertEquals(rejectedCount.get(), midFlight.rejectedTotal(), "拒绝计数不能漏计或多计");
        assertEquals(rejectedCount.get(),
                midFlight.rejectedByReason().get(RejectReason.QUEUE_FULL),
                "满员拒绝应全部计入 QUEUE_FULL");

        service.shutdown(); // 排空已接收数据
        IngestMetrics.GlobalStats drained = service.globalStats();
        assertEquals(drained.received(), drained.succeeded(), "无故障时接收数应全部成功");
        assertEquals(0, drained.failed());
        long perKeySuccessSum = service.keyStatuses().stream()
                .mapToLong(KeyStatus::successCount).sum();
        assertEquals(drained.succeeded(), perKeySuccessSum, "每键成功数之和应等于全局成功数");
        log.info("[PASS] 并发统计: 提交{}=接收{}+拒绝{}, 成功{}, 每键成功数之和={}",
                threads * perThread, drained.received(), drained.rejectedTotal(),
                drained.succeeded(), perKeySuccessSum);
    }

    @Test
    @DisplayName("场景13: 空闲键状态被自动回收，回收后再提交同一个键按新键正常处理")
    void idleKeyStateReapedAndResubmitWorks() {
        log.info("[SCENARIO] keyIdleTimeout=200ms -> 空闲键被回收 -> 再次提交同键正常成功, 状态按新键重新统计");
        IngestProperties props = baseProps();
        props.setKeyIdleTimeout(Duration.ofMillis(200));
        props.setReapInterval(Duration.ofMillis(50));
        newService(props);

        assertEquals(OutcomeStatus.SUCCESS, await(service.submit(req("idle-key", 0))).status());
        assertEquals(1, service.trackedKeyCount());
        assertEquals(1, service.keyStatus("idle-key").orElseThrow().successCount());

        waitFor(() -> service.trackedKeyCount() == 0, "idle key state to be reaped");
        assertTrue(service.keyStatus("idle-key").isEmpty(), "回收后键状态应不存在");
        assertEquals(1, service.globalStats().succeeded(), "回收不算失败也不重复计数");

        // 回收后再提交同一个键：按新键正常排队处理
        assertEquals(OutcomeStatus.SUCCESS, await(service.submit(req("idle-key", 1))).status());
        assertEquals(1, service.trackedKeyCount());
        KeyStatus reborn = service.keyStatus("idle-key").orElseThrow();
        assertEquals(1, reborn.successCount(), "回收后状态按新键重新统计");
        assertEquals(2, service.globalStats().succeeded(), "全局成功数累计不受回收影响");
        log.info("[PASS] 空闲回收: 键状态回收后再次提交正常成功, 全局统计={}", service.globalStats());
    }

    @Test
    @DisplayName("场景14: 键数超上限时回收最久未活动的空闲键，在途键不受影响")
    void maxKeysEvictsLeastRecentlyActiveIdleKeys() {
        log.info("[SCENARIO] maxKeys=2+idleTimeout很长 -> 第3个键触发回收最老的空闲键 -> 在途/新提交不受影响");
        IngestProperties props = baseProps();
        props.setMaxKeys(2);
        props.setKeyIdleTimeout(Duration.ofMinutes(10)); // 不靠空闲超时，只靠上限
        props.setReapInterval(Duration.ofMillis(50));
        newService(props);

        for (String key : List.of("k-a", "k-b", "k-c")) {
            assertEquals(OutcomeStatus.SUCCESS, await(service.submit(req(key, 0))).status());
        }
        waitFor(() -> service.trackedKeyCount() == 2, "excess idle key state to be evicted");
        assertEquals(3, service.globalStats().succeeded(), "回收不影响已完成请求的统计");

        // 被回收的键再提交仍正常；其余键不受影响
        for (String key : List.of("k-a", "k-b", "k-c")) {
            assertEquals(OutcomeStatus.SUCCESS, await(service.submit(req(key, 1))).status());
        }
        assertEquals(6, service.globalStats().succeeded());
        log.info("[PASS] 上限回收: 超限时回收至{}个键, 全部键再提交均成功, 全局统计={}",
                2, service.globalStats());
    }
}
