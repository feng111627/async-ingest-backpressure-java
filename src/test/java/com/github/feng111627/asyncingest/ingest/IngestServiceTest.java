package com.github.feng111627.asyncingest.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.feng111627.asyncingest.config.IngestProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步写入通道的场景测试。每个用例打印 [SCENARIO]/[PASS] 日志便于核对覆盖情况。
 */
class IngestServiceTest {

    private static final Logger log = LoggerFactory.getLogger(IngestServiceTest.class);

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
        props.setMaxRetries(2);
        props.setRetryBackoff(Duration.ofMillis(10));
        props.setWorkerThreads(8);
        props.setShutdownTimeout(Duration.ofSeconds(5));
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

    @Test
    @DisplayName("场景1: 多线程并发提交多个键，不丢不串、每个键内部按提交顺序生效")
    void concurrentSubmitKeepsPerKeyOrder() throws Exception {
        log.info("[SCENARIO] 4线程x4键x25条并发提交 -> 全部成功, 每个键的写入顺序与该线程提交顺序一致");
        newService(baseProps());
        int threads = 4;
        int perThread = 25;
        record Submission(String key, int seq) {
        }
        // 每个生产者线程负责一个键，线程内按 seq 顺序提交
        List<List<Submission>> submissions = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            submissions.add(new CopyOnWriteArrayList<>());
        }
        List<Thread> producers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            String key = "key-" + tid;
            producers.add(Thread.startVirtualThread(() -> {
                for (int i = 0; i < perThread; i++) {
                    SubmitResult result = service.submit(req(key, i));
                    assertInstanceOf(SubmitResult.Accepted.class, result);
                    submissions.get(tid).add(new Submission(key, i));
                }
            }));
        }
        for (Thread p : producers) {
            p.join();
        }
        // 等所有已接收请求处理完
        service.shutdown();
        assertEquals(threads * perThread, store.applied().size(), "不能丢数据");
        for (int t = 0; t < threads; t++) {
            String key = "key-" + t;
            List<String> written = store.writeLog().stream()
                    .filter(r -> r.key().equals(key))
                    .map(WriteRequest::requestId)
                    .toList();
            List<String> expected = IntStream.range(0, perThread)
                    .mapToObj(i -> key + "-" + i)
                    .toList();
            assertEquals(expected, written, "键 " + key + " 的写入顺序必须与提交顺序一致");
        }
        log.info("[PASS] 并发保序: {}线程 x {}条 = {} 条全部成功且各键有序", threads, perThread, threads * perThread);
    }

    @Test
    @DisplayName("场景2: 下游变慢时，卡住的键不拖慢其它键")
    void slowKeyDoesNotBlockOtherKeys() {
        log.info("[SCENARIO] 下游变慢(100ms/次) -> 键A积压时键B应快速完成");
        IngestProperties props = baseProps();
        newService(props);
        store.setLatencyMillis(100);

        int keyACount = 10; // 键A串行处理，约需 1s
        List<SubmitResult> keyAResults = new ArrayList<>();
        for (int i = 0; i < keyACount; i++) {
            keyAResults.add(service.submit(req("slow-key", i)));
        }
        SubmitResult keyB = service.submit(req("fast-key", 0));

        WriteOutcome outcomeB = await(keyB);
        assertEquals(OutcomeStatus.SUCCESS, outcomeB.status());
        long pendingA = keyAResults.stream()
                .map(r -> (SubmitResult.Accepted) r)
                .map(a -> a.outcome())
                .filter(f -> !f.isDone())
                .count();
        assertTrue(pendingA > 0, "键B完成时键A应仍有积压，证明键间互不阻塞");
        keyAResults.forEach(IngestServiceTest::await);
        log.info("[PASS] 键间隔离: 键A积压 {} 条期间键B已完成", pendingA);
    }

    @Test
    @DisplayName("场景3: 待处理量达上限，REJECT 策略立即拒绝并给出 QUEUE_FULL")
    void rejectStrategyRejectsWhenFull() {
        log.info("[SCENARIO] 下游慢+maxPending=2 -> 第3条提交应被 QUEUE_FULL 拒绝");
        IngestProperties props = baseProps();
        props.setMaxPending(2);
        props.setBackpressureStrategy(IngestProperties.BackpressureStrategy.REJECT);
        newService(props);
        store.setLatencyMillis(300);

        assertInstanceOf(SubmitResult.Accepted.class, service.submit(req("k", 0)));
        assertInstanceOf(SubmitResult.Accepted.class, service.submit(req("k", 1)));
        SubmitResult.Rejected rejected = assertInstanceOf(SubmitResult.Rejected.class, service.submit(req("k", 2)));
        assertEquals(RejectReason.QUEUE_FULL, rejected.reason());
        log.info("[PASS] 背压拒绝(REJECT): reason={} detail={}", rejected.reason(), rejected.detail());
    }

    @Test
    @DisplayName("场景4: BLOCK 策略排队等待，超时后以 QUEUE_FULL_TIMEOUT 拒绝")
    void blockStrategyWaitsThenRejectsOnTimeout() {
        log.info("[SCENARIO] BLOCK策略+blockTimeout=150ms -> 满员时第3条等待后超时拒绝");
        IngestProperties props = baseProps();
        props.setMaxPending(2);
        props.setBackpressureStrategy(IngestProperties.BackpressureStrategy.BLOCK);
        props.setBlockTimeout(Duration.ofMillis(150));
        newService(props);
        store.setLatencyMillis(500);

        assertInstanceOf(SubmitResult.Accepted.class, service.submit(req("k", 0)));
        assertInstanceOf(SubmitResult.Accepted.class, service.submit(req("k", 1)));
        long start = System.nanoTime();
        SubmitResult.Rejected rejected = assertInstanceOf(SubmitResult.Rejected.class, service.submit(req("k", 2)));
        long waitedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(RejectReason.QUEUE_FULL_TIMEOUT, rejected.reason());
        assertTrue(waitedMs >= 100, "BLOCK 策略应确实等待了一段时间, waited=" + waitedMs + "ms");
        log.info("[PASS] 背压拒绝(BLOCK): 等待 {}ms 后 reason={}", waitedMs, rejected.reason());
    }

    @Test
    @DisplayName("场景5: 重复提交同一 requestId，幂等只生效一次")
    void duplicateRequestIdAppliedOnce() {
        log.info("[SCENARIO] 同一 requestId 提交两次 -> 下游只生效一次，两次都返回成功");
        newService(baseProps());
        WriteRequest dup = new WriteRequest("dup-1", "dup-key", "v1");
        WriteOutcome first = await(service.submit(dup));
        WriteOutcome second = await(service.submit(dup));
        assertEquals(OutcomeStatus.SUCCESS, first.status());
        assertEquals(OutcomeStatus.SUCCESS, second.status());
        assertEquals(1, store.applied().size(), "同一 requestId 只能生效一次");
        assertEquals(2, store.writeLog().size(), "两次提交都到达了下游，但靠幂等去重");
        log.info("[PASS] 幂等去重: 提交2次, 下游调用{}次, 生效{}次", store.writeLog().size(), store.applied().size());
    }

    @Test
    @DisplayName("场景6: 下游持续失败，重试耗尽后熔断，该键后续请求快速失败")
    void retriesExhaustedOpensCircuitForKey() {
        log.info("[SCENARIO] 下游连续失败 -> 首次请求重试耗尽, 该键后续提交 KEY_CIRCUIT_OPEN");
        IngestProperties props = baseProps();
        props.setMaxRetries(2); // 共3次尝试
        newService(props);
        store.failNextTimes(3);

        WriteOutcome failed = await(service.submit(req("bad-key", 0)));
        assertEquals(OutcomeStatus.FAILED_RETRY_EXHAUSTED, failed.status());
        assertEquals(3, store.writeLog().size(), "首次尝试+2次重试=3次下游调用");

        SubmitResult.Rejected rejected = assertInstanceOf(SubmitResult.Rejected.class,
                service.submit(req("bad-key", 1)));
        assertEquals(RejectReason.KEY_CIRCUIT_OPEN, rejected.reason());
        assertEquals(3, store.writeLog().size(), "熔断后不应再调用下游");

        // 其它键不受影响
        WriteOutcome other = await(service.submit(req("good-key", 0)));
        assertEquals(OutcomeStatus.SUCCESS, other.status());
        log.info("[PASS] 重试熔断: 3次尝试耗尽, 后续提交快速失败, 其它键正常");
    }

    @Test
    @DisplayName("场景7: 重试窗口内下游恢复，重试后成功")
    void retrySucceedsWhenDownstreamRecovers() {
        log.info("[SCENARIO] 下游仅前2次失败 -> 第3次尝试成功, 结果为 SUCCESS");
        IngestProperties props = baseProps();
        props.setMaxRetries(3);
        newService(props);
        store.failNextTimes(2);

        WriteOutcome outcome = await(service.submit(req("flaky-key", 0)));
        assertEquals(OutcomeStatus.SUCCESS, outcome.status());
        assertEquals(3, store.writeLog().size());
        log.info("[PASS] 重试成功: 第3次尝试写入成功");
    }

    @Test
    @DisplayName("场景8: 优雅关闭，已接收数据在超时内全部处理完")
    void gracefulShutdownDrainsAccepted() {
        log.info("[SCENARIO] 提交5条后立即关闭 -> 全部处理完, leftover为空, 新提交被 SHUTTING_DOWN 拒绝");
        IngestProperties props = baseProps();
        props.setShutdownTimeout(Duration.ofSeconds(10));
        newService(props);
        store.setLatencyMillis(50);

        List<SubmitResult> results = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            results.add(service.submit(req("drain-key", i)));
        }
        List<WriteRequest> leftovers = service.shutdown();
        assertTrue(leftovers.isEmpty(), "超时时间内应排空, leftovers=" + leftovers);
        assertTrue(results.stream().map(IngestServiceTest::await)
                .allMatch(o -> o.status() == OutcomeStatus.SUCCESS));
        assertEquals(5, store.applied().size());

        SubmitResult.Rejected rejected = assertInstanceOf(SubmitResult.Rejected.class,
                service.submit(req("drain-key", 99)));
        assertEquals(RejectReason.SHUTTING_DOWN, rejected.reason());
        log.info("[PASS] 优雅关闭: 5条全部排空, 关闭后新提交被拒绝(SHUTTING_DOWN)");
    }

    @Test
    @DisplayName("场景9: 关闭超时，明确报告未处理完的请求，其余请求结果明确")
    void shutdownTimeoutReportsLeftovers() {
        log.info("[SCENARIO] 下游很慢+shutdownTimeout=200ms -> toBeReported");
        IngestProperties props = baseProps();
        props.setMaxPending(10);
        props.setShutdownTimeout(Duration.ofMillis(200));
        newService(props);
        store.setLatencyMillis(2_000);

        List<SubmitResult> results = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            results.add(service.submit(req("stuck-key", i)));
        }
        List<WriteRequest> leftovers = service.shutdown();
        assertEquals(3, leftovers.size(), "1条处理中+2条排队, 都未来得及完成, 应全部报告");
        // 排队中的请求应被显式标记为 NOT_PROCESSED_SHUTDOWN_TIMEOUT
        List<WriteOutcome> outcomes = results.stream().map(IngestServiceTest::await).toList();
        long notProcessed = outcomes.stream()
                .filter(o -> o.status() == OutcomeStatus.NOT_PROCESSED_SHUTDOWN_TIMEOUT).count();
        assertEquals(2, notProcessed, "排队未处理的2条应有明确结果");
        assertTrue(store.applied().size() <= 1, "不能出现写了一半: 每条写入要么完整生效要么不生效");
        log.info("[PASS] 关闭超时报告: leftovers={}, 排队请求结果=NOT_PROCESSED_SHUTDOWN_TIMEOUT", leftovers.size());
    }

    @Test
    @DisplayName("场景10: 熔断冷却结束后放行单条试探，试探成功则该键恢复")
    void circuitHalfOpenProbeSuccessRecoversKey() {
        log.info("[SCENARIO] 熔断+cooldown=200ms -> 冷却期内快速失败, 冷却后第1条为试探且成功, 键恢复");
        IngestProperties props = baseProps();
        props.setMaxRetries(2);
        props.setCircuitCooldown(Duration.ofMillis(200));
        newService(props);
        store.failNextTimes(3); // 首次请求 1+2 次尝试全部失败 -> 熔断

        WriteOutcome failed = await(service.submit(req("recover-key", 0)));
        assertEquals(OutcomeStatus.FAILED_RETRY_EXHAUSTED, failed.status());

        // 冷却期内：快速失败，不调用下游
        SubmitResult.Rejected cooling = assertInstanceOf(SubmitResult.Rejected.class,
                service.submit(req("recover-key", 1)));
        assertEquals(RejectReason.KEY_CIRCUIT_OPEN, cooling.reason());
        assertEquals(3, store.writeLog().size(), "冷却期内不应再调用下游");
        KeyStats coolingStats = keyStatsOf("recover-key");
        assertTrue(coolingStats.circuitOpen(), "熔断中");
        assertTrue(coolingStats.circuitRetryAfterMillis() > 0, "应报告剩余冷却时间");

        // 冷却结束：第一条提交作为试探被放行，下游已恢复 -> 试探成功 -> 键恢复
        sleep(250);
        WriteOutcome probe = await(service.submit(req("recover-key", 2)));
        assertEquals(OutcomeStatus.SUCCESS, probe.status(), "试探请求应成功");
        assertEquals(4, store.writeLog().size(), "试探只放行了一条去下游");

        // 恢复后：后续请求照常处理
        WriteOutcome after = await(service.submit(req("recover-key", 3)));
        assertEquals(OutcomeStatus.SUCCESS, after.status());
        KeyStats recovered = keyStatsOf("recover-key");
        assertTrue(!recovered.circuitOpen(), "试探成功后键应恢复");
        assertEquals(2, recovered.succeeded(), "该键成功数=试探1条+恢复后1条");
        log.info("[PASS] 冷却试探恢复: 试探成功, 键恢复, 后续请求正常, keyStats={}", recovered);
    }

    @Test
    @DisplayName("场景11: 试探仍失败则继续熔断，且不向下游堆积更多请求")
    void circuitHalfOpenProbeFailureStaysOpen() {
        log.info("[SCENARIO] 熔断+冷却后试探仍失败 -> 重新冷却, 后续提交继续快速失败, 下游调用数不增加");
        IngestProperties props = baseProps();
        props.setMaxRetries(2);
        props.setCircuitCooldown(Duration.ofMillis(200));
        newService(props);
        store.failNextTimes(3);

        WriteOutcome failed = await(service.submit(req("still-bad-key", 0)));
        assertEquals(OutcomeStatus.FAILED_RETRY_EXHAUSTED, failed.status());
        assertEquals(3, store.writeLog().size());

        // 冷却结束但下游仍然故障：试探请求 3 次尝试全部失败 -> 重新熔断
        sleep(250);
        store.failNextTimes(3);
        WriteOutcome probe = await(service.submit(req("still-bad-key", 1)));
        assertEquals(OutcomeStatus.FAILED_RETRY_EXHAUSTED, probe.status(), "试探请求本身应明确失败");
        assertEquals(6, store.writeLog().size(), "试探只增加了3次尝试, 没有放更多请求下去");

        // 重新冷却：后续提交继续快速失败，下游调用数不变
        SubmitResult.Rejected again = assertInstanceOf(SubmitResult.Rejected.class,
                service.submit(req("still-bad-key", 2)));
        assertEquals(RejectReason.KEY_CIRCUIT_OPEN, again.reason());
        assertEquals(6, store.writeLog().size(), "重新熔断后不应再调用下游");
        KeyStats stats = keyStatsOf("still-bad-key");
        assertTrue(stats.circuitOpen(), "试探失败后应继续熔断");
        assertTrue(stats.circuitRetryAfterMillis() > 0, "应进入新的冷却期");

        // 其它键不受影响
        WriteOutcome other = await(service.submit(req("unaffected-key", 0)));
        assertEquals(OutcomeStatus.SUCCESS, other.status());
        log.info("[PASS] 试探失败续熔: 下游调用数={} 不再增长, 其它键正常", store.writeLog().size());
    }

    @Test
    @DisplayName("场景12: 并发提交下全局与每键统计准确，拒绝按原因分别计数")
    void concurrentSubmitKeepsAccurateStats() throws Exception {
        log.info("[SCENARIO] 并发成功+并发拒绝+失败熔断混合 -> 各项计数精确等于实际发生数");
        IngestProperties props = baseProps();
        props.setMaxPending(4);
        props.setMaxRetries(2);
        props.setCircuitCooldown(Duration.ofMinutes(1));
        newService(props);

        // 阶段1: 4线程x4键x25条并发提交, 全部成功
        int threads = 4, perThread = 25;
        List<Thread> producers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final String key = "stats-key-" + t;
            producers.add(Thread.startVirtualThread(() -> {
                for (int i = 0; i < perThread; i++) {
                    await(service.submit(req(key, i)));
                }
            }));
        }
        for (Thread p : producers) {
            p.join();
        }
        IngestStats afterSuccess = service.stats();
        assertEquals(100, afterSuccess.received(), "接收数");
        assertEquals(100, afterSuccess.succeeded(), "成功数");
        assertEquals(0, afterSuccess.failed(), "失败数");
        assertEquals(0, afterSuccess.rejectedTotal(), "拒绝数");
        assertEquals(0, afterSuccess.pending(), "全部处理完, 待处理应为0");
        for (int t = 0; t < threads; t++) {
            assertEquals(perThread, keyStatsOf("stats-key-" + t).succeeded(),
                    "每键成功数应精确等于提交数");
        }

        // 阶段2: 下游变慢占满许可, 20个并发提交应全部被 QUEUE_FULL 拒绝
        store.setLatencyMillis(500);
        List<SubmitResult> slowOnes = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            slowOnes.add(service.submit(req("slow-key", i)));
        }
        List<SubmitResult> rejectedResults = new CopyOnWriteArrayList<>();
        List<Thread> rejecters = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            rejecters.add(Thread.startVirtualThread(() -> {
                for (int i = 0; i < 5; i++) {
                    rejectedResults.add(service.submit(req("overflow-key", i)));
                }
            }));
        }
        for (Thread r : rejecters) {
            r.join();
        }
        assertEquals(20, rejectedResults.size());
        assertTrue(rejectedResults.stream().allMatch(r -> r instanceof SubmitResult.Rejected rejected
                && rejected.reason() == RejectReason.QUEUE_FULL), "占满后并发提交应全部 QUEUE_FULL");
        assertEquals(20, service.stats().rejectedByReason().get(RejectReason.QUEUE_FULL),
                "QUEUE_FULL 拒绝数应精确为20");
        slowOnes.forEach(IngestServiceTest::await); // 等许可释放
        store.setLatencyMillis(0);

        // 阶段3: 下游故障 -> 1条重试耗尽失败 + 1条熔断拒绝
        store.failNextTimes(3);
        WriteOutcome failed = await(service.submit(req("bad-key", 0)));
        assertEquals(OutcomeStatus.FAILED_RETRY_EXHAUSTED, failed.status());
        SubmitResult.Rejected circuitRejected = assertInstanceOf(SubmitResult.Rejected.class,
                service.submit(req("bad-key", 1)));
        assertEquals(RejectReason.KEY_CIRCUIT_OPEN, circuitRejected.reason());

        IngestStats stats = service.stats();
        assertEquals(105, stats.received(), "接收总数=100+4+1");
        assertEquals(104, stats.succeeded(), "成功总数=100+4");
        assertEquals(1, stats.failed(), "失败总数=1");
        assertEquals(21, stats.rejectedTotal(), "拒绝总数=20(QUEUE_FULL)+1(KEY_CIRCUIT_OPEN)");
        assertEquals(1, stats.rejectedByReason().get(RejectReason.KEY_CIRCUIT_OPEN));
        assertEquals(0, stats.rejectedByReason().get(RejectReason.SHUTTING_DOWN));
        assertEquals(0, stats.rejectedByReason().get(RejectReason.QUEUE_FULL_TIMEOUT));
        KeyStats badKey = keyStatsOf("bad-key");
        assertTrue(badKey.circuitOpen(), "bad-key 应处于熔断状态");
        assertTrue(badKey.circuitRetryAfterMillis() > 0, "应报告剩余冷却时间");
        log.info("[PASS] 并发统计准确: received={} succeeded={} failed={} rejected={} 分类={}",
                stats.received(), stats.succeeded(), stats.failed(), stats.rejectedTotal(),
                stats.rejectedByReason());
    }

    @Test
    @DisplayName("场景13: 空闲键状态被自动回收，回收后同键再提交仍正常处理")
    void idleKeyStateEvictedAndResubmitWorks() {
        log.info("[SCENARIO] idleTimeout=200ms -> 空闲键状态被回收, 不计为失败/丢失, 再提交按新键正常处理");
        IngestProperties props = baseProps();
        props.setKeyIdleTimeout(Duration.ofMillis(200));
        props.setEvictionInterval(Duration.ofMillis(50));
        newService(props);

        WriteOutcome first = await(service.submit(req("ephemeral-key", 0)));
        assertEquals(OutcomeStatus.SUCCESS, first.status());
        assertEquals(1, service.stats().trackedKeys(), "提交后应有该键的运行状态");
        assertEquals(1, keyStatsOf("ephemeral-key").succeeded());

        // 等待回收器清理空闲键
        waitUntil(() -> service.stats().trackedKeys() == 0, 5_000,
                "空闲键状态应在超时后被回收");
        assertEquals(0, service.stats().failed(), "回收不是失败, 失败计数不能增加");
        assertEquals(1, service.stats().succeeded(), "回收不影响已成功的计数");
        assertEquals(1, store.applied().size(), "回收不能丢已写入的数据");

        // 回收后再提交同一个键：按新键正常排队处理
        WriteOutcome second = await(service.submit(req("ephemeral-key", 1)));
        assertEquals(OutcomeStatus.SUCCESS, second.status(), "回收后再提交应正常处理");
        assertEquals(2, store.applied().size());
        assertEquals(1, keyStatsOf("ephemeral-key").succeeded(),
                "状态已回收, 成功计数从0重新累计(与回收前互不混淆)");
        log.info("[PASS] 空闲回收: 状态已回收且不计失败, 同键再提交正常, applied={}", store.applied().size());
    }

    @Test
    @DisplayName("场景14: 键数超上限时优先回收最久未活动的空闲键")
    void maxKeysEvictsLeastRecentlyActiveIdleKeys() {
        log.info("[SCENARIO] maxKeys=2 -> 第3个键出现后, 最久未活动的空闲键被回收, 其余保留");
        IngestProperties props = baseProps();
        props.setKeyIdleTimeout(Duration.ofMinutes(10)); // 只让 maxKeys 生效
        props.setMaxKeys(2);
        props.setEvictionInterval(Duration.ofMillis(50));
        newService(props);

        await(service.submit(req("old-key", 0)));
        sleep(50); // 拉开 lastActivity 差距, 保证回收顺序确定
        await(service.submit(req("mid-key", 0)));
        sleep(50);
        await(service.submit(req("new-key", 0)));

        waitUntil(() -> service.stats().trackedKeys() == 2, 5_000,
                "键数应被回收至上限");
        List<String> remaining = service.keyStats().stream().map(KeyStats::key).sorted().toList();
        assertEquals(List.of("mid-key", "new-key"), remaining, "应回收最久未活动的 old-key");

        // 被回收的键再提交仍正常
        WriteOutcome again = await(service.submit(req("old-key", 1)));
        assertEquals(OutcomeStatus.SUCCESS, again.status());
        assertEquals(4, store.applied().size(), "回收不丢数据, 全部写入都生效");
        log.info("[PASS] 键数上限回收: 保留={}, old-key 回收后再提交正常", remaining);
    }

    private KeyStats keyStatsOf(String key) {
        return service.keyStats().stream()
                .filter(s -> s.key().equals(key))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no stats for key " + key));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", e);
        }
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMillis, String message) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(20);
        }
        throw new AssertionError("condition not met within " + timeoutMillis + "ms: " + message);
    }
}
