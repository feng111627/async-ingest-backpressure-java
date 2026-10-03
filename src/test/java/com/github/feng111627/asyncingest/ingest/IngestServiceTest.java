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
}
