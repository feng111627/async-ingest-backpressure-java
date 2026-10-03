package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 场景：下游失败按配置次数重试；重试耗尽后该键熔断，后续请求快速失败；
 * 失败在生效前抛出，不产生部分写或重复写。
 */
class RetryAndCircuitTest {

    private IngestProperties props;
    private InMemoryDownstreamStore store;
    private IngestService service;

    @BeforeEach
    void setUp() {
        props = new IngestProperties();
        props.setMaxPending(1_000);
        props.setMaxBatchSize(1);
        props.setMaxRetries(3);
        props.setRetryBackoffMs(10);
        store = new InMemoryDownstreamStore();
        service = new IngestService(props, store);
    }

    @AfterEach
    void tearDown() {
        service.shutdown();
    }

    @Test
    @DisplayName("重试成功：前 2 次失败第 3 次成功，数据恰好生效一次")
    void retryThenSucceeds_exactlyOnce() {
        store.injectFailures(2);
        WriteAck ack = service.submit("rk", "payload-1").join();
        assertThat(ack.sequence()).isZero();
        assertThat(store.readAll("rk")).containsExactly("payload-1"); // 不重不漏
        assertThat(store.writeCalls()).isEqualTo(3); // 1 次原始 + 2 次重试
        System.out.println("[通过] 重试成功: 2 次失败后第 3 次写入成功，数据恰好生效一次");
    }

    @Test
    @DisplayName("重试耗尽：该批失败且不生效，后续请求以 LANE_CIRCUIT_OPEN 快速失败")
    void retriesExhausted_circuitOpens_fastFail() throws Exception {
        store.injectFailures(100); // 一直失败

        CompletableFuture<WriteAck> first = service.submit("ck", "doomed");
        assertThat(first).failsWithin(java.time.Duration.ofSeconds(5))
                .withThrowableThat()
                .havingRootCause()
                .withMessageContaining("injected downstream failure");
        // 重试 3 次 + 首次 = 4 次调用，且失败的数据没有生效
        assertThat(store.writeCalls()).isEqualTo(4);
        assertThat(store.readAll("ck")).isEmpty();

        // 熔断后快速失败：不再调用下游
        long callsBefore = store.writeCalls();
        CompletableFuture<WriteAck> after = service.submit("ck", "after-circuit");
        try {
            after.get();
            org.assertj.core.api.Assertions.fail("熔断后应快速失败");
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(IngestRejectedException.class);
            assertThat(((IngestRejectedException) e.getCause()).reason())
                    .isEqualTo(RejectReason.LANE_CIRCUIT_OPEN);
        }
        assertThat(store.writeCalls()).isEqualTo(callsBefore);
        System.out.println("[通过] 重试耗尽熔断: 4 次调用后熔断，后续请求快速失败且不再触达下游");
    }

    @Test
    @DisplayName("熔断隔离：一个键熔断不影响其它键正常写入")
    void circuitOpen_isolatedPerKey() {
        store.injectFailures(100);
        CompletableFuture<WriteAck> doomed = service.submit("bad-key", "x");
        assertThat(doomed).failsWithin(java.time.Duration.ofSeconds(5));

        store.injectFailures(0); // 下游恢复
        List<CompletableFuture<WriteAck>> good = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            good.add(service.submit("good-key", "g" + i));
        }
        good.forEach(CompletableFuture::join);
        assertThat(store.readAll("good-key")).hasSize(10);
        System.out.println("[通过] 熔断隔离: bad-key 熔断期间 good-key 10 条正常写入");
    }

    @Test
    @DisplayName("已排队命令在熔断时以 LANE_CIRCUIT_OPEN 失败，不静默丢弃")
    void queuedCommands_failFastOnCircuit() throws Exception {
        store.setLatencyMs(150); // 让后续命令有时间排队
        store.injectFailures(100);

        CompletableFuture<WriteAck> first = service.submit("qk", "m0");
        List<CompletableFuture<WriteAck>> queued = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            queued.add(service.submit("qk", "m" + i));
        }
        assertThat(first).failsWithin(java.time.Duration.ofSeconds(5));
        for (CompletableFuture<WriteAck> f : queued) {
            try {
                f.get();
                org.assertj.core.api.Assertions.fail("排队命令应快速失败");
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IngestRejectedException.class);
                assertThat(((IngestRejectedException) e.getCause()).reason())
                        .isEqualTo(RejectReason.LANE_CIRCUIT_OPEN);
            }
        }
        assertThat(store.readAll("qk")).isEmpty(); // 全部未生效，无部分写
        System.out.println("[通过] 熔断时排队命令: 5 条排队命令全部以 LANE_CIRCUIT_OPEN 明确失败");
    }
}
