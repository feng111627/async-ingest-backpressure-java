package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 场景：高并发提交 —— 同键保序、跨键并行、重复提交按序生效、单键变慢不拖慢其它键。
 */
class IngestConcurrencyTest {

    private IngestProperties props;
    private InMemoryDownstreamStore store;
    private IngestService service;

    @BeforeEach
    void setUp() {
        props = new IngestProperties();
        props.setMaxPending(10_000);
        props.setMaxBatchSize(8);
        props.setWorkerThreads(4);
        props.setRetryBackoffMs(5);
        store = new InMemoryDownstreamStore();
        service = new IngestService(props, store);
    }

    @AfterEach
    void tearDown() {
        List<String> unfinished = service.shutdown();
        assertThat(unfinished).as("关闭后不应有未处理完的键").isEmpty();
    }

    @Test
    @DisplayName("并发提交：8 个键 × 200 条，同键严格按提交顺序生效，无丢失无重复")
    void concurrentSubmit_sameKeyOrdered_noLossNoDuplication() throws Exception {
        int keys = 8, perKey = 200;
        ExecutorService producers = Executors.newFixedThreadPool(keys);
        List<CompletableFuture<Void>> producerDone = new ArrayList<>();
        for (int k = 0; k < keys; k++) {
            String key = "key-" + k;
            producerDone.add(CompletableFuture.runAsync(() -> {
                List<CompletableFuture<WriteAck>> futures = new ArrayList<>();
                for (int i = 0; i < perKey; i++) {
                    futures.add(service.submit(key, key + "-msg-" + i));
                }
                futures.forEach(CompletableFuture::join);
            }, producers));
        }
        producerDone.forEach(CompletableFuture::join);
        producers.shutdown();
        assertThat(producers.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        for (int k = 0; k < keys; k++) {
            String key = "key-" + k;
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < perKey; i++) {
                expected.add(key + "-msg-" + i);
            }
            assertThat(store.readAll(key)).as("键 %s 的生效顺序", key).isEqualTo(expected);
        }
        System.out.println("[通过] 并发提交保序: " + keys + " 键 x " + perKey + " 条全部按序生效");
    }

    @Test
    @DisplayName("重复提交：同一内容重复提交 50 次，按提交顺序全部生效")
    void duplicateSubmit_appliedInOrder() {
        List<CompletableFuture<WriteAck>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            futures.add(service.submit("dup-key", "same-payload"));
        }
        List<WriteAck> acks = futures.stream().map(CompletableFuture::join).toList();
        // 序号从 0 连续递增，证明按提交顺序生效
        for (int i = 0; i < 50; i++) {
            assertThat(acks.get(i).sequence()).isEqualTo(i);
        }
        assertThat(store.readAll("dup-key")).hasSize(50)
                .allMatch("same-payload"::equals);
        System.out.println("[通过] 重复提交: 50 条相同内容按序生效，序号 0..49 连续");
    }

    @Test
    @DisplayName("下游变慢：某个键的下游卡住，其它键不受影响")
    void slowKey_doesNotBlockOtherKeys() {
        store.setLatencyForKey("slow-key", 300);
        CompletableFuture<WriteAck> slow = service.submit("slow-key", "slow-msg");

        long start = System.nanoTime();
        List<CompletableFuture<WriteAck>> fastFutures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            fastFutures.add(service.submit("fast-key", "fast-" + i));
        }
        fastFutures.forEach(CompletableFuture::join);
        long fastElapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(fastElapsedMs).as("快键不应被慢键拖慢").isLessThan(300);
        assertThat(slow).succeedsWithin(Duration.ofSeconds(5));
        assertThat(store.readAll("fast-key")).hasSize(20);
        assertThat(store.readAll("slow-key")).containsExactly("slow-msg");
        System.out.println("[通过] 单键变慢隔离: 慢键 300ms 延迟期间，快键 20 条在 "
                + fastElapsedMs + "ms 内完成");
    }
}
