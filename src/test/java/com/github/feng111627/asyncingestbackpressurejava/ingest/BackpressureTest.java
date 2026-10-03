package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 场景：下游处理不过来时，按配置策略对新提交排队或拒绝，拒绝原因可区分。
 */
class BackpressureTest {

    private IngestProperties props;
    private InMemoryDownstreamStore store;
    private IngestService service;

    @AfterEach
    void tearDown() {
        if (service != null) {
            store.setLatencyMs(0); // 放通下游，让关闭时能排空
            service.shutdown();
        }
    }

    private void start(IngestProperties.BackpressureStrategy strategy, int maxPending, long offerTimeoutMs) {
        props = new IngestProperties();
        props.setBackpressureStrategy(strategy);
        props.setMaxPending(maxPending);
        props.setOfferTimeoutMs(offerTimeoutMs);
        props.setMaxBatchSize(1);
        props.setRetryBackoffMs(5);
        store = new InMemoryDownstreamStore();
        service = new IngestService(props, store);
    }

    @Test
    @DisplayName("REJECT 策略：待处理量达上限后新提交立即以 QUEUE_FULL 拒绝")
    void rejectStrategy_queueFull() {
        start(IngestProperties.BackpressureStrategy.REJECT, 10, 50);
        store.setLatencyMs(100); // 下游变慢，积压

        int accepted = 0, queueFull = 0;
        List<CompletableFuture<WriteAck>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            CompletableFuture<WriteAck> f = service.submit("k" + (i % 4), "m" + i);
            futures.add(f);
        }
        for (CompletableFuture<WriteAck> f : futures) {
            try {
                f.get();
                accepted++;
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IngestRejectedException.class);
                RejectReason reason = ((IngestRejectedException) e.getCause()).reason();
                assertThat(reason).isEqualTo(RejectReason.QUEUE_FULL);
                queueFull++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        assertThat(queueFull).as("必须有可区分原因的拒绝").isGreaterThan(0);
        assertThat(accepted).isLessThanOrEqualTo(10 + 4); // 上限 + 处理中少量
        System.out.println("[通过] REJECT 背压: 100 条提交, 接受 " + accepted
                + ", 以 QUEUE_FULL 拒绝 " + queueFull);
    }

    @Test
    @DisplayName("BLOCK 策略：容量满时排队等待，下游恢复后全部成功")
    void blockStrategy_waitsAndSucceeds() {
        start(IngestProperties.BackpressureStrategy.BLOCK, 10, 5_000);
        store.setLatencyMs(20); // 下游慢但能动

        List<CompletableFuture<WriteAck>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            futures.add(service.submit("bk", "m" + i));
        }
        futures.forEach(CompletableFuture::join); // 不应有任何拒绝
        assertThat(store.readAll("bk")).hasSize(50);
        System.out.println("[通过] BLOCK 背压: 50 条在容量 10 下排队等待，全部成功");
    }

    @Test
    @DisplayName("BLOCK 策略：等待超时后以 OFFER_TIMEOUT 拒绝，与 QUEUE_FULL 可区分")
    void blockStrategy_offerTimeout() {
        start(IngestProperties.BackpressureStrategy.BLOCK, 5, 100);
        store.setLatencyMs(2_000); // 下游几乎不动

        List<CompletableFuture<WriteAck>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            futures.add(service.submit("tk", "m" + i));
        }
        int timeoutRejected = 0;
        for (CompletableFuture<WriteAck> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IngestRejectedException.class);
                assertThat(((IngestRejectedException) e.getCause()).reason())
                        .isEqualTo(RejectReason.OFFER_TIMEOUT);
                timeoutRejected++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        assertThat(timeoutRejected).isGreaterThan(0);
        System.out.println("[通过] BLOCK 超时拒绝: 以 OFFER_TIMEOUT 拒绝 " + timeoutRejected + " 条");
    }
}
