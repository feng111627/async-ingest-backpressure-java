package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 场景：进程关闭 —— 停止接收新请求，超时时间内排空已接收数据，
 * 排不完要明确报告，不能写一半或重复写。
 */
class ShutdownTest {

    private IngestService newService(InMemoryDownstreamStore store, long shutdownTimeoutMs) {
        IngestProperties props = new IngestProperties();
        props.setMaxPending(10_000);
        props.setMaxBatchSize(4);
        props.setRetryBackoffMs(5);
        props.setShutdownTimeoutMs(shutdownTimeoutMs);
        return new IngestService(props, store);
    }

    @Test
    @DisplayName("优雅关闭：已接收数据在超时内全部处理完，无丢失无重复")
    void gracefulShutdown_drainsAllAccepted() {
        InMemoryDownstreamStore store = new InMemoryDownstreamStore();
        store.setLatencyMs(30); // 下游较慢，关闭时仍有积压
        IngestService service = newService(store, 10_000);

        List<CompletableFuture<WriteAck>> futures = new ArrayList<>();
        for (int k = 0; k < 4; k++) {
            for (int i = 0; i < 25; i++) {
                futures.add(service.submit("k" + k, "k" + k + "-" + i));
            }
        }
        List<String> unfinished = service.shutdown();
        assertThat(unfinished).as("超时内应全部排空").isEmpty();

        // 所有已接收的 future 都成功完成，且每个键恰好 25 条、按序
        futures.forEach(CompletableFuture::join);
        for (int k = 0; k < 4; k++) {
            List<String> stored = store.readAll("k" + k);
            assertThat(stored).hasSize(25);
            for (int i = 0; i < 25; i++) {
                assertThat(stored.get(i)).isEqualTo("k" + k + "-" + i);
            }
        }
        System.out.println("[通过] 优雅关闭: 100 条已接收数据在关闭时全部处理完，无丢失无重复");
    }

    @Test
    @DisplayName("关闭后新提交以 SHUTTING_DOWN 拒绝")
    void shutdown_rejectsNewSubmissions() throws Exception {
        InMemoryDownstreamStore store = new InMemoryDownstreamStore();
        IngestService service = newService(store, 5_000);
        service.submit("k", "before").join();
        service.shutdown();

        CompletableFuture<WriteAck> after = service.submit("k", "after");
        try {
            after.get();
            org.assertj.core.api.Assertions.fail("关闭后应拒绝新提交");
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(IngestRejectedException.class);
            assertThat(((IngestRejectedException) e.getCause()).reason())
                    .isEqualTo(RejectReason.SHUTTING_DOWN);
        }
        assertThat(store.readAll("k")).containsExactly("before");
        System.out.println("[通过] 关闭拒新: 关闭后新提交以 SHUTTING_DOWN 拒绝");
    }

    @Test
    @DisplayName("关闭超时：明确报告未处理完的键，已生效数据不写一半")
    void shutdownTimeout_reportsUnfinishedKeys() {
        InMemoryDownstreamStore store = new InMemoryDownstreamStore();
        store.setLatencyMs(2_000); // 下游极慢，超时内排不完
        IngestService service = newService(store, 300);

        List<CompletableFuture<WriteAck>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(service.submit("stuck-key", "m" + i));
        }
        long start = System.nanoTime();
        List<String> unfinished = service.shutdown();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).isLessThan(2_000); // 没有傻等下游
        assertThat(unfinished).as("必须明确报告未处理完的键").contains("stuck-key");
        // 已生效的数据是完整的前缀（批次原子生效，不写一半）
        List<String> stored = store.readAll("stuck-key");
        for (int i = 0; i < stored.size(); i++) {
            assertThat(stored.get(i)).isEqualTo("m" + i);
        }
        System.out.println("[通过] 关闭超时报告: " + elapsedMs + "ms 后报告未处理完的键 "
                + unfinished + ", 已生效 " + stored.size() + " 条为完整前缀");
    }
}
