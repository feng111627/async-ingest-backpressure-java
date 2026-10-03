package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 场景：HTTP 入口 —— 成功返回确认，背压拒绝返回 429 及可区分原因。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ingest.max-pending=8",
        "ingest.backpressure-strategy=REJECT",
        "ingest.max-batch-size=1"
})
class IngestControllerTest {

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private InMemoryDownstreamStore store;

    @Test
    @DisplayName("HTTP 成功写入返回 200 与确认；背压时返回 429 与 QUEUE_FULL 原因")
    void ingest_ok_and_backpressure429() throws Exception {
        store.clear();

        ResponseEntity<WriteAck> ok = rest.postForEntity("/ingest",
                Map.of("key", "http-key", "payload", "hello"), WriteAck.class);
        assertThat(ok.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ok.getBody()).isNotNull();
        assertThat(ok.getBody().key()).isEqualTo("http-key");
        assertThat(store.readAll("http-key")).containsExactly("hello");

        // 下游卡住，并发打满待处理上限触发背压
        store.setLatencyMs(500);
        java.util.concurrent.atomic.AtomicInteger rejected = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger succeeded = new java.util.concurrent.atomic.AtomicInteger();
        int threads = 20, perThread = 5;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.List<java.util.concurrent.Future<?>> tasks = new java.util.ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int base = t * perThread;
            tasks.add(pool.submit(() -> {
                for (int i = 0; i < perThread; i++) {
                    ResponseEntity<Map> resp = rest.postForEntity("/ingest",
                            Map.of("key", "k" + ((base + i) % 3), "payload", "p" + (base + i)),
                            Map.class);
                    if (resp.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                        assertThat(resp.getBody()).isNotNull();
                        assertThat(resp.getBody().get("reason")).isEqualTo("QUEUE_FULL");
                        rejected.incrementAndGet();
                    } else if (resp.getStatusCode() == HttpStatus.OK) {
                        succeeded.incrementAndGet();
                    }
                }
            }));
        }
        for (var task : tasks) {
            task.get(30, java.util.concurrent.TimeUnit.SECONDS);
        }
        pool.shutdown();
        store.setLatencyMs(0);
        assertThat(rejected.get()).as("应出现 429 背压拒绝").isGreaterThan(0);
        assertThat(succeeded.get()).isGreaterThan(0);
        System.out.println("[通过] HTTP 接口: 200 确认 + 并发下 " + rejected.get()
                + " 次 429/QUEUE_FULL 背压拒绝, " + succeeded.get() + " 次成功");
    }
}
