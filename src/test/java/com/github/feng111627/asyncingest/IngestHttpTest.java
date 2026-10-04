package com.github.feng111627.asyncingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.asyncingest.ingest.InMemoryDownstreamStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * 基于真实启动的应用（内嵌 Tomcat、随机端口）做并发 HTTP 验证：
 * 确认并发提交下统计准确、每键状态可查、熔断拒绝与分类计数在真实运行条件下成立。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ingest.max-pending=1000",
        "ingest.max-retries=3",
        "ingest.retry-backoff=10ms",
        "ingest.circuit-cooldown=1m"
})
class IngestHttpTest {

    private static final Logger log = LoggerFactory.getLogger(IngestHttpTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    InMemoryDownstreamStore store;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    @DisplayName("HTTP场景: 真实应用下并发提交统计准确、每键状态可查、熔断拒绝分类计数")
    void concurrentHttpSubmitAndStats() throws Exception {
        log.info("[SCENARIO-HTTP] 真实启动应用: 8线程x25条并发POST /ingest/sync -> 统计精确; "
                + "注入下游故障 -> 熔断拒绝 KEY_CIRCUIT_OPEN 分类计数");

        // 阶段1: 8 个线程并发提交, 每线程一个键、25 条同步请求
        int threads = 8, perThread = 25;
        List<ResponseEntity<JsonNode>> responses = new CopyOnWriteArrayList<>();
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers.add(Thread.startVirtualThread(() -> {
                for (int i = 0; i < perThread; i++) {
                    Map<String, String> body = Map.of(
                            "requestId", "http-" + tid + "-" + i,
                            "key", "http-key-" + tid,
                            "payload", "v" + i);
                    responses.add(rest.postForEntity(url("/ingest/sync"), body, JsonNode.class));
                }
            }));
        }
        for (Thread w : workers) {
            w.join();
        }
        assertEquals(threads * perThread, responses.size());
        assertTrue(responses.stream().allMatch(r -> r.getStatusCode() == HttpStatus.OK),
                "并发提交应全部成功");
        assertTrue(responses.stream().allMatch(r -> "SUCCESS".equals(r.getBody().get("status").asText())),
                "每条结果都应为 SUCCESS");

        // 阶段2: 全局统计精确等于实际发生数
        JsonNode stats = rest.getForObject(url("/ingest/stats"), JsonNode.class);
        assertEquals(200, stats.get("received").asLong(), "接收数");
        assertEquals(200, stats.get("succeeded").asLong(), "成功数");
        assertEquals(0, stats.get("failed").asLong(), "失败数");
        assertEquals(0, stats.get("rejectedTotal").asLong(), "拒绝数");
        assertEquals(0, stats.get("pending").asLong(), "待处理应归零");
        log.info("[PASS] HTTP并发统计: received={} succeeded={} failed={} rejected={}",
                stats.get("received"), stats.get("succeeded"), stats.get("failed"), stats.get("rejectedTotal"));

        // 阶段3: 每键状态可查且计数准确
        JsonNode keys = rest.getForObject(url("/ingest/stats/keys"), JsonNode.class);
        assertEquals(threads, keys.size(), "应有8个键的状态");
        for (JsonNode key : keys) {
            assertEquals(perThread, key.get("succeeded").asLong(),
                    "键 " + key.get("key").asText() + " 的成功数");
            assertTrue(!key.get("circuitOpen").asBoolean(), "正常键不应熔断");
        }
        log.info("[PASS] HTTP每键状态: {} 个键, 每键 succeeded={}", keys.size(), perThread);

        // 阶段4: 注入下游故障 -> 重试耗尽熔断 -> 同键再提交被 409/KEY_CIRCUIT_OPEN 拒绝并分类计数
        store.failNextTimes(4); // 1次首试+3次重试 全部失败
        ResponseEntity<JsonNode> failed = rest.postForEntity(url("/ingest/sync"),
                Map.of("requestId", "http-bad-0", "key", "http-bad-key", "payload", "x"), JsonNode.class);
        assertEquals(HttpStatus.OK, failed.getStatusCode());
        assertEquals("FAILED_RETRY_EXHAUSTED", failed.getBody().get("status").asText());

        ResponseEntity<JsonNode> rejected = rest.postForEntity(url("/ingest"),
                Map.of("requestId", "http-bad-1", "key", "http-bad-key", "payload", "x"), JsonNode.class);
        assertEquals(HttpStatus.CONFLICT, rejected.getStatusCode(), "熔断键应返回409");
        assertEquals("KEY_CIRCUIT_OPEN", rejected.getBody().get("reason").asText());

        JsonNode after = rest.getForObject(url("/ingest/stats"), JsonNode.class);
        assertEquals(201, after.get("received").asLong());
        assertEquals(200, after.get("succeeded").asLong());
        assertEquals(1, after.get("failed").asLong());
        assertEquals(1, after.get("rejectedTotal").asLong());
        assertEquals(1, after.get("rejectedByReason").get("KEY_CIRCUIT_OPEN").asLong());
        assertEquals(0, after.get("rejectedByReason").get("QUEUE_FULL").asLong());

        JsonNode badKey = null;
        for (JsonNode key : rest.getForObject(url("/ingest/stats/keys"), JsonNode.class)) {
            if ("http-bad-key".equals(key.get("key").asText())) {
                badKey = key;
            }
        }
        assertTrue(badKey != null && badKey.get("circuitOpen").asBoolean(), "熔断键状态应可查");
        assertTrue(badKey.get("circuitRetryAfterMillis").asLong() > 0, "应报告剩余冷却时间");
        log.info("[PASS] HTTP熔断计数: failed={} KEY_CIRCUIT_OPEN拒绝={}, 键状态={}",
                after.get("failed"), after.get("rejectedByReason").get("KEY_CIRCUIT_OPEN"), badKey);
    }
}
