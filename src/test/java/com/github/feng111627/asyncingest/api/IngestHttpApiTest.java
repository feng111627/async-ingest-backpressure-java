package com.github.feng111627.asyncingest.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.asyncingest.ingest.InMemoryDownstreamStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
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
import org.springframework.test.annotation.DirtiesContext;

/**
 * 真实启动应用的 HTTP 端到端验证：并发提交、统计查询、熔断状态与冷却恢复、
 * 空闲键回收都通过真实 HTTP 接口确认，而不是只在进程内成立。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ingest.max-retries=1",
                "ingest.retry-backoff=10ms",
                "ingest.circuit-cooldown=300ms",
                "ingest.key-idle-timeout=200ms",
                "ingest.reap-interval=50ms"
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class IngestHttpApiTest {

    private static final Logger log = LoggerFactory.getLogger(IngestHttpApiTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    InMemoryDownstreamStore store;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private ResponseEntity<JsonNode> postSync(String key, int seq) {
        return rest.postForEntity(url("/ingest/sync"),
                Map.of("requestId", key + "-" + seq, "key", key, "payload", "p" + seq),
                JsonNode.class);
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
    @DisplayName("HTTP场景1: 真实应用并发提交，全局与每键统计通过 HTTP 查询且准确")
    void concurrentHttpSubmitsProduceAccurateStats() throws Exception {
        log.info("[SCENARIO] 真实应用: 4线程x25条并发HTTP提交 -> /ingest/stats 与 /ingest/stats/keys/{key} 数字精确");
        int threads = 4;
        int perThread = 25;
        AtomicInteger successResponses = new AtomicInteger();
        List<Thread> clients = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            String key = "http-key-" + t;
            clients.add(Thread.startVirtualThread(() -> {
                for (int i = 0; i < perThread; i++) {
                    ResponseEntity<JsonNode> resp = postSync(key, i);
                    assertEquals(HttpStatus.OK, resp.getStatusCode());
                    if ("SUCCESS".equals(resp.getBody().get("status").asText())) {
                        successResponses.incrementAndGet();
                    }
                }
            }));
        }
        for (Thread c : clients) {
            c.join();
        }
        assertEquals(threads * perThread, successResponses.get(), "每条并发 HTTP 提交都应成功");

        JsonNode stats = rest.getForObject(url("/ingest/stats"), JsonNode.class);
        assertEquals(threads * perThread, stats.get("received").asLong(), "接收数不能漏计或多计");
        assertEquals(threads * perThread, stats.get("succeeded").asLong());
        assertEquals(0, stats.get("failed").asLong());
        assertEquals(0, stats.get("rejectedTotal").asLong());
        assertEquals(0, stats.get("pending").asInt());
        assertEquals(threads, stats.get("trackedKeys").asInt());

        JsonNode key0 = rest.getForObject(url("/ingest/stats/keys/http-key-0"), JsonNode.class);
        assertEquals(perThread, key0.get("successCount").asLong(), "每键成功数应等于该键提交数");
        assertEquals(0, key0.get("backlog").asInt());
        assertFalse(key0.get("circuitBlocked").asBoolean());
        log.info("[PASS] HTTP并发统计: received={} succeeded={} trackedKeys={}, key0.successCount={}",
                stats.get("received"), stats.get("succeeded"), stats.get("trackedKeys"),
                key0.get("successCount"));
    }

    @Test
    @DisplayName("HTTP场景2: 熔断状态、冷却剩余时间与半开恢复都可通过 HTTP 查询确认")
    void circuitStateAndRecoveryVisibleOverHttp() throws Exception {
        log.info("[SCENARIO] 真实应用: 下游故障 -> 键被挡(可查冷却剩余) -> 冷却后半开试探成功 -> 键恢复");
        store.failNextTimes(2); // max-retries=1: 首次请求 2 次尝试全部失败

        ResponseEntity<JsonNode> failed = postSync("http-flaky", 0);
        assertEquals(HttpStatus.OK, failed.getStatusCode());
        assertEquals("FAILED_RETRY_EXHAUSTED", failed.getBody().get("status").asText());

        JsonNode blocked = rest.getForObject(url("/ingest/stats/keys/http-flaky"), JsonNode.class);
        assertTrue(blocked.get("circuitBlocked").asBoolean(), "熔断后键应被挡住");
        assertTrue(blocked.get("cooldownRemainingMillis").asLong() > 0, "应报告冷却剩余时间");

        ResponseEntity<JsonNode> rejected = postSync("http-flaky", 1);
        assertEquals(HttpStatus.CONFLICT, rejected.getStatusCode(), "冷却期间应快速失败");
        assertEquals("KEY_CIRCUIT_OPEN", rejected.getBody().get("reason").asText());
        JsonNode statsMid = rest.getForObject(url("/ingest/stats"), JsonNode.class);
        assertEquals(1, statsMid.get("rejectedByReason").get("KEY_CIRCUIT_OPEN").asLong(),
                "拒绝应按原因区分计数");

        Thread.sleep(400); // 越过 300ms 冷却
        ResponseEntity<JsonNode> probe = postSync("http-flaky", 2);
        assertEquals("SUCCESS", probe.getBody().get("status").asText(), "冷却后试探应成功");
        JsonNode recovered = rest.getForObject(url("/ingest/stats/keys/http-flaky"), JsonNode.class);
        assertFalse(recovered.get("circuitBlocked").asBoolean(), "试探成功后键应恢复");
        assertEquals(1, recovered.get("successCount").asLong());
        log.info("[PASS] HTTP熔断恢复: 冷却300ms后试探成功, 键状态恢复, stats={}",
                rest.getForObject(url("/ingest/stats"), JsonNode.class));
    }

    @Test
    @DisplayName("HTTP场景3: 空闲键状态回收可通过 HTTP 观察到，回收后同键再提交正常")
    void idleKeyReapVisibleOverHttp() {
        log.info("[SCENARIO] 真实应用: 提交成功 -> 空闲200ms后键状态被回收(404) -> 同键再提交仍成功");
        assertEquals("SUCCESS", postSync("http-idle", 0).getBody().get("status").asText());
        assertEquals(HttpStatus.OK,
                rest.getForEntity(url("/ingest/stats/keys/http-idle"), JsonNode.class).getStatusCode());

        waitFor(() -> rest.getForEntity(url("/ingest/stats/keys/http-idle"), JsonNode.class)
                        .getStatusCode() == HttpStatus.NOT_FOUND,
                "idle key state to be reaped (404)");

        assertEquals("SUCCESS", postSync("http-idle", 1).getBody().get("status").asText(),
                "回收后同键再提交应正常处理");
        JsonNode reborn = rest.getForObject(url("/ingest/stats/keys/http-idle"), JsonNode.class);
        assertEquals(1, reborn.get("successCount").asLong(), "回收后按新键重新统计");
        JsonNode stats = rest.getForObject(url("/ingest/stats"), JsonNode.class);
        assertEquals(2, stats.get("succeeded").asLong(), "回收不算失败也不丢计数");
        assertEquals(0, stats.get("failed").asLong());
        log.info("[PASS] HTTP空闲回收: 键状态回收(404)后再提交成功, stats={}", stats);
    }
}
