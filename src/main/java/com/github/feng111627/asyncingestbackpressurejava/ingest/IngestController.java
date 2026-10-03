package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 写入入口 HTTP 接口。同步等待写入结果，把背压与失败原因映射为明确的状态码。
 */
@RestController
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    public record WriteRequestBody(String key, String payload) {
    }

    @PostMapping("/ingest")
    public ResponseEntity<?> ingest(@RequestBody WriteRequestBody body) {
        try {
            WriteAck ack = ingestService.submit(body.key(), body.payload()).get();
            return ResponseEntity.ok(ack);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "interrupted"));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IngestRejectedException rej) {
                HttpStatus status = switch (rej.reason()) {
                    case QUEUE_FULL, OFFER_TIMEOUT -> HttpStatus.TOO_MANY_REQUESTS;   // 429
                    case LANE_CIRCUIT_OPEN -> HttpStatus.CONFLICT;                      // 409
                    case SHUTTING_DOWN -> HttpStatus.SERVICE_UNAVAILABLE;               // 503
                };
                return ResponseEntity.status(status)
                        .body(Map.of("error", "rejected", "reason", rej.reason().name()));
            }
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                    .body(Map.of("error", "downstream_failed", "message", String.valueOf(cause.getMessage())));
        }
    }

    /** 当前待处理量，便于观测背压。 */
    @GetMapping("/ingest/pending")
    public Map<String, Integer> pending() {
        return Map.of("pending", ingestService.pendingCount());
    }
}
