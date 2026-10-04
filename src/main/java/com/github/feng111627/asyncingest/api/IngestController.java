package com.github.feng111627.asyncingest.api;

import com.github.feng111627.asyncingest.ingest.IngestService;
import com.github.feng111627.asyncingest.ingest.IngestStats;
import com.github.feng111627.asyncingest.ingest.KeyStats;
import com.github.feng111627.asyncingest.ingest.RejectReason;
import com.github.feng111627.asyncingest.ingest.SubmitResult;
import com.github.feng111627.asyncingest.ingest.WriteOutcome;
import com.github.feng111627.asyncingest.ingest.WriteRequest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ingest")
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    /**
     * 提交写入请求。
     * 202：已接收，异步处理；429：背压拒绝（队列满）；503：正在关闭；409：该键已熔断。
     */
    @PostMapping
    public ResponseEntity<?> submit(@RequestBody WriteRequest request) {
        return switch (ingestService.submit(request)) {
            case SubmitResult.Accepted accepted ->
                    ResponseEntity.accepted().body(Map.of(
                            "requestId", request.requestId(),
                            "status", "ACCEPTED"));
            case SubmitResult.Rejected rejected ->
                    ResponseEntity.status(statusFor(rejected.reason())).body(Map.of(
                            "requestId", request.requestId(),
                            "status", "REJECTED",
                            "reason", rejected.reason().name(),
                            "detail", rejected.detail()));
        };
    }

    /**
     * 同步提交：等待最终结果返回，便于脚本与演示验证。
     */
    @PostMapping("/sync")
    public ResponseEntity<?> submitSync(@RequestBody WriteRequest request) {
        return switch (ingestService.submit(request)) {
            case SubmitResult.Accepted accepted -> {
                try {
                    WriteOutcome outcome = accepted.outcome().get();
                    yield ResponseEntity.ok(outcome);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    yield ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                } catch (ExecutionException e) {
                    yield ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                }
            }
            case SubmitResult.Rejected rejected ->
                    ResponseEntity.status(statusFor(rejected.reason())).body(Map.of(
                            "status", "REJECTED",
                            "reason", rejected.reason().name()));
        };
    }

    /**
     * 全局运行指标：接收 / 成功 / 失败 / 待处理量 / 按原因细分的拒绝数。
     */
    @GetMapping("/stats")
    public IngestStats stats() {
        return ingestService.stats();
    }

    /**
     * 每个业务键的运行状态：积压、是否熔断、距离下次试探的剩余时间、已成功写入数。
     */
    @GetMapping("/stats/keys")
    public List<KeyStats> keyStats() {
        return ingestService.keyStats();
    }

    private HttpStatus statusFor(RejectReason reason) {
        return switch (reason) {
            case QUEUE_FULL, QUEUE_FULL_TIMEOUT -> HttpStatus.TOO_MANY_REQUESTS;
            case SHUTTING_DOWN -> HttpStatus.SERVICE_UNAVAILABLE;
            case KEY_CIRCUIT_OPEN -> HttpStatus.CONFLICT;
        };
    }
}
