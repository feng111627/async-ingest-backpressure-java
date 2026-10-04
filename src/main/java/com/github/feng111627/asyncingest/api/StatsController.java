package com.github.feng111627.asyncingest.api;

import com.github.feng111627.asyncingest.ingest.IngestMetrics;
import com.github.feng111627.asyncingest.ingest.IngestService;
import com.github.feng111627.asyncingest.ingest.KeyStatus;
import com.github.feng111627.asyncingest.ingest.RejectReason;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 运行状态查询接口：全局指标与每键状态，从真实运行的应用直接可查。
 */
@RestController
@RequestMapping("/ingest/stats")
public class StatsController {

    private final IngestService ingestService;

    public StatsController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    /** 全局指标：接收 / 成功 / 失败 / 按原因区分的拒绝，以及当前待处理量与被跟踪键数。 */
    @GetMapping
    public GlobalStatsView global() {
        IngestMetrics.GlobalStats stats = ingestService.globalStats();
        return new GlobalStatsView(
                stats.received(),
                stats.succeeded(),
                stats.failed(),
                stats.rejectedTotal(),
                stats.rejectedByReason(),
                ingestService.pendingCount(),
                ingestService.trackedKeyCount());
    }

    /** 所有被跟踪键的运行状态。 */
    @GetMapping("/keys")
    public List<KeyStatus> keys() {
        return ingestService.keyStatuses();
    }

    /** 单个键的运行状态；键不存在（或状态已被回收）时 404。 */
    @GetMapping("/keys/{key}")
    public ResponseEntity<KeyStatus> key(@PathVariable String key) {
        return ingestService.keyStatus(key)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    public record GlobalStatsView(
            long received,
            long succeeded,
            long failed,
            long rejectedTotal,
            Map<RejectReason, Long> rejectedByReason,
            int pending,
            int trackedKeys) {
    }
}
