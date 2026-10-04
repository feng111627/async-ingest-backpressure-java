package com.github.feng111627.asyncingest.ingest;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 全局运行指标。所有计数用 {@link LongAdder}，并发提交下准确、不互相拖累。
 *
 * <p>计数口径：
 * <ul>
 *   <li>received：被接收（Accepted）的提交数；</li>
 *   <li>succeeded：最终结果为 SUCCESS 的请求数；</li>
 *   <li>failed：最终结果为失败（重试耗尽 / 键熔断丢弃 / 关闭超时未处理）的请求数；</li>
 *   <li>rejectedByReason：提交即被拒绝的数量，按 {@link RejectReason} 区分。</li>
 * </ul>
 * 每条提交只会落入 received 或 rejected 之一；每条被接收的请求最终只会计入
 * succeeded 或 failed 之一，不会漏计或多计。
 */
public class IngestMetrics {

    private final LongAdder received = new LongAdder();
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final Map<RejectReason, LongAdder> rejected = new EnumMap<>(RejectReason.class);

    public IngestMetrics() {
        for (RejectReason reason : RejectReason.values()) {
            rejected.put(reason, new LongAdder());
        }
    }

    public void onAccepted() {
        received.increment();
    }

    public void onRejected(RejectReason reason) {
        rejected.get(reason).increment();
    }

    public void onOutcome(OutcomeStatus status) {
        if (status == OutcomeStatus.SUCCESS) {
            succeeded.increment();
        } else {
            failed.increment();
        }
    }

    /** 当前全局指标的一致性快照。 */
    public GlobalStats snapshot() {
        Map<RejectReason, Long> rejectedSnapshot = new EnumMap<>(RejectReason.class);
        rejected.forEach((reason, adder) -> rejectedSnapshot.put(reason, adder.sum()));
        return new GlobalStats(received.sum(), succeeded.sum(), failed.sum(),
                Map.copyOf(rejectedSnapshot));
    }

    /** 全局指标快照。 */
    public record GlobalStats(
            long received,
            long succeeded,
            long failed,
            Map<RejectReason, Long> rejectedByReason) {

        public long rejectedTotal() {
            return rejectedByReason.values().stream().mapToLong(Long::longValue).sum();
        }
    }
}
