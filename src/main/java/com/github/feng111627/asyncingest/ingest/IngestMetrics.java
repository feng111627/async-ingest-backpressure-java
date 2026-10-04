package com.github.feng111627.asyncingest.ingest;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 通道全局运行指标。全部使用 AtomicLong 计数，并发提交下不漏计、不多计。
 *
 * <p>计数口径：
 * <ul>
 *   <li>received：提交被接收（进入通道）的次数；</li>
 *   <li>succeeded / failed：已接收请求的最终结果，二者之和最终等于 received
 *       （运行中差额为排队中 + 处理中的请求）；</li>
 *   <li>rejectedXxx：提交被拒绝的次数，按原因分别计数。</li>
 * </ul>
 */
public class IngestMetrics {

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final EnumMap<RejectReason, AtomicLong> rejectedByReason = new EnumMap<>(RejectReason.class);

    public IngestMetrics() {
        for (RejectReason reason : RejectReason.values()) {
            rejectedByReason.put(reason, new AtomicLong());
        }
    }

    public void onAccepted() {
        received.incrementAndGet();
    }

    public void onRejected(RejectReason reason) {
        rejectedByReason.get(reason).incrementAndGet();
    }

    public void onOutcome(OutcomeStatus status) {
        if (status == OutcomeStatus.SUCCESS) {
            succeeded.incrementAndGet();
        } else {
            failed.incrementAndGet();
        }
    }

    public long received() {
        return received.get();
    }

    public long succeeded() {
        return succeeded.get();
    }

    public long failed() {
        return failed.get();
    }

    public long rejected(RejectReason reason) {
        return rejectedByReason.get(reason).get();
    }

    public long rejectedTotal() {
        return rejectedByReason.values().stream().mapToLong(AtomicLong::get).sum();
    }

    /** 各拒绝原因的计数值快照（只含发生过的原因不必，全部原因都给出，便于调用方直接读）。 */
    public Map<RejectReason, Long> rejectedSnapshot() {
        EnumMap<RejectReason, Long> snapshot = new EnumMap<>(RejectReason.class);
        rejectedByReason.forEach((reason, counter) -> snapshot.put(reason, counter.get()));
        return Map.copyOf(snapshot);
    }
}
