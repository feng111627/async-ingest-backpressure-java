package com.github.feng111627.asyncingestbackpressurejava.ingest;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 异步批量写入入口：背压、按键保序、失败重试与优雅关闭。
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final IngestProperties props;
    private final DownstreamStore store;
    private final ConcurrentHashMap<String, KeyLane> lanes = new ConcurrentHashMap<>();
    /** 全局待处理许可：排队中 + 处理中的总量上限 */
    private final Semaphore permits;
    private final ExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public IngestService(IngestProperties properties, DownstreamStore store) {
        this.props = properties;
        this.store = store;
        this.permits = new Semaphore(properties.getMaxPending());
        this.executor = Executors.newFixedThreadPool(properties.getWorkerThreads(), r -> {
            Thread t = new Thread(r, "ingest-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 提交一条写入。返回的 future 在数据真正写入下游后完成；
     * 被拒绝时以 {@link IngestRejectedException} 完成（原因可区分）。
     */
    public CompletableFuture<WriteAck> submit(String key, String payload) {
        if (closed.get()) {
            return rejected(RejectReason.SHUTTING_DOWN, "服务正在关闭，不再接收新请求");
        }
        if (!acquirePermit()) {
            RejectReason reason = props.getBackpressureStrategy() == IngestProperties.BackpressureStrategy.REJECT
                    ? RejectReason.QUEUE_FULL : RejectReason.OFFER_TIMEOUT;
            return rejected(reason, "待处理量已达上限 " + props.getMaxPending());
        }
        if (closed.get()) { // 拿到许可后再次检查，避免关闭瞬间漏接
            permits.release();
            return rejected(RejectReason.SHUTTING_DOWN, "服务正在关闭，不再接收新请求");
        }
        KeyLane lane = lanes.computeIfAbsent(key,
                k -> new KeyLane(k, executor, store, props, permits));
        if (lane.isCircuitOpen()) {
            permits.release();
            return rejected(RejectReason.LANE_CIRCUIT_OPEN, "该键通道已熔断, key=" + key);
        }
        CompletableFuture<WriteAck> result = new CompletableFuture<>();
        lane.offer(new WriteCommand(key, payload, result));
        return result;
    }

    private boolean acquirePermit() {
        if (props.getBackpressureStrategy() == IngestProperties.BackpressureStrategy.REJECT) {
            return permits.tryAcquire();
        }
        try {
            return permits.tryAcquire(props.getOfferTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private CompletableFuture<WriteAck> rejected(RejectReason reason, String message) {
        return CompletableFuture.failedFuture(new IngestRejectedException(reason, message));
    }

    /** 当前待处理量（排队中 + 处理中）。 */
    public int pendingCount() {
        return props.getMaxPending() - permits.availablePermits();
    }

    /**
     * 关闭：停止接收新请求，在超时时间内等待已接收数据处理完。
     *
     * @return 超时后仍未处理完的键列表；为空表示全部处理完
     */
    @PreDestroy
    public List<String> shutdown() {
        if (!closed.compareAndSet(false, true)) {
            return List.of();
        }
        log.info("开始关闭，停止接收新请求，等待 {} 条待处理数据完成", pendingCount());
        long deadline = System.nanoTime() + props.getShutdownTimeoutMs() * 1_000_000L;
        while (pendingCount() > 0 && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        List<String> unfinished = lanes.values().stream()
                .filter(lane -> !lane.isIdle())
                .map(KeyLane::key)
                .sorted()
                .toList();
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (unfinished.isEmpty()) {
            log.info("关闭完成：所有已接收数据均已处理完毕");
        } else {
            log.warn("关闭超时：以下键仍有未处理完的数据: {}", unfinished);
        }
        return unfinished;
    }
}
