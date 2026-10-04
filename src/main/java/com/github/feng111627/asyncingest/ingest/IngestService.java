package com.github.feng111627.asyncingest.ingest;

import com.github.feng111627.asyncingest.config.IngestProperties;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 异步批量写入通道。
 *
 * <p>语义：
 * <ul>
 *   <li>同一业务键的请求进入该键的串行通道，严格按提交顺序写入下游；不同键走各自通道，
 *       由共享线程池并行推进，某个键卡住（下游慢/重试）不会拖慢其它键。</li>
 *   <li>全局待处理量（排队中 + 处理中）由信号量限流，达到上限后按策略排队（BLOCK）
 *       或拒绝（REJECT），拒绝原因显式可区分，绝不静默丢弃。</li>
 *   <li>下游失败按配置次数重试；重试耗尽后该键熔断，后续提交快速失败，
 *       已排在该键队列中的请求以 FAILED_KEY_CIRCUIT_OPEN 显式完成。</li>
 *   <li>关闭时停止接收新请求，等待已接收数据在超时时间内排空；
 *       超时则明确报告未处理完的请求。下游写入按 requestId 幂等，重试不会写重。</li>
 * </ul>
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final IngestProperties properties;
    private final DownstreamStore store;
    private final Semaphore pendingPermits;
    private final ConcurrentMap<String, KeyLane> lanes = new ConcurrentHashMap<>();
    private final Set<PendingTask> inFlight = ConcurrentHashMap.newKeySet();
    private final IngestMetrics metrics = new IngestMetrics();
    private final ExecutorService workers;
    private volatile boolean accepting = true;

    public IngestService(IngestProperties properties, DownstreamStore store) {
        this.properties = properties;
        this.store = store;
        this.pendingPermits = new Semaphore(properties.getMaxPending());
        this.workers = Executors.newFixedThreadPool(properties.getWorkerThreads());
    }

    /**
     * 提交一条写入请求。接收与拒绝都是显式的，不会静默丢弃。
     */
    public SubmitResult submit(WriteRequest request) {
        if (!accepting) {
            return reject(RejectReason.SHUTTING_DOWN, "ingest channel is shutting down");
        }
        KeyLane lane = lanes.computeIfAbsent(request.key(), k -> new KeyLane());
        if (lane.circuitOpen.get()) {
            return reject(RejectReason.KEY_CIRCUIT_OPEN,
                    "key '" + request.key() + "' failed permanently after retries exhausted");
        }
        if (!acquirePermit()) {
            return reject(rejectReason(),
                    "pending limit " + properties.getMaxPending() + " reached");
        }
        PendingTask task = new PendingTask(request);
        lane.queue.add(task);
        lane.lastActivityMillis = System.currentTimeMillis();
        metrics.onAccepted();
        schedule(lane);
        return SubmitResult.accepted(task.outcome);
    }

    private SubmitResult reject(RejectReason reason, String detail) {
        metrics.onRejected(reason);
        return SubmitResult.rejected(reason, detail);
    }

    private boolean acquirePermit() {
        return switch (properties.getBackpressureStrategy()) {
            case REJECT -> pendingPermits.tryAcquire();
            case BLOCK -> {
                try {
                    yield pendingPermits.tryAcquire(
                            properties.getBlockTimeout().toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    yield false;
                }
            }
        };
    }

    private RejectReason rejectReason() {
        return switch (properties.getBackpressureStrategy()) {
            case REJECT -> RejectReason.QUEUE_FULL;
            case BLOCK -> RejectReason.QUEUE_FULL_TIMEOUT;
        };
    }

    private void schedule(KeyLane lane) {
        if (lane.scheduled.compareAndSet(false, true)) {
            workers.execute(() -> drain(lane));
        }
    }

    /** 串行排空一个键的队列；同一时刻一个键只有一个 drain 在跑。 */
    private void drain(KeyLane lane) {
        try {
            while (true) {
                PendingTask task = lane.queue.poll();
                if (task == null) {
                    lane.scheduled.set(false);
                    // 防止释放调度标志与入队之间的竞态造成漏调度
                    if (!lane.queue.isEmpty() && lane.scheduled.compareAndSet(false, true)) {
                        continue;
                    }
                    return;
                }
                if (lane.circuitOpen.get()) {
                    complete(lane, task, OutcomeStatus.FAILED_KEY_CIRCUIT_OPEN,
                            "key circuit open, dropped without attempting downstream");
                    continue;
                }
                process(lane, task);
            }
        } catch (Throwable t) {
            lane.scheduled.set(false);
            log.error("drain loop crashed, rescheduling lane", t);
            schedule(lane);
        }
    }

    private void process(KeyLane lane, PendingTask task) {
        inFlight.add(task);
        lane.processing.incrementAndGet();
        try {
            int maxAttempts = properties.getMaxRetries() + 1;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    store.write(task.request);
                    complete(lane, task, OutcomeStatus.SUCCESS,
                            attempt > 1 ? "ok after " + attempt + " attempts" : "ok");
                    return;
                } catch (DownstreamException e) {
                    log.warn("downstream write failed, key={} requestId={} attempt={}/{}: {}",
                            task.request.key(), task.request.requestId(), attempt, maxAttempts, e.getMessage());
                    if (attempt < maxAttempts && !backoff(attempt)) {
                        break;
                    }
                }
            }
            complete(lane, task, OutcomeStatus.FAILED_RETRY_EXHAUSTED,
                    "downstream still failing after " + maxAttempts + " attempts");
            // 重试耗尽：熔断该键，后续提交快速失败；队列中剩余的请求显式失败
            lane.circuitOpen.set(true);
        } finally {
            lane.processing.decrementAndGet();
            inFlight.remove(task);
        }
    }

    /** @return false 表示被中断，调用方应停止重试 */
    private boolean backoff(int attempt) {
        try {
            Thread.sleep(properties.getRetryBackoff().toMillis() * attempt);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void complete(KeyLane lane, PendingTask task, OutcomeStatus status, String detail) {
        task.outcome.complete(new WriteOutcome(task.request.requestId(), task.request.key(), status, detail));
        metrics.onOutcome(status);
        if (status == OutcomeStatus.SUCCESS) {
            lane.succeeded.incrementAndGet();
        }
        lane.lastActivityMillis = System.currentTimeMillis();
        pendingPermits.release();
    }

    /**
     * 关闭通道：停止接收新请求，在超时时间内排空已接收数据。
     *
     * @return 关闭超时仍未来得及处理的请求；为空表示全部处理完
     */
    @PreDestroy
    public List<WriteRequest> shutdown() {
        accepting = false;
        boolean drained;
        try {
            // 拿满全部许可即说明没有排队/处理中的请求
            drained = pendingPermits.tryAcquire(properties.getMaxPending(),
                    properties.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            drained = false;
        }
        List<WriteRequest> leftovers = new ArrayList<>();
        if (drained) {
            // 排空成功：归还探测用许可，避免重复调用 shutdown 时误报超时
            pendingPermits.release(properties.getMaxPending());
            log.info("ingest channel shutdown: all accepted requests processed");
        } else {
            for (KeyLane lane : lanes.values()) {
                PendingTask task;
                while ((task = lane.queue.poll()) != null) {
                    complete(lane, task, OutcomeStatus.NOT_PROCESSED_SHUTDOWN_TIMEOUT,
                            "shutdown timeout before processing");
                    leftovers.add(task.request);
                }
            }
            inFlight.forEach(task -> leftovers.add(task.request));
            log.warn("ingest channel shutdown: timeout, {} request(s) not processed: {}",
                    leftovers.size(), leftovers.stream().map(WriteRequest::requestId).toList());
        }
        workers.shutdown();
        return leftovers;
    }

    /** 当前待处理量（排队中 + 处理中），用于监控与测试。 */
    public int pendingCount() {
        return properties.getMaxPending() - pendingPermits.availablePermits();
    }

    /** 全局运行指标快照。 */
    public IngestStats stats() {
        return new IngestStats(
                metrics.received(),
                metrics.succeeded(),
                metrics.failed(),
                pendingCount(),
                metrics.rejectedTotal(),
                metrics.rejectedSnapshot(),
                lanes.size());
    }

    /** 每个业务键当前的运行状态快照。 */
    public List<KeyStats> keyStats() {
        long now = System.currentTimeMillis();
        List<KeyStats> stats = new ArrayList<>();
        lanes.forEach((key, lane) -> stats.add(new KeyStats(
                key,
                lane.queue.size(),
                lane.processing.get(),
                lane.circuitOpen.get(),
                0,
                lane.succeeded.get())));
        return List.copyOf(stats);
    }

    private static final class KeyLane {
        final ConcurrentLinkedQueue<PendingTask> queue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean scheduled = new AtomicBoolean();
        final AtomicBoolean circuitOpen = new AtomicBoolean();
        /** 正在处理（含重试中）的请求数。 */
        final AtomicInteger processing = new AtomicInteger();
        /** 该键已成功写入下游的请求总数。 */
        final AtomicLong succeeded = new AtomicLong();
        /** 最近一次提交或处理完成的时间，用于空闲状态回收。 */
        volatile long lastActivityMillis = System.currentTimeMillis();
    }

    private static final class PendingTask {
        final WriteRequest request;
        final CompletableFuture<WriteOutcome> outcome = new CompletableFuture<>();

        PendingTask(WriteRequest request) {
            this.request = request;
        }
    }
}
