package com.github.feng111627.asyncingest.ingest;

import com.github.feng111627.asyncingest.config.IngestProperties;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
 *   <li>下游失败按配置次数重试；重试耗尽后该键熔断进入冷却期，冷却期内后续提交快速失败，
 *       已排在该键队列中的请求以 FAILED_KEY_CIRCUIT_OPEN 显式完成。冷却结束后放行一条
 *       试探请求：试探成功则恢复该键，试探失败则重新冷却，期间不会向下游堆积更多请求。</li>
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
    private final ScheduledExecutorService sweeper;
    private volatile boolean accepting = true;

    public IngestService(IngestProperties properties, DownstreamStore store) {
        this.properties = properties;
        this.store = store;
        this.pendingPermits = new Semaphore(properties.getMaxPending());
        this.workers = Executors.newFixedThreadPool(properties.getWorkerThreads());
        this.sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "ingest-key-evictor");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMillis = properties.getEvictionInterval().toMillis();
        this.sweeper.scheduleWithFixedDelay(this::evictIdleKeysSafely,
                intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 提交一条写入请求。接收与拒绝都是显式的，不会静默丢弃。
     */
    public SubmitResult submit(WriteRequest request) {
        if (!accepting) {
            return reject(RejectReason.SHUTTING_DOWN, "ingest channel is shutting down");
        }
        KeyLane lane = lanes.computeIfAbsent(request.key(), k -> new KeyLane());
        boolean probe = false;
        if (lane.circuitOpen.get()) {
            long retryAfter = lane.circuitReopenAtMillis - System.currentTimeMillis();
            if (retryAfter > 0) {
                // 冷却期内：保持快速失败
                return reject(RejectReason.KEY_CIRCUIT_OPEN,
                        "key '" + request.key() + "' circuit open, retry after " + retryAfter + "ms");
            }
            // 冷却结束：CAS 保证只放行一条试探请求，其余继续快速失败
            if (!lane.probeInFlight.compareAndSet(false, true)) {
                return reject(RejectReason.KEY_CIRCUIT_OPEN,
                        "key '" + request.key() + "' circuit half-open, probe already in flight");
            }
            probe = true;
        }
        if (!acquirePermit()) {
            if (probe) {
                lane.probeInFlight.set(false);
            }
            return reject(rejectReason(),
                    "pending limit " + properties.getMaxPending() + " reached");
        }
        PendingTask task = new PendingTask(request, probe);
        synchronized (lane) {
            if (lane.evicted) {
                // 拿到 lane 后它刚好被回收：释放许可与试探名额，按新键重新走一遍流程
                pendingPermits.release();
                if (probe) {
                    lane.probeInFlight.set(false);
                }
                return submit(request);
            }
            lane.queue.add(task);
            lane.lastActivityMillis = System.currentTimeMillis();
        }
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
                if (lane.circuitOpen.get() && !task.probe) {
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
                    if (task.probe) {
                        // 试探成功：恢复该键，后续请求照常处理
                        lane.circuitOpen.set(false);
                        lane.probeInFlight.set(false);
                        log.info("key '{}' circuit closed: probe request {} succeeded",
                                task.request.key(), task.request.requestId());
                    }
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
            // 重试耗尽：熔断该键并进入冷却期，后续提交快速失败；队列中剩余的请求显式失败。
            // 试探请求失败同样走这里：重新冷却，不再放行更多请求，避免给下游堆压力。
            lane.circuitOpen.set(true);
            lane.circuitReopenAtMillis = System.currentTimeMillis() + properties.getCircuitCooldown().toMillis();
            lane.probeInFlight.set(false);
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
        // 先更新统计再完成 future：调用方拿到结果时，统计一定已反映该结果
        metrics.onOutcome(status);
        if (status == OutcomeStatus.SUCCESS) {
            lane.succeeded.incrementAndGet();
        }
        lane.lastActivityMillis = System.currentTimeMillis();
        pendingPermits.release();
        task.outcome.complete(new WriteOutcome(task.request.requestId(), task.request.key(), status, detail));
    }

    /**
     * 关闭通道：停止接收新请求，在超时时间内排空已接收数据。
     *
     * @return 关闭超时仍未来得及处理的请求；为空表示全部处理完
     */
    @PreDestroy
    public List<WriteRequest> shutdown() {
        accepting = false;
        sweeper.shutdownNow();
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

    /**
     * 空闲键状态回收。只回收"空队列 + 无处理中 + 无调度中"的 lane，
     * 正在处理的请求不受影响；回收不触碰任何已接收数据，只是丢弃该键的运行状态
     * （熔断标记、成功计数等），之后同键提交按新键处理。
     */
    private void evictIdleKeysSafely() {
        try {
            evictIdleKeys();
        } catch (Throwable t) {
            log.warn("idle key eviction sweep failed", t);
        }
    }

    private void evictIdleKeys() {
        long now = System.currentTimeMillis();
        long idleMillis = properties.getKeyIdleTimeout().toMillis();
        lanes.forEach((key, lane) -> {
            if (now - lane.lastActivityMillis >= idleMillis) {
                tryEvict(key, lane);
            }
        });
        // 超出键数上限时，优先回收最久未活动的空闲键
        int excess = lanes.size() - properties.getMaxKeys();
        if (excess > 0) {
            lanes.entrySet().stream()
                    .filter(entry -> isIdle(entry.getValue()))
                    .sorted(Comparator.comparingLong(entry -> entry.getValue().lastActivityMillis))
                    .limit(excess)
                    .forEach(entry -> tryEvict(entry.getKey(), entry.getValue()));
        }
    }

    /** lane 空闲的判定：无排队、无处理中、无调度中的 drain。 */
    private boolean isIdle(KeyLane lane) {
        return lane.queue.isEmpty() && lane.processing.get() == 0 && !lane.scheduled.get();
    }

    /**
     * 与 submit 的入队共用 lane 监视器互斥：要么入队在前（lane 不再空闲，放弃回收），
     * 要么回收在前（submit 看到 evicted 标记后换新 lane 重试），不会丢请求。
     */
    private void tryEvict(String key, KeyLane lane) {
        synchronized (lane) {
            if (!isIdle(lane)) {
                return;
            }
            lane.evicted = true;
            lanes.remove(key, lane);
            log.debug("evicted idle state of key '{}'", key);
        }
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
                lane.circuitOpen.get() ? Math.max(0, lane.circuitReopenAtMillis - now) : 0,
                lane.succeeded.get())));
        return List.copyOf(stats);
    }

    private static final class KeyLane {
        final ConcurrentLinkedQueue<PendingTask> queue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean scheduled = new AtomicBoolean();
        final AtomicBoolean circuitOpen = new AtomicBoolean();
        /** 半开状态下是否已有一条试探请求在飞，保证一次只放一条。 */
        final AtomicBoolean probeInFlight = new AtomicBoolean();
        /** 熔断冷却截止时刻（epoch millis），到点后允许一条试探请求通过。 */
        volatile long circuitReopenAtMillis;
        /** 正在处理（含重试中）的请求数。 */
        final AtomicInteger processing = new AtomicInteger();
        /** 该键已成功写入下游的请求总数。 */
        final AtomicLong succeeded = new AtomicLong();
        /** 最近一次提交或处理完成的时间，用于空闲状态回收。 */
        volatile long lastActivityMillis = System.currentTimeMillis();
        /** 是否已被回收：回收后 submit 不得再向该 lane 入队，需换新 lane。 */
        volatile boolean evicted;
    }

    private static final class PendingTask {
        final WriteRequest request;
        /** 是否为熔断冷却结束后的试探请求。 */
        final boolean probe;
        final CompletableFuture<WriteOutcome> outcome = new CompletableFuture<>();

        PendingTask(WriteRequest request, boolean probe) {
            this.request = request;
            this.probe = probe;
        }
    }
}
