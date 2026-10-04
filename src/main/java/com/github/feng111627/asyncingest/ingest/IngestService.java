package com.github.feng111627.asyncingest.ingest;

import com.github.feng111627.asyncingest.config.IngestProperties;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import java.util.concurrent.atomic.LongAdder;
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
 *   <li>下游失败按配置次数重试；重试耗尽后该键熔断，后续提交快速失败。
 *       熔断冷却 {@code ingest.circuit-cooldown} 到期后自动进入<b>半开</b>状态：
 *       只放行一条试探请求，试探成功则熔断关闭、该键恢复正常；试探失败则重新熔断，
 *       冷却重新计时，期间不会往下游堆更多失败请求。</li>
 *   <li>长期空闲（无排队、无处理中、无新提交）的键，其运行状态会被后台回收，
 *       回收不算失败也不丢数据；回收后再次提交同一个键按新键正常处理。
 *       空闲时长 {@code ingest.key-idle-timeout} 与保留上限 {@code ingest.max-keys} 可配。</li>
 *   <li>关闭时停止接收新请求，等待已接收数据在超时时间内排空；
 *       超时则明确报告未处理完的请求。下游写入按 requestId 幂等，重试不会写重。</li>
 *   <li>全局与每键的运行指标通过 {@link #globalStats()} / {@link #keyStatus(String)} 查询，
 *       计数在并发下准确：每条提交只计入接收或拒绝之一，每条被接收请求只计入成功或失败之一。</li>
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
    private final ExecutorService workers;
    private final ScheduledExecutorService reaper;
    private final IngestMetrics metrics = new IngestMetrics();
    private volatile boolean accepting = true;

    public IngestService(IngestProperties properties, DownstreamStore store) {
        this.properties = properties;
        this.store = store;
        this.pendingPermits = new Semaphore(properties.getMaxPending());
        this.workers = Executors.newFixedThreadPool(properties.getWorkerThreads());
        this.reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ingest-key-reaper");
            t.setDaemon(true);
            return t;
        });
        long interval = properties.getReapInterval().toMillis();
        this.reaper.scheduleWithFixedDelay(this::reapIdleKeysSafely, interval, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * 提交一条写入请求。接收与拒绝都是显式的，不会静默丢弃。
     */
    public SubmitResult submit(WriteRequest request) {
        if (!accepting) {
            metrics.onRejected(RejectReason.SHUTTING_DOWN);
            return SubmitResult.rejected(RejectReason.SHUTTING_DOWN, "ingest channel is shutting down");
        }
        long cooldownMillis = properties.getCircuitCooldown().toMillis();
        while (true) {
            KeyLane lane = lanes.computeIfAbsent(request.key(), k -> new KeyLane());
            synchronized (lane) {
                if (lane.retired) {
                    // 状态刚被回收线程摘除，换一条新通道重来
                    continue;
                }
                if (lane.circuit == CircuitState.OPEN) {
                    if (System.currentTimeMillis() - lane.circuitOpenedAtMillis >= cooldownMillis) {
                        // 冷却结束：进入半开，只放行这一条试探请求
                        lane.circuit = CircuitState.HALF_OPEN;
                        log.info("key '{}' circuit half-open, allowing one probe request", request.key());
                    } else {
                        metrics.onRejected(RejectReason.KEY_CIRCUIT_OPEN);
                        return SubmitResult.rejected(RejectReason.KEY_CIRCUIT_OPEN,
                                "key '" + request.key() + "' circuit open, still cooling down");
                    }
                } else if (lane.circuit == CircuitState.HALF_OPEN) {
                    // 半开期间只容忍一条试探请求，其余继续快速失败
                    metrics.onRejected(RejectReason.KEY_CIRCUIT_OPEN);
                    return SubmitResult.rejected(RejectReason.KEY_CIRCUIT_OPEN,
                            "key '" + request.key() + "' half-open probe in flight");
                }
                if (!acquirePermit()) {
                    RejectReason reason = rejectReason();
                    metrics.onRejected(reason);
                    return SubmitResult.rejected(reason,
                            "pending limit " + properties.getMaxPending() + " reached");
                }
                PendingTask task = new PendingTask(request, lane.circuit == CircuitState.HALF_OPEN);
                lane.queue.add(task);
                lane.lastActivityMillis = System.currentTimeMillis();
                metrics.onAccepted();
                schedule(lane);
                return SubmitResult.accepted(task.outcome);
            }
        }
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
                CircuitState circuit = lane.circuit;
                if (circuit == CircuitState.OPEN || (circuit == CircuitState.HALF_OPEN && !task.probe)) {
                    // 熔断中，或半开期间混入的非试探请求：快速失败，不碰下游
                    complete(task, OutcomeStatus.FAILED_KEY_CIRCUIT_OPEN,
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
        lane.inFlightCount.incrementAndGet();
        try {
            int maxAttempts = properties.getMaxRetries() + 1;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    store.write(task.request);
                    lane.successCount.increment();
                    if (task.probe) {
                        // 试探成功：先释放许可再关熔断（避免与持锁等许可的提交方死锁），
                        // 最后完成结果——调用方观察到成功时该键已恢复
                        pendingPermits.release();
                        closeCircuitIfHalfOpen(lane);
                        finish(task, OutcomeStatus.SUCCESS,
                                attempt > 1 ? "ok after " + attempt + " attempts" : "ok");
                    } else {
                        complete(task, OutcomeStatus.SUCCESS,
                                attempt > 1 ? "ok after " + attempt + " attempts" : "ok");
                    }
                    return;
                } catch (DownstreamException e) {
                    log.warn("downstream write failed, key={} requestId={} attempt={}/{}: {}",
                            task.request.key(), task.request.requestId(), attempt, maxAttempts, e.getMessage());
                    if (attempt < maxAttempts && !backoff(attempt)) {
                        break;
                    }
                }
            }
            // 重试耗尽：先释放许可再熔断（避免与持锁等许可的提交方死锁），最后完成结果——
            // 调用方观察到 FAILED_RETRY_EXHAUSTED 时熔断已生效
            pendingPermits.release();
            openCircuit(lane, task.probe);
            finish(task, OutcomeStatus.FAILED_RETRY_EXHAUSTED,
                    "downstream still failing after " + maxAttempts + " attempts");
        } finally {
            inFlight.remove(task);
            lane.inFlightCount.decrementAndGet();
            lane.lastActivityMillis = System.currentTimeMillis();
        }
    }

    private void openCircuit(KeyLane lane, boolean wasProbe) {
        synchronized (lane) {
            lane.circuit = CircuitState.OPEN;
            lane.circuitOpenedAtMillis = System.currentTimeMillis();
        }
        log.warn("key circuit opened{}; fast-fail until cooldown elapses",
                wasProbe ? " again (half-open probe failed)" : "");
    }

    private void closeCircuitIfHalfOpen(KeyLane lane) {
        synchronized (lane) {
            if (lane.circuit == CircuitState.HALF_OPEN) {
                lane.circuit = CircuitState.CLOSED;
            }
        }
        log.info("half-open probe succeeded, key circuit closed");
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

    /** 记账并完成结果，然后释放许可。 */
    private void complete(PendingTask task, OutcomeStatus status, String detail) {
        finish(task, status, detail);
        pendingPermits.release();
    }

    /** 先记账再完成结果：调用方观察到结果时，指标与键状态已同步更新。 */
    private void finish(PendingTask task, OutcomeStatus status, String detail) {
        metrics.onOutcome(status);
        task.outcome.complete(new WriteOutcome(task.request.requestId(), task.request.key(), status, detail));
    }

    /** 回收长期空闲的键状态；只收空闲键，不影响在途请求，回收不算失败。 */
    private void reapIdleKeysSafely() {
        try {
            reapIdleKeys();
        } catch (Throwable t) {
            log.error("key reaper run failed", t);
        }
    }

    private void reapIdleKeys() {
        long now = System.currentTimeMillis();
        long idleMillis = properties.getKeyIdleTimeout().toMillis();
        int reaped = 0;
        for (Map.Entry<String, KeyLane> entry : lanes.entrySet()) {
            KeyLane lane = entry.getValue();
            synchronized (lane) {
                if (!lane.retired && lane.isIdle(now, idleMillis)) {
                    lane.retired = true;
                    lanes.remove(entry.getKey(), lane);
                    reaped++;
                }
            }
        }
        // 键数超上限：从最久未活动的空闲键开始继续回收（不要求满足 idleTimeout，但仍只收空闲键）
        int excess = lanes.size() - properties.getMaxKeys();
        if (excess > 0) {
            List<Map.Entry<String, KeyLane>> candidates = new ArrayList<>(lanes.entrySet());
            candidates.sort(Comparator.comparingLong(e -> e.getValue().lastActivityMillis));
            for (Map.Entry<String, KeyLane> entry : candidates) {
                if (excess <= 0) {
                    break;
                }
                KeyLane lane = entry.getValue();
                synchronized (lane) {
                    if (!lane.retired && lane.isIdle(now, 0)) {
                        lane.retired = true;
                        lanes.remove(entry.getKey(), lane);
                        excess--;
                        reaped++;
                    }
                }
            }
        }
        if (reaped > 0) {
            log.info("reaped {} idle key state(s), tracked keys now {}", reaped, lanes.size());
        }
    }

    /**
     * 关闭通道：停止接收新请求，在超时时间内排空已接收数据。
     *
     * @return 关闭超时仍未来得及处理的请求；为空表示全部处理完
     */
    @PreDestroy
    public List<WriteRequest> shutdown() {
        accepting = false;
        reaper.shutdownNow();
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
                    complete(task, OutcomeStatus.NOT_PROCESSED_SHUTDOWN_TIMEOUT,
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

    /** 全局指标快照：接收 / 成功 / 失败 / 按原因区分的拒绝。 */
    public IngestMetrics.GlobalStats globalStats() {
        return metrics.snapshot();
    }

    /** 单个键的运行状态；键不存在（或已被回收）时为空。 */
    public Optional<KeyStatus> keyStatus(String key) {
        KeyLane lane = lanes.get(key);
        return lane == null ? Optional.empty() : Optional.of(snapshotOf(key, lane));
    }

    /** 当前所有被跟踪键的运行状态。 */
    public List<KeyStatus> keyStatuses() {
        List<KeyStatus> result = new ArrayList<>();
        lanes.forEach((key, lane) -> result.add(snapshotOf(key, lane)));
        return result;
    }

    /** 当前被跟踪的键数量，用于监控与测试。 */
    public int trackedKeyCount() {
        return lanes.size();
    }

    private KeyStatus snapshotOf(String key, KeyLane lane) {
        CircuitState circuit = lane.circuit;
        long cooldownRemaining = 0;
        if (circuit == CircuitState.OPEN) {
            cooldownRemaining = Math.max(0, properties.getCircuitCooldown().toMillis()
                    - (System.currentTimeMillis() - lane.circuitOpenedAtMillis));
        }
        return new KeyStatus(key,
                lane.queue.size() + lane.inFlightCount.get(),
                circuit != CircuitState.CLOSED,
                circuit == CircuitState.HALF_OPEN,
                cooldownRemaining,
                lane.successCount.sum());
    }

    private enum CircuitState {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private static final class KeyLane {
        final ConcurrentLinkedQueue<PendingTask> queue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean scheduled = new AtomicBoolean();
        final AtomicInteger inFlightCount = new AtomicInteger();
        final LongAdder successCount = new LongAdder();
        volatile CircuitState circuit = CircuitState.CLOSED;
        volatile long circuitOpenedAtMillis;
        volatile long lastActivityMillis = System.currentTimeMillis();
        /** 是否已被回收线程摘除；由 synchronized(this) 保护，提交方据此换用新通道。 */
        boolean retired;

        /** 空闲 = 无排队、无处理中、无调度中的 drain，且空闲时长达标。 */
        boolean isIdle(long now, long idleMillis) {
            return queue.isEmpty()
                    && inFlightCount.get() == 0
                    && !scheduled.get()
                    && now - lastActivityMillis >= idleMillis;
        }
    }

    private static final class PendingTask {
        final WriteRequest request;
        final boolean probe;
        final CompletableFuture<WriteOutcome> outcome = new CompletableFuture<>();

        PendingTask(WriteRequest request, boolean probe) {
            this.request = request;
            this.probe = probe;
        }
    }
}
