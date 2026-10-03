package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单个业务键的串行通道：同一键的命令按提交顺序逐批写入下游，
 * 不同键的通道互不影响。重试耗尽后熔断，该键后续请求快速失败。
 */
final class KeyLane {

    private static final Logger log = LoggerFactory.getLogger(KeyLane.class);

    private final String key;
    private final ConcurrentLinkedQueue<WriteCommand> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean circuitOpen = new AtomicBoolean(false);
    private final AtomicLong sequence = new AtomicLong(0);

    private final Executor executor;
    private final DownstreamStore store;
    private final IngestProperties props;
    /** 全局待处理许可，每条命令完结（成功或失败）时归还一个 */
    private final Semaphore permits;

    KeyLane(String key, Executor executor, DownstreamStore store,
            IngestProperties props, Semaphore permits) {
        this.key = key;
        this.executor = executor;
        this.store = store;
        this.props = props;
        this.permits = permits;
    }

    String key() {
        return key;
    }

    boolean isCircuitOpen() {
        return circuitOpen.get();
    }

    boolean isIdle() {
        return queue.isEmpty() && !running.get();
    }

    void offer(WriteCommand cmd) {
        queue.add(cmd);
        trySchedule();
    }

    private void trySchedule() {
        if (!queue.isEmpty() && running.compareAndSet(false, true)) {
            try {
                executor.execute(this::drain);
            } catch (RuntimeException e) {
                running.set(false);
                throw e;
            }
        }
    }

    private void drain() {
        try {
            while (true) {
                if (circuitOpen.get()) {
                    failAllQueued();
                    return;
                }
                List<WriteCommand> batch = pollBatch();
                if (batch.isEmpty()) {
                    return;
                }
                writeBatchWithRetry(batch);
            }
        } finally {
            running.set(false);
            // 防止在退出前一刻有新命令入队却无人调度
            if (!queue.isEmpty()) {
                trySchedule();
            }
        }
    }

    private List<WriteCommand> pollBatch() {
        List<WriteCommand> batch = new ArrayList<>(props.getMaxBatchSize());
        WriteCommand cmd;
        while (batch.size() < props.getMaxBatchSize() && (cmd = queue.poll()) != null) {
            batch.add(cmd);
        }
        return batch;
    }

    private void writeBatchWithRetry(List<WriteCommand> batch) {
        List<String> payloads = batch.stream().map(WriteCommand::payload).toList();
        int maxAttempts = props.getMaxRetries() + 1;
        RuntimeException lastError = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                store.writeBatch(key, payloads);
                for (WriteCommand cmd : batch) {
                    cmd.result().complete(new WriteAck(key, sequence.getAndIncrement()));
                    permits.release();
                }
                return;
            } catch (RuntimeException e) {
                lastError = e;
                log.warn("key={} 下游写入失败，第 {}/{} 次尝试: {}", key, attempt, maxAttempts, e.getMessage());
                if (attempt < maxAttempts) {
                    sleep(props.getRetryBackoffMs());
                }
            }
        }
        // 重试耗尽：本批失败 + 熔断，该键后续请求快速失败
        log.error("key={} 重试 {} 次后仍失败，熔断该键通道", key, props.getMaxRetries());
        WriteFailedException failure = new WriteFailedException(
                "下游写入重试耗尽, key=" + key, lastError);
        for (WriteCommand cmd : batch) {
            cmd.result().completeExceptionally(failure);
            permits.release();
        }
        circuitOpen.set(true);
        failAllQueued();
    }

    /** 熔断后把队列里剩余的命令全部快速失败。 */
    private void failAllQueued() {
        WriteCommand cmd;
        while ((cmd = queue.poll()) != null) {
            cmd.result().completeExceptionally(new IngestRejectedException(
                    RejectReason.LANE_CIRCUIT_OPEN, "该键通道已熔断, key=" + key));
            permits.release();
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
