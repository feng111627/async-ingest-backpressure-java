package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 异步写入通道配置。
 */
@ConfigurationProperties(prefix = "ingest")
public class IngestProperties {

    public enum BackpressureStrategy {
        /** 容量满时立即拒绝 */
        REJECT,
        /** 容量满时排队等待，超时后拒绝 */
        BLOCK
    }

    /** 全局待处理上限（排队中 + 处理中） */
    private int maxPending = 1000;
    /** 背压策略 */
    private BackpressureStrategy backpressureStrategy = BackpressureStrategy.REJECT;
    /** BLOCK 策略下排队等待容量的超时（毫秒） */
    private long offerTimeoutMs = 200;
    /** 每批最大条数 */
    private int maxBatchSize = 32;
    /** 单批失败后的最大重试次数 */
    private int maxRetries = 3;
    /** 重试间隔（毫秒） */
    private long retryBackoffMs = 50;
    /** 工作线程数 */
    private int workerThreads = 4;
    /** 关闭时等待已接收数据处理完的超时（毫秒） */
    private long shutdownTimeoutMs = 10_000;

    public int getMaxPending() { return maxPending; }
    public void setMaxPending(int maxPending) { this.maxPending = maxPending; }
    public BackpressureStrategy getBackpressureStrategy() { return backpressureStrategy; }
    public void setBackpressureStrategy(BackpressureStrategy s) { this.backpressureStrategy = s; }
    public long getOfferTimeoutMs() { return offerTimeoutMs; }
    public void setOfferTimeoutMs(long offerTimeoutMs) { this.offerTimeoutMs = offerTimeoutMs; }
    public int getMaxBatchSize() { return maxBatchSize; }
    public void setMaxBatchSize(int maxBatchSize) { this.maxBatchSize = maxBatchSize; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public long getRetryBackoffMs() { return retryBackoffMs; }
    public void setRetryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public long getShutdownTimeoutMs() { return shutdownTimeoutMs; }
    public void setShutdownTimeoutMs(long shutdownTimeoutMs) { this.shutdownTimeoutMs = shutdownTimeoutMs; }
}
