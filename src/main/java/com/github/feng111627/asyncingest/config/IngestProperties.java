package com.github.feng111627.asyncingest.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 异步写入通道的可配置项。
 */
@ConfigurationProperties(prefix = "ingest")
public class IngestProperties {

    /** 全局待处理（排队中 + 处理中）请求上限，达到后触发背压。 */
    private int maxPending = 1000;

    /** 背压策略：REJECT 立即拒绝；BLOCK 阻塞等待直到有空位或超时。 */
    private BackpressureStrategy backpressureStrategy = BackpressureStrategy.REJECT;

    /** BLOCK 策略下等待空位的最长时间，超时后按 QUEUE_FULL_TIMEOUT 拒绝。 */
    private Duration blockTimeout = Duration.ofSeconds(5);

    /** 下游失败时的最大重试次数（不含首次尝试）。 */
    private int maxRetries = 3;

    /** 重试间隔基数，实际等待按次数线性放大。 */
    private Duration retryBackoff = Duration.ofMillis(50);

    /** 处理线程池大小，不同键之间并行推进的并发度。 */
    private int workerThreads = 8;

    /** 收到关闭信号后等待已接收数据处理完的最长时间。 */
    private Duration shutdownTimeout = Duration.ofSeconds(10);

    public enum BackpressureStrategy {
        REJECT,
        BLOCK
    }

    public int getMaxPending() {
        return maxPending;
    }

    public void setMaxPending(int maxPending) {
        this.maxPending = maxPending;
    }

    public BackpressureStrategy getBackpressureStrategy() {
        return backpressureStrategy;
    }

    public void setBackpressureStrategy(BackpressureStrategy backpressureStrategy) {
        this.backpressureStrategy = backpressureStrategy;
    }

    public Duration getBlockTimeout() {
        return blockTimeout;
    }

    public void setBlockTimeout(Duration blockTimeout) {
        this.blockTimeout = blockTimeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Duration getRetryBackoff() {
        return retryBackoff;
    }

    public void setRetryBackoff(Duration retryBackoff) {
        this.retryBackoff = retryBackoff;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }
}
