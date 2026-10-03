package com.github.feng111627.asyncingest.ingest;

/**
 * 提交被拒绝的原因，调用方可据此区分处理。
 */
public enum RejectReason {
    /** 待处理量达到上限，REJECT 策略下立即拒绝。 */
    QUEUE_FULL,
    /** BLOCK 策略下等待空位超时。 */
    QUEUE_FULL_TIMEOUT,
    /** 进程正在关闭，不再接收新请求。 */
    SHUTTING_DOWN,
    /** 该键重试耗尽已被熔断，后续请求快速失败。 */
    KEY_CIRCUIT_OPEN
}
