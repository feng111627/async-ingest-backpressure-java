package com.github.feng111627.asyncingest.ingest;

/**
 * 已接收请求的最终处理结果。
 */
public enum OutcomeStatus {
    /** 写入成功（含重试后成功与幂等去重命中）。 */
    SUCCESS,
    /** 重试耗尽仍失败。 */
    FAILED_RETRY_EXHAUSTED,
    /** 因该键熔断被丢弃（排在该键失败请求之后）。 */
    FAILED_KEY_CIRCUIT_OPEN,
    /** 关闭超时，未来得及处理。 */
    NOT_PROCESSED_SHUTDOWN_TIMEOUT
}
