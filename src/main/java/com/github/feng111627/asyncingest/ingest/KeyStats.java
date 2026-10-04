package com.github.feng111627.asyncingest.ingest;

/**
 * 单个业务键的运行状态快照。
 *
 * @param key                    业务键
 * @param queued                 该键排队等待处理的请求数
 * @param processing             该键正在处理（含重试中）的请求数
 * @param circuitOpen            该键是否因连续失败被熔断挡住
 * @param circuitRetryAfterMillis 熔断状态下距离下次放行试探的剩余毫秒；未熔断为 0
 * @param succeeded              该键已成功写入下游的请求总数
 */
public record KeyStats(String key,
                       long queued,
                       long processing,
                       boolean circuitOpen,
                       long circuitRetryAfterMillis,
                       long succeeded) {
}
