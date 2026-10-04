package com.github.feng111627.asyncingest.ingest;

/**
 * 单个业务键的运行状态快照（只读，供监控查询）。
 *
 * @param key                    业务键
 * @param backlog                当前积压：排队中 + 处理中的请求数
 * @param circuitBlocked         是否因连续失败被挡住（熔断打开或半开试探中）
 * @param circuitHalfOpen        是否正处于半开试探状态（已放行一条试探请求）
 * @param cooldownRemainingMillis 熔断冷却剩余毫秒数；未被挡住时为 0
 * @param successCount           该键已成功写入的条数
 */
public record KeyStatus(
        String key,
        int backlog,
        boolean circuitBlocked,
        boolean circuitHalfOpen,
        long cooldownRemainingMillis,
        long successCount) {
}
