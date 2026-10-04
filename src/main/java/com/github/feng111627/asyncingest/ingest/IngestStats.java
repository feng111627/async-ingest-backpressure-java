package com.github.feng111627.asyncingest.ingest;

import java.util.Map;

/**
 * 通道全局运行指标快照。
 *
 * @param received        已接收（进入通道）的请求总数
 * @param succeeded       已成功写入下游的请求总数
 * @param failed          最终失败的请求总数（重试耗尽 / 熔断丢弃 / 关闭超时未处理）
 * @param pending         当前待处理量（排队中 + 处理中）
 * @param rejectedTotal   被拒绝的请求总数
 * @param rejectedByReason 按原因细分的拒绝数（QUEUE_FULL / QUEUE_FULL_TIMEOUT / SHUTTING_DOWN / KEY_CIRCUIT_OPEN）
 * @param trackedKeys     当前仍保留运行状态的业务键数量
 */
public record IngestStats(long received,
                          long succeeded,
                          long failed,
                          long pending,
                          long rejectedTotal,
                          Map<RejectReason, Long> rejectedByReason,
                          int trackedKeys) {
}
