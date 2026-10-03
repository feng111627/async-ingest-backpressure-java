package com.github.feng111627.asyncingestbackpressurejava.ingest;

/**
 * 提交被拒绝的原因，调用方可据此区分处理。
 */
public enum RejectReason {
    /** 待处理量达到上限，且策略为直接拒绝 */
    QUEUE_FULL,
    /** 排队策略下等待超时仍未获得容量 */
    OFFER_TIMEOUT,
    /** 该业务键重试耗尽，进入快速失败状态 */
    LANE_CIRCUIT_OPEN,
    /** 进程正在关闭，不再接收新请求 */
    SHUTTING_DOWN
}
