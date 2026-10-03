package com.github.feng111627.asyncingest.ingest;

/**
 * 一条已接收请求的最终结果。
 */
public record WriteOutcome(String requestId, String key, OutcomeStatus status, String detail) {

    public static WriteOutcome success(String requestId, String key) {
        return new WriteOutcome(requestId, key, OutcomeStatus.SUCCESS, "ok");
    }

    public static WriteOutcome failed(String requestId, String key, OutcomeStatus status, String detail) {
        return new WriteOutcome(requestId, key, status, detail);
    }
}
