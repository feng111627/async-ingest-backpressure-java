package com.github.feng111627.asyncingest.ingest;

import java.util.concurrent.CompletableFuture;

/**
 * 提交结果：要么被接收并返回最终结果的 future，要么被拒绝并给出可区分的原因。
 */
public sealed interface SubmitResult {

    record Accepted(CompletableFuture<WriteOutcome> outcome) implements SubmitResult {
    }

    record Rejected(RejectReason reason, String detail) implements SubmitResult {
    }

    static SubmitResult accepted(CompletableFuture<WriteOutcome> outcome) {
        return new Accepted(outcome);
    }

    static SubmitResult rejected(RejectReason reason, String detail) {
        return new Rejected(reason, detail);
    }
}
