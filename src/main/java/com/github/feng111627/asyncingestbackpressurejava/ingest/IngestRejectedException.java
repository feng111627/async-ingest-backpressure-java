package com.github.feng111627.asyncingestbackpressurejava.ingest;

/**
 * 提交被拒绝时抛出，携带可区分的拒绝原因。
 */
public class IngestRejectedException extends RuntimeException {
    private final RejectReason reason;

    public IngestRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
