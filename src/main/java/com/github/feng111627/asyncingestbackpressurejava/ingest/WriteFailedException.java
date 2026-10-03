package com.github.feng111627.asyncingestbackpressurejava.ingest;

/**
 * 下游写入在重试耗尽后仍然失败，携带最后一次下游异常。
 */
public class WriteFailedException extends RuntimeException {
    public WriteFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
