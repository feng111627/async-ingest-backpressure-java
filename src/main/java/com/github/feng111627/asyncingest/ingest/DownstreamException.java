package com.github.feng111627.asyncingest.ingest;

/**
 * 下游存储写入失败。
 */
public class DownstreamException extends Exception {

    public DownstreamException(String message) {
        super(message);
    }
}
