package com.github.feng111627.asyncingest.ingest;

/**
 * 下游存储抽象。写入必须按 requestId 幂等：同一 requestId 重复写入不产生重复效果。
 */
public interface DownstreamStore {

    void write(WriteRequest request) throws DownstreamException;
}
