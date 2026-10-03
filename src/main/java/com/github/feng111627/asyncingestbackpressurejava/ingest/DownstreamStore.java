package com.github.feng111627.asyncingestbackpressurejava.ingest;

import java.util.List;

/**
 * 下游存储抽象。实现必须保证一批写入要么全部生效、要么全部不生效（抛异常），
 * 不允许写一半。
 */
public interface DownstreamStore {
    /**
     * 原子地写入同一业务键的一批数据。
     *
     * @throws RuntimeException 写入失败，整批未生效
     */
    void writeBatch(String key, List<String> payloads);
}
