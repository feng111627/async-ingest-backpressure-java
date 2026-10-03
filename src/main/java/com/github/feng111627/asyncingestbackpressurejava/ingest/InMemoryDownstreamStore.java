package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地内存模拟的下游存储：整批原子生效，支持注入延迟与失败用于测试。
 * 失败在生效之前抛出，因此不会产生写一半的情况。
 */
@Component
public class InMemoryDownstreamStore implements DownstreamStore {

    private final Map<String, CopyOnWriteArrayList<String>> data = new ConcurrentHashMap<>();
    private final Map<String, Long> keyLatencyMs = new ConcurrentHashMap<>();
    private final AtomicLong latencyMs = new AtomicLong(0);
    private final AtomicLong failuresToInject = new AtomicLong(0);
    private final AtomicLong writeCalls = new AtomicLong(0);

    @Override
    public void writeBatch(String key, List<String> payloads) {
        writeCalls.incrementAndGet();
        long latency = Math.max(latencyMs.get(), keyLatencyMs.getOrDefault(key, 0L));
        if (latency > 0) {
            try {
                Thread.sleep(latency);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted during simulated latency", e);
            }
        }
        if (failuresToInject.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            throw new RuntimeException("injected downstream failure");
        }
        // 原子生效：对单个键的批次整体追加，要么全部可见要么全部不可见
        CopyOnWriteArrayList<String> list = data.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());
        synchronized (list) {
            list.addAll(payloads);
        }
    }

    /** 读取某个键已生效的全部数据（按生效顺序）。 */
    public List<String> readAll(String key) {
        return List.copyOf(data.getOrDefault(key, new CopyOnWriteArrayList<>()));
    }

    /** 下游写入调用总次数（含失败），用于验证重试行为。 */
    public long writeCalls() {
        return writeCalls.get();
    }

    /** 设置每次写入注入的延迟（毫秒）。 */
    public void setLatencyMs(long ms) {
        latencyMs.set(ms);
    }

    /** 设置某个键的写入延迟（毫秒），用于模拟单个键下游变慢。 */
    public void setLatencyForKey(String key, long ms) {
        keyLatencyMs.put(key, ms);
    }

    /** 设置接下来 n 次写入调用抛异常（在生效前抛出，保证不产生部分写）。 */
    public void injectFailures(long n) {
        failuresToInject.set(n);
    }

    public void clear() {
        data.clear();
        keyLatencyMs.clear();
        latencyMs.set(0);
        failuresToInject.set(0);
        writeCalls.set(0);
    }
}
