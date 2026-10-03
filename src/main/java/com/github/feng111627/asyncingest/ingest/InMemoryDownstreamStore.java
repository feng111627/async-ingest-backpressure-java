package com.github.feng111627.asyncingest.ingest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * 用本地内存模拟的下游存储。
 *
 * <p>写入按 requestId 幂等：同一 requestId 重复写入只生效一次，不会产生重复数据，
 * 因此重试与重复提交都是安全的。
 *
 * <p>支持注入固定延迟（模拟下游变慢）与前 N 次调用失败（模拟下游故障），仅供测试与演示。
 */
@Component
public class InMemoryDownstreamStore implements DownstreamStore {

    private final Map<String, String> appliedByRequestId = new ConcurrentHashMap<>();
    private final List<WriteRequest> writeLog = new CopyOnWriteArrayList<>();

    private volatile long latencyMillis = 0;
    private final AtomicInteger remainingFailures = new AtomicInteger(0);

    @Override
    public void write(WriteRequest request) throws DownstreamException {
        writeLog.add(request);
        if (remainingFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            throw new DownstreamException("injected downstream failure");
        }
        long latency = latencyMillis;
        if (latency > 0) {
            try {
                Thread.sleep(latency);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DownstreamException("interrupted while writing");
            }
        }
        // putIfAbsent 保证幂等：重复 requestId 不覆盖、不重复生效
        appliedByRequestId.putIfAbsent(request.requestId(), request.key() + "=" + request.payload());
    }

    /** 已生效的写入（按 requestId 去重后）。 */
    public Map<String, String> applied() {
        return Map.copyOf(appliedByRequestId);
    }

    /** 全部写入调用日志（含重试与幂等去重的重复调用），用于测试断言。 */
    public List<WriteRequest> writeLog() {
        return List.copyOf(writeLog);
    }

    /** 模拟下游变慢：每次写入固定睡眠指定毫秒。 */
    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }

    /** 模拟下游故障：接下来的 times 次写入调用直接抛错。 */
    public void failNextTimes(int times) {
        remainingFailures.set(times);
    }
}
