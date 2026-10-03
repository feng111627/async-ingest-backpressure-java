# async-ingest-backpressure-java

在 Spring Boot 工程内提供一条**异步批量写入通道**：请求先进入有界的内存通道，再由后台
worker 按键有序地写入下游存储（本工程用本地内存 `InMemoryDownstreamStore` 模拟）。
目标是在高并发提交、下游变慢、进程退出等场景下做到**不丢、不串、不无界堆积**。

## 结构

```
ingest/
  WriteRequest              写入请求(requestId 幂等键, key 业务键, payload)
  SubmitResult              提交结果: Accepted(带结果 future) / Rejected(带可区分原因)
  RejectReason              拒绝原因: QUEUE_FULL / QUEUE_FULL_TIMEOUT / SHUTTING_DOWN / KEY_CIRCUIT_OPEN
  WriteOutcome/OutcomeStatus 最终结果: SUCCESS / FAILED_RETRY_EXHAUSTED / FAILED_KEY_CIRCUIT_OPEN / NOT_PROCESSED_SHUTDOWN_TIMEOUT
  DownstreamStore           下游存储抽象(写入必须按 requestId 幂等)
  InMemoryDownstreamStore   内存模拟下游, 可注入延迟与失败
  IngestService             通道核心: 按键保序 + 背压 + 重试熔断 + 优雅关闭
api/IngestController        REST 入口: POST /ingest(异步) 与 POST /ingest/sync(同步等结果)
```

## 顺序与并发语义

- 每个业务键对应一条**串行通道**（per-key lane），同一键的请求严格按提交顺序写入下游。
- 不同键的通道共享一个 worker 线程池（`ingest.worker-threads`）**并行推进**；
  某个键因下游慢或重试而卡住时，只占用自己的通道，不拖慢其它键。
- 下游写入按 `requestId` 幂等（`putIfAbsent`），因此重试与重复提交都不会写重。

## 背压与限流

全局待处理量（排队中 + 处理中）由信号量限制，上限为 `ingest.max-pending`，
达到上限后按 `ingest.backpressure-strategy` 处理新提交：

| 策略 | 行为 | 拒绝原因 |
| --- | --- | --- |
| `REJECT`（默认） | 立即拒绝 | `QUEUE_FULL` → HTTP 429 |
| `BLOCK` | 阻塞等待空位，最长 `ingest.block-timeout`，超时拒绝 | `QUEUE_FULL_TIMEOUT` → HTTP 429 |

其它拒绝原因：`SHUTTING_DOWN`（关闭中，HTTP 503）、`KEY_CIRCUIT_OPEN`（键已熔断，HTTP 409）。
所有拒绝都是**显式返回**的，不依赖超时兜底、不静默丢弃。

## 失败与重试语义

- 下游抛 `DownstreamException` 时按 `ingest.max-retries` 重试，间隔按
  `ingest.retry-backoff * 次数` 线性放大；重试窗口内恢复则最终 `SUCCESS`。
- 重试耗尽后：该请求以 `FAILED_RETRY_EXHAUSTED` 完成，同时**熔断该键**——
  该键后续提交直接以 `KEY_CIRCUIT_OPEN` 快速失败，已排在该键队列中的请求以
  `FAILED_KEY_CIRCUIT_OPEN` 显式完成；其它键不受影响。

## 关闭行为

进程收到关闭信号（`@PreDestroy`）后：

1. 立即停止接收新请求（`SHUTTING_DOWN`）；
2. 在 `ingest.shutdown-timeout` 内等待已接收数据全部处理完；
3. 超时则**明确报告**未处理完的请求（排队中的以 `NOT_PROCESSED_SHUTDOWN_TIMEOUT`
   完成其结果 future，处理中的列入返回清单），并记录告警日志。

下游写入按 requestId 幂等且单条原子生效，不会出现写了一半或重复写入。

## 验证方法

```bash
mvn test
```

`IngestServiceTest` 覆盖 9 个场景，日志中每个场景都有 `[SCENARIO]` / `[PASS]` 标记：

1. 多线程并发提交多键：不丢不串、各键内部有序
2. 下游变慢：卡住的键不拖慢其它键
3. 背压 `REJECT`：满员立即拒绝 `QUEUE_FULL`
4. 背压 `BLOCK`：等待超时拒绝 `QUEUE_FULL_TIMEOUT`
5. 重复提交同一 requestId：幂等只生效一次
6. 下游持续失败：重试耗尽熔断，该键快速失败，其它键正常
7. 重试窗口内恢复：重试后成功
8. 优雅关闭：已接收数据全部排空，新请求被拒绝
9. 关闭超时：明确报告未处理完的请求

本地运行（不依赖任何外部服务）：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080
curl -X POST localhost:8080/ingest/sync \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"r1","key":"user-1","payload":"hello"}'
```
