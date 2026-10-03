# 异步批量写入通道（async-ingest-backpressure-java）

在 Spring Boot 工程内提供一条异步批量写入通道：高并发提交、下游变慢、进程退出
三种情况下保证数据**不丢、不串、不无界堆积**。下游存储用本地内存模拟
（`InMemoryDownstreamStore`），可注入延迟与失败，便于本地验证。

## 架构

```
HTTP POST /ingest ──► IngestService ──► KeyLane(按业务键) ──► DownstreamStore
                          │                  │  串行保序、批量、重试、熔断
                          └─ 全局信号量背压（maxPending）
```

- `IngestService`：入口。全局信号量控制"排队中 + 处理中"总量；提交返回
  `CompletableFuture<WriteAck>`，数据真正写入下游后才完成。
- `KeyLane`：每个业务键一条串行通道，队列 + 共享工作线程池调度。
  同一键严格按提交顺序生效；不同键互不阻塞，单键卡住不拖慢其它键。
- `InMemoryDownstreamStore`：内存模拟下游，整批原子生效（要么全生效要么抛异常），
  支持注入延迟（全局/按键）与失败次数。

## 背压与限流

待处理量（排队中 + 处理中）达到 `ingest.max-pending` 后，按
`ingest.backpressure-strategy` 处理新提交：

| 策略 | 行为 | 拒绝原因 |
|---|---|---|
| `REJECT`（默认） | 立即拒绝 | `QUEUE_FULL` → HTTP 429 |
| `BLOCK` | 排队等待容量，最多 `ingest.offer-timeout-ms` | 超时后 `OFFER_TIMEOUT` → HTTP 429 |

拒绝通过 `IngestRejectedException`（携带 `RejectReason`）传播，HTTP 层映射为
明确状态码与 `reason` 字段，**不靠超时或静默丢弃兜底**。

## 失败与重试语义

- 单批写入失败按 `ingest.max-retries` 次重试，间隔 `ingest.retry-backoff-ms`。
- 重试期间该键后续数据在队列中等待，**不影响其它键**。
- 重试耗尽：该批 future 以 `WriteFailedException` 失败，该键通道熔断；
  已排队的及后续提交以 `LANE_CIRCUIT_OPEN`（HTTP 409）**快速失败**，不再触达下游。
- 下游模拟器在生效前抛异常，因此失败批次**不产生部分写**；重试只对未生效的
  批次重放，不会重复写。成功路径每批恰好生效一次（测试断言 `writeCalls` 与存储内容）。

## 关闭行为

收到关闭信号（`@PreDestroy`，如 SIGTERM）后：

1. 立即停止接收新请求，新提交以 `SHUTTING_DOWN`（HTTP 503）拒绝；
2. 在 `ingest.shutdown-timeout-ms` 内等待已接收数据全部处理完；
3. 超时则返回并记录**未处理完的键列表**；已生效的数据始终是完整前缀
   （批次原子生效），不会出现写一半。

## 配置项（`ingest.*`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `max-pending` | 1000 | 全局待处理上限 |
| `backpressure-strategy` | REJECT | REJECT / BLOCK |
| `offer-timeout-ms` | 200 | BLOCK 策略排队超时 |
| `max-batch-size` | 32 | 每批最大条数 |
| `max-retries` | 3 | 单批失败重试次数 |
| `retry-backoff-ms` | 50 | 重试间隔 |
| `worker-threads` | 4 | 工作线程数 |
| `shutdown-timeout-ms` | 10000 | 关闭排空超时 |

## HTTP 接口

- `POST /ingest`  body `{"key":"...","payload":"..."}`
  - 200：`{"key":"...","sequence":N}`（该键内生效序号，从 0 递增）
  - 429：`{"error":"rejected","reason":"QUEUE_FULL|OFFER_TIMEOUT"}`
  - 409：`{"error":"rejected","reason":"LANE_CIRCUIT_OPEN"}`
  - 503：`{"error":"rejected","reason":"SHUTTING_DOWN"}`
- `GET /ingest/pending`：当前待处理量。

## 验证方法

```bash
mvn test
```

全部测试本地运行、不依赖外部服务。测试日志中以 `[通过] ...` 标明每个场景的结果：

| 测试类 | 覆盖场景 |
|---|---|
| `IngestConcurrencyTest` | 8 键 × 200 条并发提交保序无丢失无重复；同一内容重复提交 50 次按序生效；单键下游 300ms 延迟期间其它键正常 |
| `BackpressureTest` | REJECT 打满上限后 `QUEUE_FULL`；BLOCK 排队等待全部成功；BLOCK 超时 `OFFER_TIMEOUT` |
| `RetryAndCircuitTest` | 失败重试后恰好生效一次；重试耗尽熔断 + 快速失败；熔断按键隔离；排队命令熔断时明确失败 |
| `ShutdownTest` | 关闭时 100 条积压全部排空；关闭后拒新；关闭超时报告未处理完的键且已生效数据为完整前缀 |
| `IngestControllerTest` | HTTP 200 确认与并发下 429/QUEUE_FULL 映射 |
