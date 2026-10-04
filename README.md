# async-ingest-backpressure-java

在 Spring Boot 工程内提供一条**异步批量写入通道**：请求先进入有界的内存通道，再由后台
worker 按键有序地写入下游存储（本工程用本地内存 `InMemoryDownstreamStore` 模拟）。
目标是在高并发提交、下游变慢、下游故障、进程退出等场景下做到**不丢、不串、不无界堆积**，
并且运行状态**可观测**、故障后**可自动恢复**、长期空闲的键状态**可自动回收**。

## 结构

```
ingest/
  WriteRequest              写入请求(requestId 幂等键, key 业务键, payload)
  SubmitResult              提交结果: Accepted(带结果 future) / Rejected(带可区分原因)
  RejectReason              拒绝原因: QUEUE_FULL / QUEUE_FULL_TIMEOUT / SHUTTING_DOWN / KEY_CIRCUIT_OPEN
  WriteOutcome/OutcomeStatus 最终结果: SUCCESS / FAILED_RETRY_EXHAUSTED / FAILED_KEY_CIRCUIT_OPEN / NOT_PROCESSED_SHUTDOWN_TIMEOUT
  DownstreamStore           下游存储抽象(写入必须按 requestId 幂等)
  InMemoryDownstreamStore   内存模拟下游, 可注入延迟与失败
  IngestMetrics             全局指标: 接收/成功/失败/按原因拒绝, 并发下准确
  KeyStatus                 单键状态快照: 积压/是否被挡/冷却剩余/成功数
  IngestService             通道核心: 按键保序 + 背压 + 重试熔断 + 半开恢复 + 空闲回收 + 优雅关闭
api/IngestController        REST 入口: POST /ingest(异步) 与 POST /ingest/sync(同步等结果)
api/StatsController         运行状态查询: GET /ingest/stats 与 GET /ingest/stats/keys[/{key}]
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

其它拒绝原因：`SHUTTING_DOWN`（关闭中，HTTP 503）、`KEY_CIRCUIT_OPEN`（键被挡，HTTP 409）。
所有拒绝都是**显式返回**的，不依赖超时兜底、不静默丢弃。

## 失败、重试与熔断恢复语义

- 下游抛 `DownstreamException` 时按 `ingest.max-retries` 重试，间隔按
  `ingest.retry-backoff * 次数` 线性放大；重试窗口内恢复则最终 `SUCCESS`。
- 重试耗尽后：该请求以 `FAILED_RETRY_EXHAUSTED` 完成，同时**熔断该键**——
  该键后续提交直接以 `KEY_CIRCUIT_OPEN` 快速失败，已排在该键队列中的请求以
  `FAILED_KEY_CIRCUIT_OPEN` 显式完成；其它键不受影响。
- **冷却与半开恢复**：熔断经过 `ingest.circuit-cooldown`（默认 30s）后自动进入
  **半开**状态——只放行**一条**试探请求，半开期间其它提交继续快速失败，不会往下游堆更多失败请求：
  - 试探成功 → 熔断关闭，该键恢复正常，后续请求照常按序处理；
  - 试探失败 → 重新熔断，冷却重新计时，等待下一次试探。
- 整个过程同一键的先后顺序不乱、不重复写入；冷却期间的快速失败行为与之前完全一致。

## 空闲键状态回收

业务键持续变化，长期空闲的键占用的运行状态（队列、熔断状态、成功计数）会被后台
回收任务自动清理：

- 一个键**空闲**指：无排队、无处理中、无调度中的任务，且最近一次活动（提交或处理完成）
  距今超过 `ingest.key-idle-timeout`（默认 10min）。
- 回收任务每 `ingest.reap-interval`（默认 30s）扫描一次；当被跟踪的键数超过
  `ingest.max-keys`（默认 10000）时，即使未满空闲时长，也会从**最久未活动的空闲键**
  开始回收，直到键数回到上限以内。
- 回收只针对空闲状态：**在途请求不受影响**，回收不算数据丢失、不计入失败、
  不影响全局统计；被回收的键再次提交时按**新键**正常排队处理（每键计数重新开始，
  全局计数不受影响）。

## 运行状态查询

从真实启动的应用里直接查询（并发下计数准确：每条提交只计入接收或拒绝之一，
每条被接收请求只计入成功或失败之一）：

```
GET /ingest/stats             全局: received / succeeded / failed / rejectedTotal
                              / rejectedByReason(按原因区分) / pending / trackedKeys
GET /ingest/stats/keys        全部被跟踪键的状态列表
GET /ingest/stats/keys/{key}  单键: backlog(积压) / circuitBlocked(是否被挡)
                              / circuitHalfOpen(是否半开试探中)
                              / cooldownRemainingMillis(冷却剩余) / successCount
                              键不存在(或已回收)时 404
```

## 关闭行为

进程收到关闭信号（`@PreDestroy`）后：

1. 立即停止接收新请求（`SHUTTING_DOWN`）；
2. 在 `ingest.shutdown-timeout` 内等待已接收数据全部处理完；
3. 超时则**明确报告**未处理完的请求（排队中的以 `NOT_PROCESSED_SHUTDOWN_TIMEOUT`
   完成其结果 future，处理中的列入返回清单），并记录告警日志。

下游写入按 requestId 幂等且单条原子生效，不会出现写了一半或重复写入。

## 配置一览（`ingest.*`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `max-pending` | 1000 | 全局待处理上限，达到后触发背压 |
| `backpressure-strategy` | REJECT | REJECT 立即拒绝 / BLOCK 阻塞等待 |
| `block-timeout` | 5s | BLOCK 策略等待空位的最长时间 |
| `max-retries` | 3 | 下游失败最大重试次数（不含首次） |
| `retry-backoff` | 50ms | 重试间隔基数，按次数线性放大 |
| `worker-threads` | 8 | 处理线程池大小 |
| `shutdown-timeout` | 10s | 关闭时等待排空的最长时间 |
| `circuit-cooldown` | 30s | 熔断冷却时间，到期后半开试探 |
| `max-keys` | 10000 | 最多保留的键状态数，超限回收最久未活动的空闲键 |
| `key-idle-timeout` | 10min | 键空闲多久后状态可回收 |
| `reap-interval` | 30s | 空闲键回收扫描间隔 |

## 兼容范围

- 上一轮的全部语义保持不变：按键保序、背压两种策略与拒绝原因、重试与熔断快速失败、
  幂等去重、优雅关闭排空，均有回归用例守护。
- 新增能力均为**纯增量**：熔断键从"一直挡"变为"冷却后自动半开试探"，冷却期间的
  快速失败行为不变；查询接口与回收机制不影响既有提交路径的行为与结果。
- 配置项全部有默认值，不配置时行为与上一轮一致（冷却 30s、回收默认开启但阈值宽松）。

## 验证方法

```bash
mvn test
```

用例日志中每个场景都有 `[SCENARIO]` / `[PASS]` 标记，共 17 个场景：

`IngestServiceTest`（上一轮回归）：
1. 多线程并发提交多键：不丢不串、各键内部有序
2. 下游变慢：卡住的键不拖慢其它键
3. 背压 `REJECT`：满员立即拒绝 `QUEUE_FULL`
4. 背压 `BLOCK`：等待超时拒绝 `QUEUE_FULL_TIMEOUT`
5. 重复提交同一 requestId：幂等只生效一次
6. 下游持续失败：重试耗尽熔断，该键快速失败，其它键正常
7. 重试窗口内恢复：重试后成功
8. 优雅关闭：已接收数据全部排空，新请求被拒绝
9. 关闭超时：明确报告未处理完的请求

`IngestServiceRecoveryTest`（本轮新增）：
10. 冷却后半开试探成功：键恢复，后续请求照常处理（半开期间并发的第二条仍被拒）
11. 试探仍失败：重新熔断、冷却重新计时、不往下游堆更多失败请求；再次冷却后可恢复
12. 并发提交统计准确：接收+拒绝=总提交、成功=接收、拒绝按原因区分、每键成功数之和=全局成功数
13. 空闲键状态被回收后，再次提交同一个键按新键正常处理
14. 键数超上限时回收最久未活动的空闲键，在途键不受影响

`IngestHttpApiTest`（真实启动的应用 + 真实 HTTP）：
15. 并发 HTTP 提交后，`/ingest/stats` 与 `/ingest/stats/keys/{key}` 数字精确
16. 熔断状态、冷却剩余时间、半开恢复都可通过 HTTP 查询确认
17. 空闲键回收（键状态 404）与同键再提交都可通过 HTTP 确认

本地运行（不依赖任何外部服务）：

```bash
mvn package
java -jar target/async-ingest-backpressure-java-1.0-SNAPSHOT.jar --server.port=8080

curl -X POST localhost:8080/ingest/sync \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"r1","key":"user-1","payload":"hello"}'
curl localhost:8080/ingest/stats
curl localhost:8080/ingest/stats/keys/user-1
```
