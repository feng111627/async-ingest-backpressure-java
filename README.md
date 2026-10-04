# async-ingest-backpressure-java

在 Spring Boot 工程内提供一条**异步批量写入通道**：请求先进入有界的内存通道，再由后台
worker 按键有序地写入下游存储（本工程用本地内存 `InMemoryDownstreamStore` 模拟）。
目标是在高并发提交、下游变慢、进程退出等场景下做到**不丢、不串、不无界堆积**，
并提供运行期可观测、熔断自动恢复与空闲状态回收能力。

## 结构

```
ingest/
  WriteRequest              写入请求(requestId 幂等键, key 业务键, payload)
  SubmitResult              提交结果: Accepted(带结果 future) / Rejected(带可区分原因)
  RejectReason              拒绝原因: QUEUE_FULL / QUEUE_FULL_TIMEOUT / SHUTTING_DOWN / KEY_CIRCUIT_OPEN
  WriteOutcome/OutcomeStatus 最终结果: SUCCESS / FAILED_RETRY_EXHAUSTED / FAILED_KEY_CIRCUIT_OPEN / NOT_PROCESSED_SHUTDOWN_TIMEOUT
  DownstreamStore           下游存储抽象(写入必须按 requestId 幂等)
  InMemoryDownstreamStore   内存模拟下游, 可注入延迟与失败
  IngestMetrics             全局指标: 接收/成功/失败/按原因拒绝, 全 AtomicLong 计数
  IngestStats/KeyStats      全局与每键运行状态快照
  IngestService             通道核心: 按键保序 + 背压 + 重试熔断 + 冷却恢复 + 空闲回收 + 优雅关闭
api/IngestController        REST 入口: POST /ingest(异步), POST /ingest/sync(同步等结果),
                            GET /ingest/stats(全局指标), GET /ingest/stats/keys(每键状态)
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

## 失败、重试与冷却恢复

- 下游抛 `DownstreamException` 时按 `ingest.max-retries` 重试，间隔按
  `ingest.retry-backoff * 次数` 线性放大；重试窗口内恢复则最终 `SUCCESS`。
- 重试耗尽后：该请求以 `FAILED_RETRY_EXHAUSTED` 完成，同时**熔断该键**并进入冷却期
  （`ingest.circuit-cooldown`，默认 30s）。冷却期内该键提交直接以 `KEY_CIRCUIT_OPEN`
  快速失败，已排在该键队列中的请求以 `FAILED_KEY_CIRCUIT_OPEN` 显式完成；其它键不受影响。
- **冷却结束后自动进入半开状态**：下一条提交作为**试探请求**被放行（CAS 保证同一时刻
  只放一条，其余继续快速失败）。试探仍走该键的串行通道，键内顺序不乱、靠 requestId
  幂等不会写重。
  - 试探成功 → 该键立即恢复，后续请求照常处理；
  - 试探失败（重试耗尽）→ 重新进入冷却期，期间不会向下游堆积更多请求。

## 空闲键状态回收

业务键的运行状态（lane、熔断标记、成功计数等）由后台线程按
`ingest.eviction-interval`（默认 1m）周期扫描回收：

- 一个键**空闲**（无排队、无处理中、无调度中的请求）超过 `ingest.key-idle-timeout`
  （默认 10m）即被回收；
- 保留的键数超过 `ingest.max-keys`（默认 10000）时，优先回收最久未活动的空闲键；
- 回收只针对空闲状态：入队与回收在同一把键级锁下互斥，**在途请求不受影响**；
  回收不触碰任何已接收数据，不计入失败，也不会把键误判为故障；
- 回收后再提交同一个键，按全新键正常排队、处理、计数。

## 运行状态查询

所有计数用 `AtomicLong` 累计，且**统计更新先于结果对调用方可见**，并发提交下不漏计、
不多计。从真实运行的应用即可查询：

```bash
# 全局指标: received / succeeded / failed / pending / rejectedTotal /
#           rejectedByReason(QUEUE_FULL, QUEUE_FULL_TIMEOUT, SHUTTING_DOWN, KEY_CIRCUIT_OPEN) / trackedKeys
curl localhost:8080/ingest/stats

# 每键状态: queued / processing / circuitOpen / circuitRetryAfterMillis / succeeded
curl localhost:8080/ingest/stats/keys
```

## 关闭行为

进程收到关闭信号（`@PreDestroy`）后：

1. 立即停止接收新请求（`SHUTTING_DOWN`），并停止空闲回收线程；
2. 在 `ingest.shutdown-timeout` 内等待已接收数据全部处理完；
3. 超时则**明确报告**未处理完的请求（排队中的以 `NOT_PROCESSED_SHUTDOWN_TIMEOUT`
   完成其结果 future，处理中的列入返回清单），并记录告警日志。

下游写入按 requestId 幂等且单条原子生效，不会出现写了一半或重复写入。

## 配置项

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `ingest.max-pending` | 1000 | 全局待处理量上限，达到后触发背压 |
| `ingest.backpressure-strategy` | REJECT | REJECT 立即拒绝 / BLOCK 阻塞等待 |
| `ingest.block-timeout` | 5s | BLOCK 策略等待空位的最长时间 |
| `ingest.max-retries` | 3 | 下游失败的最大重试次数（不含首试） |
| `ingest.retry-backoff` | 50ms | 重试间隔基数，按次数线性放大 |
| `ingest.circuit-cooldown` | 30s | 键熔断后的冷却时间，到点放行单条试探 |
| `ingest.worker-threads` | 8 | 处理线程池大小（键间并行度） |
| `ingest.shutdown-timeout` | 10s | 关闭时等待排空的最长时间 |
| `ingest.key-idle-timeout` | 10m | 键空闲多久后回收其运行状态 |
| `ingest.max-keys` | 10000 | 最多保留状态的键数，超出回收最久空闲键 |
| `ingest.eviction-interval` | 1m | 空闲键回收扫描间隔 |

## 兼容范围

- 上一轮语义全部保持不变：按键保序、背压两种策略与拒绝原因、重试熔断、幂等去重、
  优雅关闭与超时报告，老用例全部保留在回归套件中。
- 行为变化仅有一处：熔断的键不再永久拒绝，冷却（`ingest.circuit-cooldown`）后会
  自动放行单条试探；冷却期内的快速失败行为与之前完全一致。
- 新增配置均有默认值，不配置时行为与本说明一致；对外接口新增两个只读 GET 端点，
  原有 POST 端点的请求/响应格式不变。

## 验证方法

```bash
mvn test
```

`IngestServiceTest` 覆盖 14 个场景，日志中每个场景都有 `[SCENARIO]` / `[PASS]` 标记：

1. 多线程并发提交多键：不丢不串、各键内部有序
2. 下游变慢：卡住的键不拖慢其它键
3. 背压 `REJECT`：满员立即拒绝 `QUEUE_FULL`
4. 背压 `BLOCK`：等待超时拒绝 `QUEUE_FULL_TIMEOUT`
5. 重复提交同一 requestId：幂等只生效一次
6. 下游持续失败：重试耗尽熔断，该键快速失败，其它键正常
7. 重试窗口内恢复：重试后成功
8. 优雅关闭：已接收数据全部排空，新请求被拒绝
9. 关闭超时：明确报告未处理完的请求
10. 熔断冷却结束：单条试探成功，键恢复，后续请求正常
11. 试探仍失败：继续熔断，不向下游堆积更多请求
12. 并发提交下全局与每键统计准确，拒绝按原因分别计数
13. 空闲键状态被自动回收，回收后同键再提交仍正常处理
14. 键数超上限时优先回收最久未活动的空闲键

`IngestHttpTest` 基于**真实启动的应用**（内嵌 Tomcat、随机端口）做并发 HTTP 验证：
8 线程并发 POST `/ingest/sync` 后统计精确、每键状态可查、熔断拒绝按原因计数，
日志带 `[SCENARIO-HTTP]` / `[PASS]` 标记。

本地运行（不依赖任何外部服务）：

```bash
# 整体打包为标准可执行 jar 并启动（默认端口 8080）
mvn clean package
java -jar target/async-ingest-backpressure-java-1.0-SNAPSHOT.jar

curl -X POST localhost:8080/ingest/sync \
  -H 'Content-Type: application/json' \
  -d '{"requestId":"r1","key":"user-1","payload":"hello"}'
curl localhost:8080/ingest/stats
curl localhost:8080/ingest/stats/keys
```
