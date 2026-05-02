# RMemQueue 全面修复设计文档

**日期**：2026-05-02
**范围**：全部 P0/P1/P2 Bug + 性能瓶颈修复
**破坏性变更**：允许（测试同步更新）

---

## 背景与目标

RMemQueue 是一个类 Kafka 的线程间内存消息队列库。分析发现 15+ 个 Bug 和 13 个性能瓶颈，涵盖：

- Producer/Consumer 不共享 Broker（根本性架构 Bug）
- StreamConsumer 阻塞 tokio worker 线程
- MessageStream busy-spin（CPU 100%）
- 多分区订阅时只等 partition[0] 的唤醒
- OffsetOutOfRange 时数据跳跃丢失
- ConsistentPartitioner i32::MIN.abs() panic
- ConsumerGroup rebalance TOCTOU

修复分四层进行，保持清晰的边界和可测试性。

---

## 第一层：Registry 层

### 目标
让相同 `broker.id` 的 Producer/Consumer 共享同一个 `Arc<Broker>`。

### 实现

新增 `src/registry.rs`：

```
static REGISTRY: Lazy<Mutex<HashMap<String, Weak<Broker>>>> = ...

impl BrokerRegistry {
    pub fn get_or_create(config: &RmqClientConfig) -> RmqResult<Arc<Broker>>
}
```

- 使用 `Weak<Broker>` 存储，最后一个 client 销毁时 Broker 自动回收
- `get_or_create` 按 `broker.id` 查找，若 `Weak` 已失效则重建
- `BaseProducer::new` / `BaseConsumer::new` / `FutureProducer::new` / `StreamConsumer::new` 内部改为调用此方法
- 外部 `pub` API 签名不变，`FromRmqConfig` 实现不变

### 验证
同一 `broker.id` 的两个 client 的 `broker()` 返回 `Arc::ptr_eq` 为 `true`。

---

## 第二层：Partition 层

### 目标
修复 9 个 Partition/Broker 层 Bug，统一通知机制为 `tokio::sync::Notify`。

### 通知机制改造

- `PartitionNotify` 内部改为 `Arc<tokio::sync::Notify>`
- sync 路径用 `Handle::current().block_on(tokio::time::timeout(dur, notify.notified()))`
- `Broker::wait_for_messages` 对**所有**分区 notify 做 `tokio::select!`，消除"只等 partition[0]"延迟

### Bug 修复清单

| # | Bug | 位置 | 修复方式 |
|---|-----|------|---------|
| 1 | 容量 off-by-one | `partition.rs:102` | `>` 改为 `>=` |
| 2 | 空 log + offset≠0 报 OutOfRange | `partition.rs:116` | 检查 `offset == next_offset` 返回 `Ok(None)`，否则报错 |
| 3 | OffsetOutOfRange 跳 newest+1 丢数据 | `broker.rs:205` | 改为跳到 `oldest`（消费者追上最旧可用消息） |
| 4 | evict 从未被调用 | `partition.rs` | 在 `append` 写入后触发惰性驱逐 |
| 5 | `retention_capacity` 配置无效 | `config.rs` | 传入 `PartitionConfig`，evict 逻辑中生效 |
| 6 | `partition_count` 持锁热路径 | `topic.rs:44` | 改为 `AtomicI32`，零锁读取 |
| 7 | `produce` 双次 `SystemTime::now()` | `broker.rs:124` | 移除 broker 侧冗余调用，复用 partition 时间戳 |

---

## 第三层：Consumer 层

### 目标
修复 async busy-spin、阻塞 worker 线程、Clone 语义、分区轮转、TOCTOU 等问题。

### StreamConsumer 异步修复

**MessageStream::poll_next**：
- 不再 `wake_by_ref()`
- 改为注册到 `tokio::sync::Notify` 的 waker，有新消息时才被唤醒

**StreamConsumer::recv**：
```rust
loop {
    notify.notified().await;
    if let Some(msg) = poll_once() { return Ok(msg) }
}
```
不再调用阻塞的 `BaseConsumer::poll`。

### BaseConsumer 修复

| # | Bug/性能 | 位置 | 修复方式 |
|---|----------|------|---------|
| 1 | `Clone` 共享 `member_id` 破坏组 | `L143` | clone 时生成新 `member_id`，清空 `group_id` 和 state |
| 2 | `poll_once` 每次 Vec 克隆 | `L90-109` | 在锁内迭代，不提前 snapshot |
| 3 | 分区轮询从 0 开始饿死其他分区 | `L111` | 引入 `AtomicUsize` 轮转起点 |
| 4 | `member_id` 时间戳碰撞 | `L46` | 加进程级 `AtomicU64` 计数器后缀 |
| 5 | `paused` 键重复 String 分配 | `L330-340` | `(Arc<str>, i32)` 替换 `(String, i32)` |

### ConsumerGroup 修复

- `rebalance`：升级为持**写锁**全程运行，消除 read→drop→write TOCTOU
- `leave`：合并两次锁获取，在一次写锁内完成 remove + 判空 + rebalance

---

## 第四层：Producer 层

### 目标
消除成功路径多余 clone，修复 partitioner panic，修复 headers 丢失。

### BaseProducer::send 优化

**当前问题**：`key_clone` / `payload_clone` / `headers.clone()` 无论成功失败都提前克隆。

**修复**：
- 成功路径：直接 move key/payload 进 broker，回调使用 `RecordMetadata`
- 失败路径：仅在 `Err` 分支按需构造 `OwnedMessage`
- headers 丢失 Bug：失败回调的 `OwnedMessage` 补上 `headers: record.headers.clone()`

### Partitioner 修复

| # | Bug | 位置 | 修复方式 |
|---|-----|------|---------|
| 1 | `i32::MIN.abs()` panic | `ConsistentPartitioner:38` | 改用 `(hash % num_partitions as u32) as i32`，全程 u32 |
| 2 | `num_partitions == 0` 除零 | `RoundRobin`/`Random` | 入口加 `if num_partitions <= 0 { return 0 }` |
| 3 | `Random` 固定初始 state | `RandomPartitioner` | 初始值改为线程 ID 异或进程启动时间 |

---

## 文件变更清单

| 文件 | 变更类型 |
|------|---------|
| `src/registry.rs` | 新增 |
| `src/partition.rs` | 修改（通知机制、evict、off-by-one、read_one） |
| `src/topic.rs` | 修改（AtomicI32 partition_count） |
| `src/broker.rs` | 修改（registry、wait_for_messages、fetch_one_from_position、produce） |
| `src/consumer_group.rs` | 修改（rebalance TOCTOU、leave 合并锁） |
| `src/base_consumer.rs` | 修改（Clone、poll_once、member_id、paused 键） |
| `src/stream_consumer.rs` | 修改（recv、poll_next） |
| `src/base_producer.rs` | 修改（send clone 优化、headers 修复） |
| `src/partitioner.rs` | 修改（panic 修复、除零保护） |
| `src/lib.rs` | 修改（pub use registry） |
| `src/config.rs` | 修改（retention_capacity 传递） |
| `tests/integration_sync.rs` | 更新（适配 breaking changes） |
| `tests/integration_async.rs` | 更新（适配 breaking changes） |

---

## 成功标准

1. `cargo test` 全部通过（含更新后的集成测试）
2. `cargo bench` 吞吐量相比修复前不低于原值（性能只增不减）
3. 同一 `broker.id` 的 Producer/Consumer 可正常通信
4. `StreamConsumer` 在无消息时 CPU 接近 0%
5. 多分区订阅时所有分区的消息延迟均匀

---

## 不在本次范围内

- `TopicPartitionList` 的 `find_partition` O(n) 线性查找（使用量少，延后）
- `OwnedHeaders::get` O(n) 线性查找（延后）
- 动态增加分区（非本库定位）
