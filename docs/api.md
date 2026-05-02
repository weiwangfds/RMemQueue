# RMemQueue API 文档

## 项目概述

RMemQueue 是一个类 Kafka 的内存消息队列库，专为 Rust 线程间通信设计。它提供了完整的生产者-消费者模型，支持分区、消费者组、偏移量管理、异步流等企业级特性，同时保持轻量级和高性能。

### 核心特性

- **内存存储**：所有消息存储在内存中，实现零延迟的读写操作
- **分区支持**：每个主题可配置多个分区，实现并行处理
- **消费者组**：支持消费者组协调和分区分配
- **偏移量管理**：自动或手动提交消费偏移量
- **异步流**：基于 tokio 的异步 API（通过 `async` feature）
- **类型安全**：绕过序列化，直接发送和接收类型化消息
- **可扩展架构**：通过 trait 支持自定义后端、分区器、驱逐策略等

### Feature Flags

```toml
[dependencies]
rmemqueue = { version = "x.x.x", features = ["async", "serde"] }
```

- `async`（默认）：启用 tokio 异步 API（`FutureProducer`、`StreamProducer`、`StreamConsumer`、`MessageStream`）
- `serde`：启用 JSON 序列化辅助函数

## 快速开始

### 基础生产消费示例

```rust
use rmemqueue::*;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建配置
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "my-broker");
    config.set("default.num.partitions", "3");

    // 创建生产者
    let producer = BaseProducer::new(&config)?;

    // 发送消息
    let record = BaseRecord::to("test-topic")
        .payload(b"Hello, RMemQueue!")
        .key(b"key-1");

    let meta = producer.send(record)?;
    println!("消息已发送到分区 {}，偏移量 {}", meta.partition, meta.offset);

    // 创建消费者
    let mut consumer_config = RmqClientConfig::new();
    consumer_config.set("broker.id", "my-broker");

    let consumer = BaseConsumer::new(&consumer_config)?;

    // 手动分配分区
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("test-topic", 0);
    consumer.assign(&tpl)?;

    // 消费消息
    if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(1)) {
        let payload = String::from_utf8(msg.payload().unwrap().to_vec())?;
        println!("收到消息: {}", payload);
    }

    Ok(())
}
```

### 异步生产消费示例

```rust
use rmemqueue::*;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建配置
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "async-broker");

    // 创建异步生产者
    let producer = FutureProducer::new(&config)?;

    // 发送消息
    let record = FutureRecord::to("async-topic")
        .payload(b"async message")
        .key(b"async-key");

    let meta = producer.send(record).await?;
    println!("异步消息已发送: {:?}", meta);

    // 创建异步消费者
    let consumer = StreamConsumer::new(&config)?;
    consumer.subscribe(&["async-topic"])?;

    // 使用流 API 消费消息
    let mut stream = consumer.stream();
    while let Some(msg) = stream.next().await {
        let msg = msg?;
        let payload = String::from_utf8(msg.payload().unwrap().to_vec())?;
        println!("收到异步消息: {}", payload);
    }

    Ok(())
}
```

### 类型化消息示例

```rust
use rmemqueue::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Order {
    id: u64,
    product: String,
    quantity: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "typed-broker");

    // 创建类型化生产者
    let producer: TypedProducer<Order> = TypedProducer::new(&config)?;

    // 发送类型化消息
    let order = Arc::new(Order {
        id: 1,
        product: "Widget".to_string(),
        quantity: 10,
    });

    let meta = producer.send("orders", order, None)?;
    println!("订单已发送: {:?}", meta);

    // 创建类型化消费者
    let consumer: TypedConsumer<Order> = TypedConsumer::new(&config)?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("orders", 0);
    consumer.assign(&tpl)?;

    // 消费类型化消息
    if let Some(Ok(msg)) = consumer.poll(std::time::Duration::from_secs(1)) {
        let order = msg.payload();
        println!("收到订单: id={}, product={}, quantity={}",
                 order.id, order.product, order.quantity);
    }

    Ok(())
}
```

## 配置

`RmqClientConfig` 用于配置 RMemQueue 客户端。所有配置通过键值对方式设置。

### 创建配置

```rust
let mut config = RmqClientConfig::new();
config.set("broker.id", "my-broker");
config.set("default.num.partitions", "3");
```

### 配置键

| 配置键 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `broker.id` | String | （必填） | Broker 标识符，用于区分不同的 Broker 实例 |
| `default.num.partitions` | i32 | 1 | 新主题的默认分区数 |
| `partition.buffer.capacity` | usize | 10000 | 每个分区缓冲区的最大消息数 |
| `retention.policy` | String | "none" | 保留策略（当前仅用于配置） |
| `retention.capacity` | usize | （无） | 基于容量的保留，超过此容量时删除旧消息 |
| `retention.ms` | u64 | （无） | 基于时间的保留（毫秒），超过此时间的消息将被删除 |
| `group.session.timeout.ms` | u64 | 30000 | 消费者组会话超时时间（毫秒） |
| `group.id` | String | （无） | 消费者组 ID（消费者专用） |

### 配置示例

```rust
use rmemqueue::*;

fn make_config() -> RmqClientConfig {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "production-broker");
    config.set("default.num.partitions", "4");
    config.set("partition.buffer.capacity", "50000");
    config.set("retention.ms", "3600000"); // 1 小时
    config.set("retention.capacity", "100000");
    config
}

// 带消费者组的配置
fn make_consumer_config() -> RmqClientConfig {
    let mut config = make_config();
    config.set("group.id", "my-consumer-group");
    config
}
```

### 配置操作

```rust
// 获取配置值
let broker_id = config.get("broker.id"); // 返回 Option<&str>

// 移除配置值
config.remove("default.num.partitions");

// 链式调用
config.set("key1", "value1")
     .set("key2", "value2");
```

## Broker

Broker 是 RMemQueue 的核心组件，负责管理主题、分区和消息存储。生产者和消费者通过共享的 Broker Arc 实例进行通信。

### 创建 Broker

```rust
use rmemqueue::*;

// 使用默认的 InMemoryBackend
let config = RmqClientConfig::new();
config.set("broker.id", "broker-1");

let broker = Broker::new(config)?;

// 使用自定义后端
let custom_backend = Arc::new(MyCustomBackend::new());
let broker = Broker::with_backend(config, custom_backend)?;

// 使用自定义分区器
use rmemqueue::ConsistentPartitioner;

let partitioner = Arc::new(ConsistentPartitioner::new());
let broker = Broker::with_partitioner(config, partitioner)?;
```

### Broker 方法

#### `shutdown()`

关闭 Broker，释放资源。关闭后将拒绝所有操作。

```rust
broker.shutdown()?;
```

#### `watermarks()`

获取指定主题分区的低水位和高水位（最早和最新偏移量）。

```rust
let (oldest, newest) = broker.watermarks("my-topic", 0)?;
println!("分区 0: 最早偏移={}, 最新偏移={}", oldest, newest);
```

#### `RecordMetadata`

消息发送成功后返回的元数据。

```rust
pub struct RecordMetadata {
    pub topic: String,        // 主题名称
    pub partition: i32,      // 分区编号
    pub offset: i64,         // 消息偏移量
    pub timestamp: i64,       // 时间戳（毫秒）
}
```

## 生产者

### BaseProducer

同步生产者，使用阻塞方式发送消息。

#### 创建 BaseProducer

```rust
use rmemqueue::*;

let config = RmqClientConfig::new();
config.set("broker.id", "producer-broker");

let producer = BaseProducer::new(&config)?;
```

#### `send()`

发送消息，返回 `RecordMetadata` 或错误。

```rust
let record = BaseRecord::to("my-topic")
    .payload(b"message content")
    .key(b"message-key");

let meta = producer.send(record)?;
println!("消息发送成功: 分区={}, 偏移={}", meta.partition, meta.offset);
```

#### `flush()`

刷新待发送的消息（内存操作，无实际效果）。

```rust
producer.flush()?;
```

#### `in_flight_count()`

获取未完成的消息数量（内存操作，始终返回 0）。

```rust
let count = producer.in_flight_count();
println!("未完成消息数: {}", count);
```

#### `metadata()`

获取 Broker 或指定主题的元数据。

```rust
// 获取所有主题的元数据
let meta = producer.metadata(None)?;
println!("Broker ID: {}, 主题数: {}", meta.broker_id, meta.topics.len());

// 获取特定主题的元数据
let meta = producer.metadata(Some("my-topic"))?;
println!("主题 {} 有 {} 个分区", meta.topics[0].name, meta.topics[0].partitions.len());
```

#### `watermarks()`

获取分区的低水位和高水位。

```rust
let (oldest, newest) = producer.watermarks("my-topic", 0)?;
```

#### 克隆 BaseProducer

```rust
let producer1 = BaseProducer::new(&config)?;
let producer2 = producer1.clone(); // 共享同一个 Broker Arc
```

### FutureProducer（async feature）

异步生产者，使用 `async fn send()` 发送消息。

```rust
use rmemqueue::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "future-broker");

    let producer = FutureProducer::new(&config)?;

    let record = FutureRecord::to("async-topic")
        .payload(b"async payload")
        .key(b"async-key");

    let meta = producer.send(record).await?;
    println!("异步消息发送: {:?}", meta);

    Ok(())
}
```

### StreamProducer（async feature）

支持 `futures_sink::Sink` 的异步生产者。

#### 发送 OwnedRecord

```rust
use rmemqueue::*;

let producer = StreamProducer::new(&config)?;

let record = OwnedRecord::to("sink-topic")
    .payload(b"owned data".to_vec())
    .key(b"owned key".to_vec());

let meta = producer.send(record)?;
```

#### 发送 BaseRecord

```rust
let record = BaseRecord::to("record-topic")
    .payload(b"data")
    .key(b"key");

let meta = producer.send_record(record)?;
```

#### 使用 Sink

```rust
use futures_sink::Sink;

let mut sink = producer.sink();

// 准备发送
Pin::new(&mut sink).poll_ready(&mut cx)?;

// 开始发送
Pin::new(&mut sink).start_send(record)?;

// 刷新
Pin::new(&mut sink).poll_flush(&mut cx)?;

// 关闭
Pin::new(&mut sink).poll_close(&mut cx)?;
```

### TypedProducer<P, K>

类型化生产者，直接发送 Rust 类型，绕过序列化。

```rust
use rmemqueue::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct MyData {
    id: u64,
    name: String,
}

let producer: TypedProducer<MyData> = TypedProducer::new(&config)?;

let data = Arc::new(MyData {
    id: 1,
    name: "test".to_string(),
});

// 基本发送
let meta = producer.send("my-topic", data, None)?;

// 指定分区发送
let meta = producer.send_with_partition("my-topic", 0, data, None)?;

// 带头部发送
let headers = OwnedHeaders::new()
    .insert(Header { key: "type".to_string(), value: Some(b"data".to_vec()) });
let meta = producer.send_with_headers("my-topic", data, None, headers)?;
```

### Producer trait

所有生产者都实现 `Producer` trait。

```rust
pub trait Producer {
    fn broker(&self) -> &Arc<Broker>;
    fn send<'a, K, P>(&self, record: BaseRecord<'a, K, P>) -> Result<RecordMetadata, (RmqError, BaseRecord<'a, K, P>)>
    where K: ToBytes + ?Sized, P: ToBytes + ?Sized;
    fn flush(&self) -> RmqResult<()>;
    fn in_flight_count(&self) -> i32;
    fn metadata(&self, topic: Option<&str>) -> RmqResult<Metadata>;
    fn watermarks(&self, topic: &str, partition: i32) -> RmqResult<(i64, i64)>;
}
```

## 消费者

### BaseConsumer

同步消费者，使用阻塞方式消费消息。

#### 创建 BaseConsumer

```rust
use rmemqueue::*;

let mut config = RmqClientConfig::new();
config.set("broker.id", "consumer-broker");
config.set("group.id", "my-group"); // 可选

let consumer = BaseConsumer::new(&config)?;
```

#### `subscribe()`

订阅主题，加入消费者组进行分区分配。

```rust
consumer.subscribe(&["topic-1", "topic-2"])?;
```

#### `unsubscribe()`

取消订阅所有主题。

```rust
consumer.unsubscribe()?;
```

#### `assign()`

手动分配分区（不使用消费者组）。

```rust
let mut tpl = TopicPartitionList::new();
tpl.add_partition("my-topic", 0);
tpl.add_partition("my-topic", 1);
tpl.add_partition("my-topic", 2);

consumer.assign(&tpl)?;
```

#### `poll()`

轮询消息，返回 `Option<RmqResult<BorrowedMessage>>`。

```rust
use std::time::Duration;

loop {
    match consumer.poll(Duration::from_secs(1)) {
        Some(Ok(msg)) => {
            let payload = String::from_utf8(msg.payload().unwrap().to_vec())?;
            println!("收到消息: {}", payload);
        }
        Some(Err(e)) => {
            eprintln!("消费错误: {}", e);
        }
        None => {
            println!("超时，继续等待...");
        }
    }
}
```

#### `iter()`

返回消息迭代器。

```rust
for msg_result in consumer.iter() {
    match msg_result {
        Ok(msg) => {
            println!("收到消息: {:?}", msg.payload());
        }
        Err(e) => eprintln!("错误: {}", e),
    }
}
```

#### `seek()`

设置消费位置。

```rust
// 从最早的消息开始
consumer.seek("my-topic", 0, Offset::Beginning)?;

// 从最新的消息开始
consumer.seek("my-topic", 0, Offset::End)?;

// 从特定偏移量开始
consumer.seek("my-topic", 0, Offset::Offset(100))?;

// 从存储的偏移量开始
consumer.seek("my-topic", 0, Offset::Stored)?;

// 从末尾往前 N 条开始
consumer.seek("my-topic", 0, Offset::OffsetTail(10))?;
```

#### `commit()`

提交偏移量。

```rust
let mut tpl = TopicPartitionList::new();
tpl.add_partition_offset("my-topic", 0, Offset::Offset(100));

// 同步提交
consumer.commit(&tpl, CommitMode::Sync)?;

// 异步提交
consumer.commit(&tpl, CommitMode::Async)?;
```

#### `commit_message()`

提交消息的偏移量（偏移量 + 1）。

```rust
if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(1)) {
    // 处理消息...
    consumer.commit_message(&msg, CommitMode::Sync)?;
}
```

#### `store_offset()`

存储当前偏移量位置（不提交到组）。

```rust
consumer.store_offset("my-topic", 0, 100)?;
```

#### `committed()`

获取已提交的偏移量。

```rust
let committed = consumer.committed()?;
for elem in committed.elements() {
    println!("{}/{}: {:?}", elem.topic, elem.partition, elem.offset);
}
```

#### `position()`

获取当前消费位置。

```rust
let position = consumer.position()?;
for elem in position.elements() {
    println!("{}/{}: {:?}", elem.topic, elem.partition, elem.offset);
}
```

#### `assignment()`

获取当前分配的分区。

```rust
let assignment = consumer.assignment()?;
println!("分配的分区数: {}", assignment.count());
```

#### `subscription()`

获取当前订阅的主题。

```rust
let subscription = consumer.subscription()?;
for elem in subscription.elements() {
    println!("订阅主题: {}", elem.topic);
}
```

#### `pause()` 和 `resume()`

暂停和恢复分区的消费。

```rust
let mut pause_tpl = TopicPartitionList::new();
pause_tpl.add_partition("my-topic", 0);

consumer.pause(&pause_tpl)?;
// ... 处理其他分区 ...
consumer.resume(&pause_tpl)?;
```

#### `metadata()` 和 `watermarks()`

获取元数据和水位信息。

```rust
let meta = consumer.metadata(Some("my-topic"))?;
let (oldest, newest) = consumer.watermarks("my-topic", 0)?;
```

### StreamConsumer（async feature）

异步消费者，支持流式消费。

```rust
use rmemqueue::*;
use futures_util::StreamExt;

let consumer = StreamConsumer::new(&config)?;
consumer.subscribe(&["async-topic"])?;

// 使用 recv() 方法
loop {
    let msg = consumer.recv().await?;
    println!("收到消息: {:?}", msg.payload());
}

// 使用 stream() 方法
let mut stream = consumer.stream();
while let Some(msg) = stream.next().await {
    let msg = msg?;
    println!("收到消息: {:?}", msg.payload());
}
```

### TypedConsumer<P, K>

类型化消费者，直接消费 Rust 类型。

```rust
use rmemqueue::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct MyData {
    id: u64,
    name: String,
}

let consumer: TypedConsumer<MyData> = TypedConsumer::new(&config)?;

// 手动分配分区
let mut tpl = TopicPartitionList::new();
tpl.add_partition("my-topic", 0);
consumer.assign(&tpl)?;

// 消费类型化消息
if let Some(Ok(msg)) = consumer.poll(std::time::Duration::from_secs(1)) {
    let data = msg.payload();
    println!("收到数据: id={}, name={}", data.id, data.name);
}

// 使用消费者组
consumer.subscribe(&["my-topic"])?;
```

### Consumer trait

所有消费者都实现 `Consumer` trait。

```rust
pub trait Consumer {
    fn broker(&self) -> &Arc<Broker>;
    fn subscribe(&self, topics: &[&str]) -> RmqResult<()>;
    fn unsubscribe(&self) -> RmqResult<()>;
    fn subscription(&self) -> RmqResult<TopicPartitionList>;
    fn assign(&self, partitions: &TopicPartitionList) -> RmqResult<()>;
    fn assignment(&self) -> RmqResult<TopicPartitionList>;
    fn seek(&self, topic: &str, partition: i32, offset: Offset) -> RmqResult<()>;
    fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> RmqResult<()>;
    fn commit_message(&self, msg: &BorrowedMessage<'_>, mode: CommitMode) -> RmqResult<()>;
    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> RmqResult<()>;
    fn committed(&self) -> RmqResult<TopicPartitionList>;
    fn position(&self) -> RmqResult<TopicPartitionList>;
    fn pause(&self, partitions: &TopicPartitionList) -> RmqResult<()>;
    fn resume(&self, partitions: &TopicPartitionList) -> RmqResult<()>;
    fn metadata(&self, topic: Option<&str>) -> RmqResult<Metadata>;
    fn watermarks(&self, topic: &str, partition: i32) -> RmqResult<(i64, i64)>;
}
```

### CommitMode

提交模式枚举。

```rust
pub enum CommitMode {
    Sync,  // 同步提交
    Async, // 异步提交
}
```

## 消息类型

### BaseRecord

同步生产者使用的消息记录，使用构建器模式。

```rust
let record = BaseRecord::to("my-topic")
    .payload(b"message payload")
    .key(b"message key")
    .partition(0)              // 可选，默认自动分配
    .timestamp(1234567890)     // 可选，默认当前时间
    .headers(headers);          // 可选
```

#### `BaseRecord::to()`

创建记录并指定主题。

```rust
let record: BaseRecord<[u8], [u8]> = BaseRecord::to("topic-name");
```

#### `payload()`

设置消息负载。

```rust
let record = BaseRecord::to("topic").payload(b"data");
```

#### `key()`

设置消息键。

```rust
let record = BaseRecord::to("topic").key(b"key");
```

#### `partition()`

指定分区。

```rust
let record = BaseRecord::to("topic").partition(2);
```

#### `timestamp()`

设置时间戳（毫秒）。

```rust
let record = BaseRecord::to("topic").timestamp(1234567890);
```

#### `headers()`

设置消息头部。

```rust
let headers = OwnedHeaders::new()
    .insert(Header { key: "type".to_string(), value: Some(b"data".to_vec()) });
let record = BaseRecord::to("topic").headers(headers);
```

### FutureRecord

`FutureRecord` 是 `BaseRecord` 的类型别名，用于异步生产者。

```rust
pub type FutureRecord<'a, K = [u8], P = [u8]> = BaseRecord<'a, K, P>;
```

使用方式与 `BaseRecord` 完全相同。

### OwnedRecord

拥有所有权的记录，用于异步流生产者。

```rust
let record = OwnedRecord::to("my-topic")
    .payload(b"data".to_vec())     // Vec<u8> 而非 &[u8]
    .key(b"key".to_vec())         // Vec<u8> 而非 &[u8]
    .partition(0)
    .timestamp(1234567890)
    .headers(headers);
```

### BorrowedMessage

从 `poll()` 返回的借用消息。

```rust
if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(1)) {
    println!("主题: {}", msg.topic());
    println!("分区: {}", msg.partition());
    println!("偏移: {}", msg.offset());
    println!("时间戳: {:?}", msg.timestamp());
    println!("键: {:?}", msg.key());
    println!("负载: {:?}", msg.payload());
    println!("头部: {:?}", msg.headers());

    // 转换为 OwnedMessage
    let owned = msg.detach();
}
```

#### `detach()`

将 `BorrowedMessage` 转换为 `OwnedMessage`，获得所有权。

```rust
let owned_msg = borrowed_msg.detach();
```

### OwnedMessage

拥有所有权的消息。

```rust
pub struct OwnedMessage {
    pub payload: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Timestamp,
    pub headers: Option<OwnedHeaders>,
}
```

### Message trait

所有消息类型都实现 `Message` trait。

```rust
pub trait Message {
    fn key(&self) -> Option<&[u8]>;
    fn payload(&self) -> Option<&[u8]>;
    fn topic(&self) -> &str;
    fn partition(&self) -> i32;
    fn offset(&self) -> i64;
    fn timestamp(&self) -> Timestamp;
    fn headers(&self) -> Option<&OwnedHeaders>;

    // 解码辅助方法
    fn decode_payload<T: FromBytes>(&self) -> Option<T>;
    fn decode_key<T: FromBytes>(&self) -> Option<T>;
}
```

#### 解码负载和键

```rust
// 解码为 String
let text: String = msg.decode_payload()?;

// 解码为 Vec<u8>
let bytes: Vec<u8> = msg.decode_payload()?;

// 解码键
let key: String = msg.decode_key()?;
```

### Timestamp

时间戳枚举。

```rust
pub enum Timestamp {
    NotAvailable,           // 时间戳不可用
    CreateTime(i64),       // 创建时间（毫秒）
    LogAppendTime(i64),    // 日志追加时间（毫秒）
}
```

#### 创建当前时间戳

```rust
let ts = Timestamp::now();
let millis = ts.to_millis(); // Option<i64>
```

#### 获取毫秒值

```rust
match msg.timestamp() {
    Timestamp::CreateTime(ms) => println!("时间戳: {} ms", ms),
    Timestamp::LogAppendTime(ms) => println!("追加时间: {} ms", ms),
    Timestamp::NotAvailable => println!("无时间戳"),
}
```

## 序列化（serde feature）

启用 `serde` feature 后，可以使用 JSON 序列化辅助函数。

### `to_json_bytes()`

将值序列化为 JSON 字节。

```rust
use serde::{Serialize, Deserialize};
use rmemqueue::*;

#[derive(Serialize, Deserialize)]
struct Order {
    id: u64,
    product: String,
}

let order = Order { id: 1, product: "Widget".to_string() };
let bytes = to_json_bytes(&order);
```

### `from_json_bytes()`

从 JSON 字节反序列化。

```rust
let order: Order = from_json_bytes(&bytes)?;
```

### `SerdeJson<T>`

包装类型，实现 `FromBytes` trait。

```rust
use rmemqueue::SerdeJson;

// 消费时自动反序列化
let order: SerdeJson<Order> = msg.decode_payload()?;
let inner: Order = order.0;

// 生产时序列化
let order = Order { id: 1, product: "Widget".to_string() };
let json = to_json_bytes(&order);
let record = BaseRecord::to("orders")
    .payload(json.as_slice())
    .key(b"order-key");
```

### 完整示例

```rust
use rmemqueue::*;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct OrderCreated {
    order_id: u64,
    product: String,
    quantity: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RmqClientConfig::new();
    config.set("broker.id", "serde-broker");

    let producer = BaseProducer::new(&config)?;

    let order = OrderCreated {
        order_id: 42,
        product: "Widget".to_string(),
        quantity: 3,
    };

    // 序列化并发送
    let payload = to_json_bytes(&order);
    let record = BaseRecord::to("order-events")
        .payload(payload.as_slice())
        .key(b"order-42");
    producer.send(record)?;

    // 消费并反序列化
    let consumer = BaseConsumer::new(&config)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("order-events", 0);
    consumer.assign(&tpl)?;
    consumer.seek("order-events", 0, Offset::Beginning)?;

    if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(1)) {
        let decoded: SerdeJson<OrderCreated> = msg.decode_payload()?;
        println!("收到订单: {:?}", decoded.0);
    }

    Ok(())
}
```

## Headers

消息头部用于携带元数据。

### Header

单个头部条目。

```rust
pub struct Header {
    pub key: String,
    pub value: Option<Vec<u8>>,
}
```

### OwnedHeaders

头部集合，使用构建器模式。

```rust
let headers = OwnedHeaders::new()
    .insert(Header { key: "content-type".to_string(), value: Some(b"application/json".to_vec()) })
    .insert(Header { key: "trace-id".to_string(), value: Some(b"12345".to_vec()) })
    .insert(Header { key: "flag".to_string(), value: None });
```

#### `new()`

创建空的头部集合。

```rust
let headers = OwnedHeaders::new();
```

#### `new_with_capacity()`

创建指定容量的头部集合。

```rust
let headers = OwnedHeaders::new_with_capacity(10);
```

#### `insert()`

插入头部。

```rust
let headers = headers.insert(Header {
    key: "key".to_string(),
    value: Some(b"value".to_vec()),
});
```

#### `get()`

按键查找头部。

```rust
if let Some(header) = headers.get("content-type") {
    println!("Content-Type: {:?}", header.value);
}
```

#### `get_at()`

按索引查找头部。

```rust
if let Some(header) = headers.get_at(0) {
    println!("第一个头部: {}", header.key);
}
```

#### `iter()`

遍历所有头部。

```rust
for header in headers.iter() {
    println!("{}: {:?}", header.key, header.value);
}
```

#### `len()` 和 `is_empty()`

获取头部数量。

```rust
println!("头部数量: {}", headers.len());
if headers.is_empty() {
    println!("无头部");
}
```

#### `count()`

`count()` 与 `len()` 相同。

### 完整示例

```rust
use rmemqueue::*;

let headers = OwnedHeaders::new()
    .insert(Header { key: "message-id".to_string(), value: Some(b"msg-001".to_vec()) })
    .insert(Header { key: "source".to_string(), value: Some(b"service-a".to_vec()) })
    .insert(Header { key: "retry-count".to_string(), value: None });

let record = BaseRecord::to("events")
    .payload(b"event data")
    .key(b"event-key")
    .headers(headers);

let meta = producer.send(record)?;

// 消费时读取头部
if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(1)) {
    if let Some(headers) = msg.headers() {
        for header in headers.iter() {
            println!("{}: {:?}", header.key, header.value);
        }
    }
}
```

## 分区与偏移量

### Offset

偏移量枚举，用于指定消费位置。

```rust
pub enum Offset {
    Beginning,      // 从最早的消息开始
    End,            // 从最新的消息之后开始
    Stored,         // 从存储的偏移量开始
    Offset(i64),    // 从指定偏移量开始
    OffsetTail(i64), // 从末尾往前 N 条开始
}
```

#### 使用示例

```rust
// 从头开始
consumer.seek("topic", 0, Offset::Beginning)?;

// 从最新开始
consumer.seek("topic", 0, Offset::End)?;

// 从特定偏移量开始
consumer.seek("topic", 0, Offset::Offset(100))?;

// 从存储的偏移量开始
consumer.seek("topic", 0, Offset::Stored)?;

// 从末尾往前 10 条开始
consumer.seek("topic", 0, Offset::OffsetTail(10))?;
```

### TopicPartitionList

主题分区列表，用于分区分配、偏移量提交等。

#### `new()` 和 `with_capacity()`

```rust
let tpl = TopicPartitionList::new();
let tpl = TopicPartitionList::with_capacity(10);
```

#### `add_partition()`

添加分区（使用默认 `Offset::Stored`）。

```rust
tpl.add_partition("topic-1", 0);
tpl.add_partition("topic-1", 1);
tpl.add_partition("topic-2", 0);
```

#### `add_partition_offset()`

添加分区并指定偏移量。

```rust
tpl.add_partition_offset("topic-1", 0, Offset::Beginning);
tpl.add_partition_offset("topic-1", 1, Offset::Offset(100));
```

#### `find_partition()`

查找指定分区。

```rust
if let Some(elem) = tpl.find_partition("topic-1", 0) {
    println!("分区偏移: {:?}", elem.offset);
}
```

#### `elements()`

获取所有元素。

```rust
for elem in tpl.elements() {
    println!("{}/{}: {:?}", elem.topic, elem.partition, elem.offset);
}
```

#### `elements_for_topic()`

获取指定主题的所有元素。

```rust
for elem in tpl.elements_for_topic("topic-1") {
    println!("分区 {}: {:?}", elem.partition, elem.offset);
}
```

#### `count()`

获取元素数量。

```rust
println!("总数: {}", tpl.count());
```

### TopicPartitionElem

主题分区元素。

```rust
pub struct TopicPartitionElem {
    pub topic: String,
    pub partition: i32,
    pub offset: Offset,
}
```

### 完整示例

```rust
use rmemqueue::*;

// 手动分配多个分区
let mut tpl = TopicPartitionList::new();
tpl.add_partition("events", 0);
tpl.add_partition("events", 1);
tpl.add_partition("events", 2);

consumer.assign(&tpl)?;

// 提交多个分区的偏移量
let mut commit_tpl = TopicPartitionList::new();
commit_tpl.add_partition_offset("events", 0, Offset::Offset(100));
commit_tpl.add_partition_offset("events", 1, Offset::Offset(200));
commit_tpl.add_partition_offset("events", 2, Offset::Offset(300));

consumer.commit(&commit_tpl, CommitMode::Sync)?;

// 获取并打印已提交的偏移量
let committed = consumer.committed()?;
for elem in committed.elements() {
    if let Offset::Offset(offset) = elem.offset {
        println!("{}/{}: 已提交到 {}", elem.topic, elem.partition, offset);
    }
}
```

## 分区策略

### Partitioner trait

分区选择策略 trait。

```rust
pub trait Partitioner: Send + Sync {
    fn partition(&self, topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32;
}
```

### RoundRobinPartitioner

轮询分区器，按顺序循环分配分区。

```rust
use rmemqueue::RoundRobinPartitioner;

let partitioner = Arc::new(RoundRobinPartitioner::new());
let broker = Broker::with_partitioner(config, partitioner)?;
```

### ConsistentPartitioner

一致性哈希分区器，相同的键总是映射到相同的分区。当键为 None 时，回退到轮询行为。

```rust
use rmemqueue::ConsistentPartitioner;

let partitioner = Arc::new(ConsistentPartitioner::new());
let broker = Broker::with_partitioner(config, partitioner)?;
```

### RandomPartitioner

伪随机分区器，不依赖 `rand` crate。

```rust
use rmemqueue::RandomPartitioner;

let partitioner = Arc::new(RandomPartitioner::new());
let broker = Broker::with_partitioner(config, partitioner)?;
```

### 自定义分区器

```rust
use rmemqueue::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

struct CustomPartitioner;

impl Partitioner for CustomPartitioner {
    fn partition(&self, _topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32 {
        match key {
            Some(k) => {
                let mut hasher = DefaultHasher::new();
                k.hash(&mut hasher);
                (hasher.finish() % num_partitions as u64) as i32
            }
            None => 0,
        }
    }
}

let partitioner = Arc::new(CustomPartitioner);
let broker = Broker::with_partitioner(config, partitioner)?;
```

## 后端

### BrokerBackend trait

后端 trait，定义存储抽象。

```rust
pub trait BrokerBackend: Send + Sync + 'static {
    fn ensure_topic(&self, topic: &str, num_partitions: i32) -> RmqResult<()>;
    fn produce(&self, topic: &str, partition: i32, key: Option<Vec<u8>>, payload: Option<Vec<u8>>, headers: Option<OwnedHeaders>, timestamp: Option<i64>) -> RmqResult<(i32, i64, i64)>;
    fn fetch(&self, topic: &str, partition: i32, offset: i64, max_count: usize) -> RmqResult<Vec<Arc<StoredMessage>>>;
    fn fetch_one(&self, topic: &str, partition: i32, offset: i64) -> RmqResult<Option<Arc<StoredMessage>>>;
    fn watermarks(&self, topic: &str, partition: i32) -> RmqResult<(i64, i64)>;
    fn metadata(&self, broker_id: &str, topic: Option<&str>) -> RmqResult<Metadata>;
    fn get_partition_notifies(&self, topics: &[String]) -> RmqResult<Vec<Arc<PartitionNotify>>>;
    fn wait_for_messages(&self, topics: &[String], timeout: Duration) -> RmqResult<bool>;
    fn shutdown(&self) -> RmqResult<()>;
}
```

### InMemoryBackend

默认的内存后端实现。

```rust
use rmemqueue::InMemoryBackend;

let backend = Arc::new(InMemoryBackend::new(config));
let broker = Broker::with_backend(config, backend)?;
```

### 自定义后端

```rust
use rmemqueue::*;
use std::sync::Arc;

struct MyBackend {
    // 后端状态
}

impl BrokerBackend for MyBackend {
    fn ensure_topic(&self, topic: &str, num_partitions: i32) -> RmqResult<()> {
        // 实现主题创建
        Ok(())
    }

    fn produce(&self, topic: &str, partition: i32, key: Option<Vec<u8>>, payload: Option<Vec<u8>>, headers: Option<OwnedHeaders>, timestamp: Option<i64>) -> RmqResult<(i32, i64, i64)> {
        // 实现消息生产
        Ok((partition, 0, timestamp.unwrap_or(0)))
    }

    // 实现其他方法...
}

let backend = Arc::new(MyBackend);
let broker = Broker::with_backend(config, backend)?;
```

## 偏移量存储

### OffsetStore trait

偏移量存储 trait。

```rust
pub trait OffsetStore: Send + Sync + 'static {
    fn commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> RmqResult<()>;
    fn committed(&self, group_id: &str, topic: &str, partition: i32) -> RmqResult<Option<i64>>;
}
```

### InMemoryOffsetStore

默认的内存偏移量存储实现。

## 分区分配

### PartitionAssignor trait

分区分配器 trait。

```rust
pub trait PartitionAssignor: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn assign(&self, group_id: &str, members: &[&str], topics: &[String], partition_counts: &HashMap<String, i32>) -> RmqResult<HashMap<String, TopicPartitionList>>;
}
```

### RoundRobinAssignor

默认的轮询分区分配器。

## 驱逐策略

### EvictionPolicy trait

驱逐策略 trait。

```rust
pub trait EvictionPolicy: Send + Sync {
    fn should_evict(&self, buffer_len: usize, front_msg: &StoredMessage) -> bool;
}
```

### TimeEviction

基于时间的驱逐策略。

```rust
pub struct TimeEviction {
    pub retention_ms: u64,
}
```

当消息存在时间超过 `retention_ms` 时，将被驱逐。

### CapacityEviction

基于容量的驱逐策略。

```rust
pub struct CapacityEviction {
    pub retention_capacity: usize,
}
```

当缓冲区消息数超过 `retention_capacity` 时，旧消息将被驱逐。

### 使用驱逐策略

驱逐策略通过配置启用：

```rust
let mut config = RmqClientConfig::new();
config.set("broker.id", "eviction-broker");
config.set("retention.ms", "3600000");      // 1 小时后驱逐
config.set("retention.capacity", "100000");  // 超过 10 万条时驱逐
```

## 上下文

### ClientContext trait

客户端上下文 trait，提供错误回调。

```rust
pub trait ClientContext: Send + Sync + 'static {
    fn error(&self, error: RmqError, reason: &str);
}
```

### ProducerContext trait

生产者上下文 trait，提供交付回调。

```rust
pub trait ProducerContext: ClientContext {
    fn delivery(&self, result: &DeliveryResult, metadata: RecordMetadata);
}
```

### ConsumerContext trait

消费者上下文 trait，提供重平衡和提交回调。

```rust
pub trait ConsumerContext: ClientContext {
    fn rebalance(&self, event: &RebalanceEvent);
    fn commit_callback(&self, result: RmqResult<()>, offsets: &TopicPartitionList);
}
```

### DefaultClientContext、DefaultProducerContext、DefaultConsumerContext

默认的上下文实现，不执行任何操作。

### DeliveryResult

交付结果类型别名。

```rust
pub type DeliveryResult = Result<RecordMetadata, (RmqError, OwnedMessage)>;
```

### RebalanceEvent

重平衡事件枚举。

```rust
pub enum RebalanceEvent {
    Assigned(TopicPartitionList),   // 分区已分配
    Revoked(TopicPartitionList),   // 分区已撤销
}
```

### 自定义上下文

```rust
use rmemqueue::*;

struct MyProducerContext;

impl ClientContext for MyProducerContext {
    fn error(&self, error: RmqError, reason: &str) {
        eprintln!("错误: {} - {}", error, reason);
    }
}

impl ProducerContext for MyProducerContext {
    fn delivery(&self, result: &DeliveryResult, metadata: RecordMetadata) {
        match result {
            Ok(meta) => println!("交付成功: {:?}", meta),
            Err((e, _msg)) => println!("交付失败: {}", e),
        }
    }
}

let producer = BaseProducer::with_context(&config, MyProducerContext)?;
```

## 元数据

### Metadata

集群元数据。

```rust
pub struct Metadata {
    pub broker_id: String,
    pub topics: Vec<TopicMetadata>,
}
```

### TopicMetadata

主题元数据。

```rust
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
    pub error: Option<RmqError>,
}
```

### PartitionMetadata

分区元数据。

```rust
pub struct PartitionMetadata {
    pub id: i32,
    pub oldest_offset: i64,
    pub newest_offset: i64,
    pub message_count: i64,
}
```

### 使用元数据

```rust
let meta = producer.metadata(None)?;

println!("Broker ID: {}", meta.broker_id);

for topic in &meta.topics {
    println!("主题: {}", topic.name);
    if let Some(e) = &topic.error {
        println!("  错误: {}", e);
    }
    for partition in &topic.partitions {
        println!("  分区 {}: 最早={}, 最新={}, 消息数={}",
                 partition.id, partition.oldest_offset, partition.newest_offset, partition.message_count);
    }
}
```

## 错误处理

### RmqError

错误枚举。

```rust
pub enum RmqError {
    TopicNotFound(String),                          // 主题未找到
    PartitionOutOfRange { topic: String, partition: i32 },  // 分区超出范围
    OffsetOutOfRange { topic: String, partition: i32, offset: i64 },  // 偏移量超出范围
    GroupNotFound(String),                         // 消费者组未找到
    AlreadySubscribed(Vec<String>),                // 已订阅
    NotSubscribed,                                // 未订阅
    BrokerShutdown,                                // Broker 已关闭
    BufferFull { topic: String, partition: i32 }, // 缓冲区已满
    InvalidConfig(String),                         // 无效配置
    Custom(String),                                // 自定义错误
}
```

### RmqResult

结果类型别名。

```rust
pub type RmqResult<T> = Result<T, RmqError>;
```

### 错误处理示例

```rust
use rmemqueue::*;

fn send_message(producer: &BaseProducer, topic: &str, payload: &[u8]) -> RmqResult<()> {
    let record = BaseRecord::to(topic).payload(payload);
    let meta = producer.send(record)?;
    println!("消息发送成功: {:?}", meta);
    Ok(())
}

fn handle_errors() {
    match send_message(&producer, "test-topic", b"hello") {
        Ok(_) => println!("成功"),
        Err(RmqError::TopicNotFound(t)) => eprintln!("主题未找到: {}", t),
        Err(RmqError::BufferFull { topic, partition }) => eprintln!("缓冲区已满: {}/{}", topic, partition),
        Err(e) => eprintln!("错误: {}", e),
    }
}
```

### 特定错误处理

```rust
match producer.send(record) {
    Ok(meta) => println!("成功: {:?}", meta),
    Err((e, _rec)) => {
        match e {
            RmqError::TopicNotFound(t) => eprintln!("主题不存在: {}", t),
            RmqError::BrokerShutdown => eprintln!("Broker 已关闭"),
            RmqError::BufferFull { .. } => eprintln!("缓冲区已满，请稍后重试"),
            _ => eprintln!("发送失败: {}", e),
        }
    }
}
```

## Feature Flags

### async

启用基于 tokio 的异步 API。

```toml
[dependencies]
rmemqueue = { version = "x.x.x", features = ["async"] }
```

启用的类型：
- `FutureProducer` - 异步生产者
- `StreamProducer` - 支持 Sink 的异步生产者
- `StreamConsumer` - 异步消费者
- `MessageStream` - 实现 `futures::Stream` 的消息流

### serde

启用 JSON 序列化辅助函数。

```toml
[dependencies]
rmemqueue = { version = "x.x.x", features = ["serde"] }
```

启用的函数和类型：
- `to_json_bytes<T>()` - 序列化为 JSON
- `from_json_bytes<T>()` - 从 JSON 反序列化
- `SerdeJson<T>` - 包装类型，实现 `FromBytes`

### 默认 features

默认启用 `async` feature。

```toml
rmemqueue = { version = "x.x.x" }  # 默认启用 async
```

## 实用示例

### 生产者消费者端到端

```rust
use rmemqueue::*;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "e2e-broker");
    config.set("default.num.partitions", "1");

    // 生产 5 条消息
    let producer = BaseProducer::new(&config)?;
    for i in 0..5 {
        let payload = format!("msg-{}", i);
        let record = BaseRecord::to("e2e-topic")
            .payload(payload.as_bytes())
            .key(b"");
        producer.send(record)?;
    }

    // 验证元数据
    let meta = producer.metadata(Some("e2e-topic"))?;
    assert_eq!(meta.topics.len(), 1);
    assert_eq!(meta.topics[0].partitions.len(), 1);

    // 消费消息
    let consumer = BaseConsumer::new(&config)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("e2e-topic", 0);
    consumer.assign(&tpl)?;
    consumer.seek("e2e-topic", 0, Offset::Beginning)?;

    let mut received = 0;
    loop {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(msg)) => {
                let payload = String::from_utf8(msg.payload().unwrap().to_vec())?;
                println!("收到: {}", payload);
                received += 1;
                if received >= 5 {
                    break;
                }
            }
            Some(Err(e)) => eprintln!("错误: {}", e),
            None => break,
        }
    }

    Ok(())
}
```

### 消费者组

```rust
use rmemqueue::*;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "group-broker");
    config.set("default.num.partitions", "3");

    // 生产者
    let producer = BaseProducer::new(&config)?;
    for i in 0..10 {
        let payload = format!("msg-{}", i);
        let record = BaseRecord::to("group-topic")
            .payload(payload.as_bytes())
            .key(b"");
        producer.send(record)?;
    }

    // 消费者 1
    let mut consumer1_config = RmqClientConfig::new();
    consumer1_config.set("broker.id", "group-broker");
    consumer1_config.set("group.id", "test-group");

    let consumer1 = BaseConsumer::new(&consumer1_config)?;
    consumer1.subscribe(&["group-topic"])?;

    // 消费者 2
    let mut consumer2_config = RmqClientConfig::new();
    consumer2_config.set("broker.id", "group-broker");
    consumer2_config.set("group.id", "test-group");

    let consumer2 = BaseConsumer::new(&consumer2_config)?;
    consumer2.subscribe(&["group-topic"])?;

    // 两个消费者将自动分配分区
    let assignment1 = consumer1.assignment()?;
    let assignment2 = consumer2.assignment()?;

    println!("消费者 1 分配: {} 个分区", assignment1.count());
    println!("消费者 2 分配: {} 个分区", assignment2.count());

    Ok(())
}
```

### 带头部的消息

```rust
use rmemqueue::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RmqClientConfig::new();
    config.set("broker.id", "header-broker");

    let producer = BaseProducer::new(&config)?;

    // 创建头部
    let headers = OwnedHeaders::new()
        .insert(Header { key: "message-type".to_string(), value: Some(b"event".to_vec()) })
        .insert(Header { key: "source-service".to_string(), value: Some(b"order-service".to_vec()) })
        .insert(Header { key: "retry-count".to_string(), value: Some(b"0".to_vec()) });

    let record = BaseRecord::to("events")
        .payload(b"{\"event\":\"order_created\"}")
        .key(b"order-001")
        .headers(headers);

    producer.send(record)?;

    // 消费并读取头部
    let consumer = BaseConsumer::new(&config)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("events", 0);
    consumer.assign(&tpl)?;

    if let Some(Ok(msg)) = consumer.poll(std::time::Duration::from_secs(1)) {
        println!("负载: {:?}", msg.payload());
        if let Some(headers) = msg.headers() {
            for header in headers.iter() {
                println!("{}: {:?}", header.key, header.value);
            }
        }
    }

    Ok(())
}
```

### 偏移量管理

```rust
use rmemqueue::*;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "offset-broker");
    config.set("group.id", "offset-group");

    let producer = BaseProducer::new(&config)?;
    for i in 0..10 {
        let payload = format!("msg-{}", i);
        let record = BaseRecord::to("offset-topic")
            .payload(payload.as_bytes())
            .key(b"");
        producer.send(record)?;
    }

    let consumer = BaseConsumer::new(&config)?;
    consumer.subscribe(&["offset-topic"])?;

    // 消费 5 条消息
    for _ in 0..5 {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(1)) {
            println!("消费: {:?}", msg.payload());
            // 手动提交
            consumer.commit_message(&msg, CommitMode::Sync)?;
        }
    }

    // 查看已提交的偏移量
    let committed = consumer.committed()?;
    for elem in committed.elements() {
        if let Offset::Offset(offset) = elem.offset {
            println!("已提交 {}/{}: {}", elem.topic, elem.partition, offset);
        }
    }

    // 重启消费者，将从提交的偏移量继续消费
    let consumer2 = BaseConsumer::new(&config)?;
    consumer2.subscribe(&["offset-topic"])?;

    // 只能收到后 5 条消息
    for _ in 0..5 {
        if let Some(Ok(msg)) = consumer2.poll(Duration::from_secs(1)) {
            println!("继续消费: {:?}", msg.payload());
        }
    }

    Ok(())
}
```

### 类型化消息生产消费

```rust
use rmemqueue::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct UserEvent {
    user_id: u64,
    event_type: String,
    timestamp: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RmqClientConfig::new();
    config.set("broker.id", "typed-broker");

    let producer: TypedProducer<UserEvent> = TypedProducer::new(&config)?;
    let consumer: TypedConsumer<UserEvent> = TypedConsumer::new(&config)?;

    // 生产类型化消息
    for i in 0..5 {
        let event = Arc::new(UserEvent {
            user_id: i,
            event_type: "login".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as i64,
        });
        producer.send("user-events", event, None)?;
    }

    // 消费类型化消息
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("user-events", 0);
    consumer.assign(&tpl)?;

    for _ in 0..5 {
        if let Some(Ok(msg)) = consumer.poll(std::time::Duration::from_secs(1)) {
            let event = msg.payload();
            println!("用户 {} 事件 {}", event.user_id, event.event_type);
        }
    }

    Ok(())
}
```

### 异步流处理

```rust
use rmemqueue::*;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "async-broker");

    // 生产消息
    let producer = StreamProducer::new(&config)?;
    for i in 0..10 {
        let record = OwnedRecord::to("stream-topic")
            .payload(format!("msg-{}", i).into_bytes())
            .key(b"".to_vec());
        producer.send(record)?;
    }

    // 使用流 API 消费
    let consumer = StreamConsumer::new(&config)?;
    consumer.subscribe(&["stream-topic"])?;

    let mut stream = consumer.stream();
    while let Some(msg_result) = stream.next().await {
        let msg = msg_result?;
        let payload = String::from_utf8(msg.payload().unwrap().to_vec())?;
        println!("流式消费: {}", payload);
    }

    Ok(())
}
```

## 最佳实践

### 选择合适的消费者类型

- **BaseConsumer**：同步环境，或不在 tokio runtime 中
- **StreamConsumer**：异步环境，在 tokio runtime 中使用

### 合理配置分区数

- 分区数影响并行度和吞吐量
- 通常设置为消费者数量的倍数
- 过多的分区会增加管理开销

### 处理偏移量越界

```rust
match consumer.poll(Duration::from_secs(1)) {
    Some(Ok(msg)) => { /* 处理消息 */ }
    Some(Err(RmqError::OffsetOutOfRange { .. })) => {
        // 自动调整到最早的消息
        consumer.seek("topic", 0, Offset::Beginning)?;
    }
    Some(Err(e)) => eprintln!("错误: {}", e),
    None => { /* 超时 */ }
}
```

### 使用消费者组

消费者组自动进行分区分配和负载均衡：

```rust
// 多个消费者加入同一个组
config.set("group.id", "my-group");
consumer.subscribe(&["topic"])?;
```

### 处理重平衡

```rust
struct MyConsumerContext;

impl ConsumerContext for MyConsumerContext {
    fn rebalance(&self, event: &RebalanceEvent) {
        match event {
            RebalanceEvent::Assigned(tpl) => {
                println!("分区已分配: {} 个", tpl.count());
            }
            RebalanceEvent::Revoked(tpl) => {
                println!("分区已撤销: {} 个", tpl.count());
                // 执行清理操作
            }
        }
    }
}
```

### 合理设置缓冲区容量

```rust
// 根据消息大小和保留策略调整
config.set("partition.buffer.capacity", "10000");
config.set("retention.ms", "3600000");  // 1 小时
config.set("retention.capacity", "100000");
```

### 共享 Broker Arc

生产者和消费者会自动共享相同 broker.id 的 Broker Arc：

```rust
let config = RmqClientConfig::new();
config.set("broker.id", "shared-broker");

let producer = BaseProducer::new(&config)?;
let consumer = BaseConsumer::new(&config)?;

// 它们共享同一个 Broker Arc
assert!(Arc::ptr_eq(producer.broker(), consumer.broker()));
```

## 性能考虑

### 内存使用

- 所有消息存储在内存中
- 注意 `partition.buffer.capacity` 和 `retention.capacity` 的设置
- 使用驱逐策略限制内存使用

### 并发性能

- 使用多个分区实现并行处理
- 分区锁基于 parking_lot::RwLock，性能优异
- 消费者组自动分配分区，避免竞争

### 批处理

RMemQueue 当前不支持真正的批处理 API，但可以在应用层实现：

```rust
// 应用层批处理
let mut batch = Vec::new();
for _ in 0..100 {
    if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(10)) {
        batch.push(msg);
    }
}
// 处理整个批次
process_batch(batch);
```

## 故障排除

### BrokerShutdown 错误

在调用 `broker.shutdown()` 后尝试操作会返回此错误。

### BufferFull 错误

当分区缓冲区满时发生。解决方法：
- 增加 `partition.buffer.capacity`
- 减少消息生产速度
- 使用驱逐策略清理旧消息

### OffsetOutOfRange 错误

消费者尝试读取超出范围的偏移量。库会自动调整到最早的可用偏移量。

### TopicNotFound 错误

尝试操作不存在的主题。发送消息会自动创建主题。

### GroupNotFound 错误

尝试提交偏移量到不存在的消费者组。

## 总结

RMemQueue 提供了完整的 Kafka 风格消息队列功能，专为 Rust 线程间通信优化。其核心优势包括：

- **简单易用**：直观的 API，类似 rdkafka
- **高性能**：内存存储，零网络开销
- **类型安全**：支持类型化消息，避免序列化开销
- **异步支持**：基于 tokio 的完整异步 API
- **可扩展**：通过 trait 支持自定义后端、分区器等

选择合适的 producer/consumer 类型、合理配置分区和缓冲区、正确处理偏移量和错误，即可充分发挥 RMemQueue 的性能优势。
