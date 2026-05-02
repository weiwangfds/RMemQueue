use std::sync::Arc;
use std::time::Duration;

use rmemqueue::*;

fn make_config() -> RmqClientConfig {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "test-broker");
    config.set("default.num.partitions", "3");
    config
}

fn make_config_with_group(group_id: &str) -> RmqClientConfig {
    let mut config = make_config();
    config.set("group.id", group_id);
    config
}

#[test]
fn test_config_creation() {
    let mut config = RmqClientConfig::new();
    config
        .set("broker.id", "my-broker")
        .set("default.num.partitions", "4");

    assert_eq!(config.get("broker.id"), Some("my-broker"));
    assert_eq!(config.get("default.num.partitions"), Some("4"));
    assert_eq!(config.get("nonexistent"), None);

    config.remove("default.num.partitions");
    assert_eq!(config.get("default.num.partitions"), None);
}

#[test]
fn test_producer_send_message() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    let record = BaseRecord::to("test-topic")
        .payload(b"hello world")
        .key(b"key-1");

    let meta = producer.send(record).expect("send should succeed");
    assert_eq!(meta.topic, "test-topic");
    assert!(meta.partition >= 0 && meta.partition < 3);
    assert_eq!(meta.offset, 0);
    assert!(meta.timestamp > 0);
}

#[test]
fn test_producer_send_multiple() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    for i in 0..10 {
        let key = format!("key-{}", i);
        let payload = format!("msg-{}", i);
        let record = BaseRecord::to("multi-topic")
            .payload(payload.as_bytes())
            .key(key.as_bytes());

        let meta = producer.send(record).expect("send should succeed");
        assert_eq!(meta.topic, "multi-topic");
        assert!(meta.offset >= 0);
    }
}

#[test]
fn test_producer_send_with_headers() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "content-type".to_owned(),
            value: Some(b"application/json".to_vec()),
        })
        .insert(Header {
            key: "trace-id".to_owned(),
            value: Some(b"12345".to_vec()),
        });

    let record = BaseRecord::to("header-topic")
        .payload(b"{\"key\":\"value\"}" as &[u8])
        .key(b"" as &[u8])
        .headers(headers);

    let meta = producer
        .send(record)
        .expect("send with headers should succeed");
    assert_eq!(meta.topic, "header-topic");
}

#[test]
fn test_producer_send_with_partition() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    let record = BaseRecord::to("partitioned-topic")
        .payload(b"partitioned-msg" as &[u8])
        .key(b"" as &[u8])
        .partition(2);

    let meta = producer.send(record).expect("send should succeed");
    assert_eq!(meta.partition, 2);
}

#[test]
fn test_producer_metadata() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    let record: BaseRecord<[u8], [u8]> = BaseRecord::to("meta-topic")
        .payload(b"data" as &[u8])
        .key(b"" as &[u8]);
    producer.send(record).expect("send");

    let meta = producer.metadata(None).expect("get metadata");
    assert_eq!(meta.broker_id, "test-broker");
    assert!(!meta.topics.is_empty());

    let topic_meta = meta
        .topics
        .iter()
        .find(|t| t.name == "meta-topic")
        .expect("find topic");
    assert_eq!(topic_meta.partitions.len(), 3);
}

#[test]
fn test_producer_metadata_single_topic() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    let record: BaseRecord<[u8], [u8]> = BaseRecord::to("single-meta-topic")
        .payload(b"data" as &[u8])
        .key(b"" as &[u8]);
    producer.send(record).expect("send");

    let meta = producer
        .metadata(Some("single-meta-topic"))
        .expect("get metadata");
    assert_eq!(meta.topics.len(), 1);
    assert_eq!(meta.topics[0].name, "single-meta-topic");
}

#[test]
fn test_broker_watermarks() {
    let mut config2 = RmqClientConfig::new();
    config2.set("broker.id", "wm-broker");
    config2.set("default.num.partitions", "2");

    let producer2 = BaseProducer::new(&config2).expect("producer2");
    for i in 0..5 {
        let payload = format!("msg-{}", i);
        let record: BaseRecord<[u8], [u8]> = BaseRecord::to("wm-topic")
            .payload(payload.as_bytes())
            .key(b"" as &[u8])
            .partition(0);
        producer2.send(record).expect("send");
    }

    let (_oldest, newest) = producer2.watermarks("wm-topic", 0).expect("watermarks");
    assert!(newest >= 4, "newest offset should be >= 4, got {}", newest);
}

#[test]
fn test_broker_shutdown() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "shutdown-broker");
    let broker = Broker::new(config).expect("create broker");
    broker.shutdown().expect("shutdown should succeed");
}

#[test]
fn test_consumer_creation() {
    let config = make_config_with_group("test-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");
    drop(consumer);
}

#[test]
fn test_consumer_subscribe() {
    let config = make_config_with_group("sub-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");

    let result = consumer.subscribe(&["sub-topic-1", "sub-topic-2"]);
    assert!(result.is_ok(), "subscribe should succeed: {:?}", result);

    let assignment = consumer.assignment().expect("get assignment");
    assert!(assignment.count() > 0, "should have assigned partitions");
}

#[test]
fn test_consumer_manual_assign() {
    let config = make_config_with_group("assign-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("manual-topic", 0);
    tpl.add_partition("manual-topic", 1);
    tpl.add_partition("manual-topic", 2);

    consumer.assign(&tpl).expect("assign should succeed");

    let assignment = consumer.assignment().expect("get assignment");
    assert_eq!(assignment.count(), 3);
}

#[test]
fn test_consumer_pause_resume() {
    let config = make_config_with_group("pause-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("pause-topic", 0);
    tpl.add_partition("pause-topic", 1);

    consumer.assign(&tpl).expect("assign");

    let mut pause_tpl = TopicPartitionList::new();
    pause_tpl.add_partition("pause-topic", 0);

    consumer.pause(&pause_tpl).expect("pause should succeed");
    consumer.resume(&pause_tpl).expect("resume should succeed");
}

#[test]
fn test_consumer_seek() {
    let config = make_config_with_group("seek-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("seek-topic", 0);

    consumer.assign(&tpl).expect("assign");
    consumer
        .seek("seek-topic", 0, Offset::Offset(42))
        .expect("seek should succeed");

    let position = consumer.position().expect("get position");
    let elem = position
        .find_partition("seek-topic", 0)
        .expect("find partition");
    assert_eq!(elem.offset, Offset::Offset(42));
}

#[test]
fn test_consumer_commit_and_committed() {
    let config = make_config_with_group("commit-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");

    consumer
        .subscribe(&["commit-topic", "commit-topic-2"])
        .expect("subscribe to register group");

    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset("commit-topic", 0, Offset::Offset(10));

    consumer
        .commit(&commit_tpl, CommitMode::Sync)
        .expect("commit");

    let committed = consumer.committed().expect("get committed");
    assert!(committed.count() > 0);
}

#[test]
fn test_consumer_unsubscribe() {
    let config = make_config_with_group("unsub-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");

    consumer.subscribe(&["unsub-topic"]).expect("subscribe");
    consumer.unsubscribe().expect("unsubscribe should succeed");

    let assignment = consumer.assignment().expect("get assignment");
    assert_eq!(assignment.count(), 0);
}

#[test]
fn test_topic_partition_list() {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("topic-a", 0);
    tpl.add_partition("topic-a", 1);
    tpl.add_partition_offset("topic-b", 0, Offset::Offset(42));

    assert_eq!(tpl.count(), 3);

    let elem = tpl.find_partition("topic-a", 0).expect("find");
    assert_eq!(elem.partition, 0);

    let elems = tpl.elements_for_topic("topic-a");
    assert_eq!(elems.len(), 2);

    assert!(tpl.find_partition("nonexistent", 0).is_none());
}

#[test]
fn test_offset_variants() {
    assert_eq!(Offset::Beginning, Offset::Beginning);
    assert_eq!(Offset::Offset(42), Offset::Offset(42));
    assert_eq!(Offset::OffsetTail(10), Offset::OffsetTail(10));
    assert!(Offset::Beginning != Offset::End);
}

#[test]
fn test_headers() {
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "k1".to_owned(),
            value: Some(b"v1".to_vec()),
        })
        .insert(Header {
            key: "k2".to_owned(),
            value: None,
        });

    assert_eq!(headers.len(), 2);
    assert_eq!(headers.count(), 2);

    let h1 = headers.get("k1").expect("find k1");
    assert_eq!(h1.value.as_deref(), Some(&b"v1"[..]));

    let h2 = headers.get_at(1).expect("get at 1");
    assert_eq!(h2.key, "k2");
    assert!(h2.value.is_none());

    assert!(headers.get("k3").is_none());
    assert!(!headers.is_empty());

    let all: Vec<_> = headers.iter().collect();
    assert_eq!(all.len(), 2);
}

#[test]
fn test_owned_message() {
    let msg = OwnedMessage {
        payload: Some(b"hello".to_vec()),
        key: Some(b"world".to_vec()),
        topic: "test-topic".to_owned(),
        partition: 1,
        offset: 42,
        timestamp: Timestamp::CreateTime(1234567890),
        headers: None,
    };

    assert_eq!(msg.topic(), "test-topic");
    assert_eq!(msg.partition(), 1);
    assert_eq!(msg.offset(), 42);
    assert_eq!(msg.payload().unwrap(), b"hello");
    assert_eq!(msg.key().unwrap(), b"world");
    assert!(msg.headers().is_none());
}

#[test]
fn test_timestamp() {
    let ts = Timestamp::now();
    assert!(ts.to_millis().unwrap() > 0);

    let ts2 = Timestamp::CreateTime(1000);
    assert_eq!(ts2.to_millis(), Some(1000));

    assert_eq!(Timestamp::NotAvailable.to_millis(), None);
}

#[test]
fn test_producer_flush_and_in_flight() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    producer.flush().expect("flush should succeed");
    assert_eq!(producer.in_flight_count(), 0);
}

#[test]
fn test_producer_clone() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    let cloned = producer.clone();

    let record: BaseRecord<[u8], [u8]> = BaseRecord::to("clone-topic")
        .payload(b"cloned" as &[u8])
        .key(b"" as &[u8]);
    let meta = cloned.send(record).expect("send from clone");
    assert_eq!(meta.topic, "clone-topic");
}

#[test]
fn test_consumer_clone() {
    let config = make_config_with_group("clone-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");
    let _cloned = consumer.clone();
}

#[test]
fn test_error_topic_not_found() {
    let config = make_config();
    let producer = BaseProducer::new(&config).expect("create producer");

    let result = producer.metadata(Some("nonexistent-topic"));
    assert!(result.is_err());
}

#[test]
fn test_producer_from_config() {
    let config = make_config();
    let producer: BaseProducer = FromRmqConfig::from_config(&config).expect("from config");
    drop(producer);
}

#[test]
fn test_consumer_poll_timeout() {
    let config = make_config_with_group("poll-group");
    let consumer = BaseConsumer::new(&config).expect("create consumer");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("poll-topic", 0);
    consumer.assign(&tpl).expect("assign");

    let result = consumer.poll(Duration::from_millis(10));
    assert!(result.is_none(), "poll on empty topic should return None");
}

#[test]
fn test_producer_end_to_end() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "e2e-broker");
    config.set("default.num.partitions", "1");

    let producer = BaseProducer::new(&config).expect("producer");
    for i in 0..5 {
        let payload = format!("msg-{}", i);
        let record: BaseRecord<[u8], [u8]> = BaseRecord::to("e2e-topic")
            .payload(payload.as_bytes())
            .key(b"" as &[u8]);
        producer.send(record).expect("send");
    }

    let meta = producer.metadata(Some("e2e-topic")).expect("metadata");
    assert_eq!(meta.topics.len(), 1);
    assert_eq!(meta.topics[0].partitions.len(), 1);

    let pm = &meta.topics[0].partitions[0];
    assert!(
        pm.message_count >= 5,
        "should have at least 5 messages, got {}",
        pm.message_count
    );
}

#[test]
fn test_shared_broker_produce_and_consume() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "shared-broker-e2e");
    config.set("default.num.partitions", "1");

    let producer = BaseProducer::new(&config).expect("producer");
    for i in 0..5 {
        let payload = format!("msg-{}", i);
        let record: BaseRecord<[u8], [u8]> = BaseRecord::to("shared-topic")
            .payload(payload.as_bytes())
            .key(b"" as &[u8]);
        producer.send(record).expect("send");
    }

    let consumer_check = BaseConsumer::new(&config).expect("consumer");
    let consumer_broker = consumer_check.broker().clone();
    drop(consumer_check);
    assert!(
        Arc::ptr_eq(producer.broker(), &consumer_broker),
        "producer and consumer should share the same broker Arc"
    );

    let consumer = BaseConsumer::new(&config).expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("shared-topic", 0);
    consumer.assign(&tpl).expect("assign");
    consumer.seek("shared-topic", 0, Offset::Beginning).expect("seek");

    for i in 0..5 {
        let result = consumer.poll(std::time::Duration::from_secs(1));
        assert!(result.is_some(), "should receive message {}", i);
        let msg = result.unwrap().expect("msg should be ok");
        let payload = format!("msg-{}", i);
        assert_eq!(msg.payload().unwrap(), payload.as_bytes());
    }
}

#[test]
fn test_shared_broker_with_group() {
    let mut producer_config = RmqClientConfig::new();
    producer_config.set("broker.id", "group-shared-broker");
    producer_config.set("default.num.partitions", "2");

    let mut consumer_config = RmqClientConfig::new();
    consumer_config.set("broker.id", "group-shared-broker");
    consumer_config.set("default.num.partitions", "2");
    consumer_config.set("group.id", "test-shared-group");

    let producer = BaseProducer::new(&producer_config).expect("producer");
    let consumer = BaseConsumer::new(&consumer_config).expect("consumer");

    assert!(
        Arc::ptr_eq(producer.broker(), consumer.broker()),
        "producer and consumer should share the same broker Arc"
    );

    consumer
        .subscribe(&["group-shared-topic"])
        .expect("subscribe");

    for i in 0..3 {
        let payload = format!("group-msg-{}", i);
        let record: BaseRecord<[u8], [u8]> = BaseRecord::to("group-shared-topic")
            .payload(payload.as_bytes())
            .key(b"" as &[u8]);
        producer.send(record).expect("send");

        let result = consumer.poll(std::time::Duration::from_secs(1));
        assert!(result.is_some(), "should receive message {}", i);
    }
}

#[test]
fn test_consumer_clone_gets_new_member_id() {
    let config = make_config_with_group("clone-mid-group");
    let consumer = BaseConsumer::new(&config).expect("consumer");
    let cloned = consumer.clone();

    assert!(
        Arc::ptr_eq(consumer.broker(), cloned.broker()),
        "clone should share broker"
    );
}

#[test]
fn test_offset_out_of_range_recovers_to_oldest() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "offset-recovery-broker");
    config.set("default.num.partitions", "1");

    let producer = BaseProducer::new(&config).expect("producer");
    let record: BaseRecord<[u8], [u8]> = BaseRecord::to("recovery-topic")
        .payload(b"msg" as &[u8])
        .key(b"" as &[u8]);
    producer.send(record).expect("send");

    let consumer = BaseConsumer::new(&config).expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("recovery-topic", 0);
    consumer.assign(&tpl).expect("assign");

    consumer
        .seek("recovery-topic", 0, Offset::Offset(9999))
        .expect("seek");

    let result = consumer.poll(std::time::Duration::from_millis(100));
    assert!(
        result.is_some(),
        "should recover from OutOfRange and receive message"
    );
}

#[test]
fn test_decode_payload_string() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "decode-string-broker");
    config.set("default.num.partitions", "1");
    let producer = BaseProducer::new(&config).expect("producer");

    let record = BaseRecord::to("string-topic")
        .payload(b"hello world" as &[u8])
        .key(b"key");
    producer.send(record).expect("send");

    let consumer = BaseConsumer::new(&config).expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("string-topic", 0);
    consumer.assign(&tpl).expect("assign");
    consumer.seek("string-topic", 0, Offset::Beginning).expect("seek");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some());
    let msg = result.unwrap().expect("msg");
    let decoded: String = msg.decode_payload().expect("decode string");
    assert_eq!(decoded, "hello world");
}

#[test]
fn test_decode_payload_vec() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "decode-vec-broker");
    config.set("default.num.partitions", "1");
    let producer = BaseProducer::new(&config).expect("producer");

    let record = BaseRecord::to("vec-topic")
        .payload(b"\x01\x02\x03" as &[u8])
        .key(b"");
    producer.send(record).expect("send");

    let consumer = BaseConsumer::new(&config).expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("vec-topic", 0);
    consumer.assign(&tpl).expect("assign");
    consumer.seek("vec-topic", 0, Offset::Beginning).expect("seek");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some());
    let msg = result.unwrap().expect("msg");
    let decoded: Vec<u8> = msg.decode_payload().expect("decode vec");
    assert_eq!(decoded, vec![1, 2, 3]);
}

#[test]
fn test_decode_key_string() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "decode-key-broker");
    config.set("default.num.partitions", "1");
    let producer = BaseProducer::new(&config).expect("producer");

    let record = BaseRecord::to("key-str-topic")
        .payload(b"data" as &[u8])
        .key(b"my-key" as &[u8]);
    producer.send(record).expect("send");

    let consumer = BaseConsumer::new(&config).expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("key-str-topic", 0);
    consumer.assign(&tpl).expect("assign");
    consumer.seek("key-str-topic", 0, Offset::Beginning).expect("seek");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some());
    let msg = result.unwrap().expect("msg");
    let key: String = msg.decode_key().expect("decode key");
    assert_eq!(key, "my-key");
}

#[test]
fn test_decode_payload_none() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "decode-none-broker");
    config.set("default.num.partitions", "1");
    let producer = BaseProducer::new(&config).expect("producer");

    let record: BaseRecord<[u8], [u8]> = BaseRecord::to("empty-topic");
    producer.send(record).expect("send");

    let consumer = BaseConsumer::new(&config).expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("empty-topic", 0);
    consumer.assign(&tpl).expect("assign");
    consumer.seek("empty-topic", 0, Offset::Beginning).expect("seek");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some());
    let msg = result.unwrap().expect("msg");
    assert!(msg.payload().is_none());
    let decoded: Option<Vec<u8>> = msg.decode_payload();
    assert!(decoded.is_none());
}
