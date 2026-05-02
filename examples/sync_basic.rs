//! Sync basic example — produce and consume messages using the synchronous API.
//!
//! Run: cargo run --example sync_basic

use std::time::Duration;

use rmemqueue::*;

fn main() -> Result<(), RmqError> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "sync-basic-broker");
    config.set("default.num.partitions", "3");

    let producer = BaseProducer::new(&config)?;

    for i in 0..10 {
        let key = format!("key-{}", i);
        let payload = format!("message-{}", i);
        let record = BaseRecord::to("test-topic")
            .payload(payload.as_bytes())
            .key(key.as_bytes());

        let meta = producer.send(record).map_err(|(e, _)| e)?;
        println!(
            "[Producer] sent message {} -> partition={}, offset={}",
            i, meta.partition, meta.offset
        );
    }

    producer.flush()?;

    let metadata = producer.metadata(None)?;
    println!("\n[Metadata] broker_id={}", metadata.broker_id);
    for topic in &metadata.topics {
        println!(
            "[Metadata] topic={}, partitions={}",
            topic.name,
            topic.partitions.len()
        );
        for p in &topic.partitions {
            println!(
                "  partition {}: oldest={}, newest={}, count={}",
                p.id, p.oldest_offset, p.newest_offset, p.message_count
            );
        }
    }

    let consumer = BaseConsumer::new(&config)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("test-topic", 0);
    tpl.add_partition("test-topic", 1);
    tpl.add_partition("test-topic", 2);
    consumer.assign(&tpl)?;
    consumer.seek("test-topic", 0, Offset::Beginning)?;
    consumer.seek("test-topic", 1, Offset::Beginning)?;
    consumer.seek("test-topic", 2, Offset::Beginning)?;

    println!("\n--- Consuming messages ---");
    let mut received = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while received < 10 && std::time::Instant::now() < deadline {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(100)) {
            let payload = msg
                .payload()
                .map(|p| String::from_utf8_lossy(p).to_string())
                .unwrap_or_default();
            let key = msg
                .key()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_default();
            println!(
                "[Consumer] offset={}, key={}, payload={}",
                msg.offset(),
                key,
                payload
            );
            received += 1;
        }
    }

    println!("\nDone. Received {}/10 messages.", received);
    Ok(())
}
