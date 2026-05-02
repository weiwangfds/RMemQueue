//! Async basic example — produce and consume messages using the tokio async API.
//!
//! Run: cargo run --example async_basic

use rmemqueue::*;

#[tokio::main]
async fn main() -> Result<(), RmqError> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "async-basic-broker");
    config.set("default.num.partitions", "2");

    let producer = FutureProducer::new(&config)?;

    for i in 0..10 {
        let payload = format!("async-msg-{}", i);
        let key = format!("key-{}", i);
        let record = FutureRecord::to("async-topic")
            .payload(payload.as_bytes())
            .key(key.as_bytes());

        let meta = producer.send(record).await.map_err(|(e, _)| e)?;
        println!(
            "[FutureProducer] sent -> partition={}, offset={}",
            meta.partition, meta.offset
        );
    }

    let mut consumer_config = RmqClientConfig::new();
    consumer_config.set("broker.id", "async-basic-broker");
    consumer_config.set("default.num.partitions", "2");
    consumer_config.set("group.id", "async-consumer-group");

    let consumer = StreamConsumer::new(&consumer_config)?;
    consumer.subscribe(&["async-topic"])?;

    println!("\n--- Consuming via StreamConsumer::recv ---");
    for i in 0..10 {
        let msg = match tokio::time::timeout(std::time::Duration::from_secs(2), consumer.recv())
            .await
        {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(RmqError::Custom("recv timeout".to_owned())),
        };
        let payload = msg
            .payload()
            .map(|p| String::from_utf8_lossy(p).to_string())
            .unwrap_or_default();
        println!(
            "[StreamConsumer] #{} offset={}, payload={}",
            i + 1,
            msg.offset(),
            payload
        );
    }

    println!("\nDone. All 10 messages produced and consumed asynchronously.");
    Ok(())
}
