//! Consumer group example — demonstrates automatic partition assignment
//! and offset committing within a consumer group.
//!
//! Run: cargo run --example consumer_group

use std::thread;
use std::time::Duration;

use rmemqueue::*;

fn main() -> Result<(), RmqError> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "group-broker");
    config.set("default.num.partitions", "4");

    let producer = BaseProducer::new(&config)?;
    for i in 0..20 {
        let payload = format!("group-msg-{}", i);
        let key = format!("key-{}", i % 4);
        let record = BaseRecord::to("group-topic")
            .payload(payload.as_bytes())
            .key(key.as_bytes());
        producer.send(record).map_err(|(e, _)| e)?;
    }
    println!("[Producer] sent 20 messages to group-topic (4 partitions)");

    let mut handles = Vec::new();

    for consumer_id in 0..2 {
        let config_clone = config.clone();
        let handle = thread::spawn(move || {
            let mut cconfig = config_clone;
            cconfig.set("group.id", "my-consumer-group");

            let consumer = BaseConsumer::new(&cconfig).unwrap();
            consumer.subscribe(&["group-topic"]).unwrap();

            let assignment = consumer.assignment().unwrap();
            let partitions: Vec<String> = assignment
                .elements()
                .iter()
                .map(|e| format!("p{}", e.partition))
                .collect();
            println!(
                "[Consumer-{}] joined group, assigned: {}",
                consumer_id,
                partitions.join(", ")
            );

            let mut count = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while std::time::Instant::now() < deadline {
                if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
                    let payload = String::from_utf8_lossy(msg.payload().unwrap()).to_string();
                    println!(
                        "  [Consumer-{}] partition={}, offset={}, payload={}",
                        consumer_id,
                        msg.partition(),
                        msg.offset(),
                        payload
                    );

                    let _ = consumer.commit_message(&msg, CommitMode::Sync);
                    count += 1;
                }
            }
            println!("[Consumer-{}] total received: {}", consumer_id, count);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("\nDone. Both consumers shared the 4 partitions via consumer group.");
    Ok(())
}
