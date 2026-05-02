//! Serde JSON example — serialize/deserialize messages with serde.
//!
//! Requires the `serde` feature: cargo run --example serde_json --features serde
//!
//! Demonstrates two approaches:
//!   1. Manual JSON via to_json_bytes / from_json_bytes
//!   2. SerdeJson<T> wrapper with FromBytes trait

use std::time::Duration;

use rmemqueue::*;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Event {
    event_type: String,
    timestamp: u64,
    payload: String,
}

fn main() -> Result<(), RmqError> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "serde-broker");
    config.set("default.num.partitions", "1");

    println!("=== Manual JSON via to_json_bytes / from_json_bytes ===\n");

    let producer = BaseProducer::new(&config)?;

    for i in 0..5 {
        let event = Event {
            event_type: "click".to_string(),
            timestamp: 1000 + i,
            payload: format!("button-{}", i),
        };
        let json_bytes = to_json_bytes(&event);
        let record = BaseRecord::to("events-v1")
            .payload(json_bytes.as_slice())
            .key(event.event_type.as_bytes());
        producer.send(record).map_err(|(e, _)| e)?;
    }
    println!("[Producer] sent 5 events (manual JSON)");

    let consumer = BaseConsumer::new(&config)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("events-v1", 0);
    consumer.assign(&tpl)?;
    consumer.seek("events-v1", 0, Offset::Beginning)?;

    println!("[Consumer] consuming:");
    for _ in 0..5 {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(1)) {
            let event: Option<SerdeJson<Event>> = msg.decode_payload();
            if let Some(SerdeJson(e)) = event {
                println!("  {:?}", e);
            }
        }
    }

    println!("\n=== SerdeJson<T> wrapper ===\n");

    for i in 0..3 {
        let event = Event {
            event_type: "purchase".to_string(),
            timestamp: 2000 + i,
            payload: format!("item-{}", i),
        };
        let json_bytes = to_json_bytes(&event);
        let record: BaseRecord<[u8], [u8]> = BaseRecord::to("events-v2").payload(json_bytes.as_slice());
        producer.send(record).map_err(|(e, _)| e)?;
    }
    println!("[Producer] sent 3 events (SerdeJson<T>)");

    let mut tpl2 = TopicPartitionList::new();
    tpl2.add_partition("events-v2", 0);
    consumer.assign(&tpl2)?;
    consumer.seek("events-v2", 0, Offset::Beginning)?;

    println!("[Consumer] consuming:");
    for _ in 0..3 {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(1)) {
            let wrapper: Option<SerdeJson<Event>> = msg.decode_payload();
            if let Some(SerdeJson(e)) = wrapper {
                println!("  {:?}", e);
            }
        }
    }

    println!("\nDone. Both serde approaches work.");
    Ok(())
}
