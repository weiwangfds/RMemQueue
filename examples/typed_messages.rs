//! Typed messages example — zero-copy produce/consume without serialization.
//!
//! Messages are passed as Arc<T> directly, avoiding any byte serialization.
//! This requires the InMemoryBackend (the default).
//!
//! Run: cargo run --example typed_messages

use std::sync::Arc;
use std::time::Duration;

use rmemqueue::*;

#[derive(Debug, Clone)]
struct Order {
    id: u64,
    product: String,
    quantity: u32,
    price: f64,
}

fn main() -> Result<(), RmqError> {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "typed-broker");
    config.set("default.num.partitions", "2");

    let producer: TypedProducer<Order> = TypedProducer::new(&config)?;

    let orders = vec![
        Order {
            id: 1001,
            product: "Widget".to_string(),
            quantity: 5,
            price: 9.99,
        },
        Order {
            id: 1002,
            product: "Gadget".to_string(),
            quantity: 2,
            price: 24.99,
        },
        Order {
            id: 1003,
            product: "Doohickey".to_string(),
            quantity: 10,
            price: 3.50,
        },
    ];

    for order in &orders {
        let arc_order = Arc::new(order.clone());
        let meta = producer.send("orders", arc_order, None)?;
        println!(
            "[TypedProducer] order {} -> partition={}, offset={}",
            order.id, meta.partition, meta.offset
        );
    }

    let consumer: TypedConsumer<Order> = TypedConsumer::new(&config)?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("orders", 0);
    tpl.add_partition("orders", 1);
    consumer.assign(&tpl)?;
    consumer.seek("orders", 0, Offset::Beginning)?;
    consumer.seek("orders", 1, Offset::Beginning)?;

    println!("\n--- Consuming typed orders ---");
    let mut received = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while received < orders.len() && std::time::Instant::now() < deadline {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
            let order = msg.payload();
            println!(
                "[TypedConsumer] order={}, product={}, qty={}, price={:.2}",
                order.id, order.product, order.quantity, order.price
            );
            received += 1;
        }
    }

    println!(
        "\nDone. Received {}/{} typed orders (zero-copy, no serialization).",
        received,
        orders.len()
    );
    Ok(())
}
