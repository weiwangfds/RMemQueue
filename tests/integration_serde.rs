use rmemqueue::*;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct OrderCreated {
    order_id: u64,
    product: String,
    quantity: u32,
}

fn make_config(id: &str) -> RmqClientConfig {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", id);
    config.set("default.num.partitions", "1");
    config
}

#[test]
fn test_serde_roundtrip() {
    let config = make_config("serde-roundtrip-broker");
    let producer = BaseProducer::new(&config).expect("producer");

    let order = OrderCreated {
        order_id: 42,
        product: "widget".to_owned(),
        quantity: 3,
    };

    let payload = rmemqueue::to_json_bytes(&order);
    let record = BaseRecord::to("order-events")
        .payload(payload.as_slice())
        .key(b"order-42");
    producer.send(record).expect("send");

    let consumer = BaseConsumer::new(&config).expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("order-events", 0);
    consumer.assign(&tpl).expect("assign");
    consumer
        .seek("order-events", 0, Offset::Beginning)
        .expect("seek");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some());
    let msg = result.unwrap().expect("msg");

    let decoded: SerdeJson<OrderCreated> = msg.decode_payload().expect("decode");
    assert_eq!(decoded.0, order);
}

#[test]
fn test_serde_via_from_json_bytes() {
    let order = OrderCreated {
        order_id: 99,
        product: "gadget".to_owned(),
        quantity: 1,
    };
    let bytes = to_json_bytes(&order);
    let restored: OrderCreated = from_json_bytes(&bytes).expect("from_json_bytes");
    assert_eq!(restored, order);
}

#[test]
fn test_serde_decode_key() {
    let config = make_config("serde-key-broker");
    let producer = BaseProducer::new(&config).expect("producer");

    let order = OrderCreated {
        order_id: 7,
        product: "bolt".to_owned(),
        quantity: 100,
    };

    let payload = to_json_bytes(&order);
    let key = to_json_bytes(&serde_json::json!({ "order_id": 7 }));
    let record = BaseRecord::to("keyed-order-events")
        .payload(payload.as_slice())
        .key(key.as_slice());
    producer.send(record).expect("send");

    let consumer = BaseConsumer::new(&config).expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("keyed-order-events", 0);
    consumer.assign(&tpl).expect("assign");
    consumer
        .seek("keyed-order-events", 0, Offset::Beginning)
        .expect("seek");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some());
    let msg = result.unwrap().expect("msg");

    let decoded_order: SerdeJson<OrderCreated> = msg.decode_payload().expect("decode payload");
    assert_eq!(decoded_order.0, order);

    let key_val: serde_json::Value = from_json_bytes(msg.key().unwrap()).expect("decode key");
    assert_eq!(key_val["order_id"], 7);
}
