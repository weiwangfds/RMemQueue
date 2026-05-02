use std::sync::Arc;
use rmemqueue::*;

#[derive(Debug, PartialEq, Clone)]
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
fn test_typed_produce_and_consume() {
    let config = make_config("typed-basic-broker");
    let producer: TypedProducer<OrderCreated> = TypedProducer::new(&config).expect("producer");
    let consumer: TypedConsumer<OrderCreated> = TypedConsumer::new(&config).expect("consumer");

    let order = Arc::new(OrderCreated {
        order_id: 1,
        product: "widget".to_owned(),
        quantity: 5,
    });
    let meta = producer.send("orders", order.clone(), None).expect("send");
    assert_eq!(meta.topic, "orders");
    assert_eq!(meta.offset, 0);

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("orders", 0);
    consumer.assign(&tpl).expect("assign");
    consumer.seek("orders", 0, Offset::Beginning).expect("seek");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some(), "should receive typed message");
    let msg = result.unwrap().expect("msg ok");
    assert_eq!(msg.payload().order_id, 1);
    assert_eq!(msg.payload().product, "widget");
    assert_eq!(msg.payload().quantity, 5);
    assert!(msg.key().is_none());
}

#[test]
fn test_typed_with_key() {
    let config = make_config("typed-key-broker");

    type MyKey = String;

    let producer: TypedProducer<OrderCreated, MyKey> = TypedProducer::new(&config).expect("producer");
    let consumer: TypedConsumer<OrderCreated, MyKey> = TypedConsumer::new(&config).expect("consumer");

    let order = Arc::new(OrderCreated {
        order_id: 2,
        product: "gadget".to_owned(),
        quantity: 10,
    });
    let key = Arc::new("order-2".to_owned());
    producer.send("keyed-orders", order, Some(key)).expect("send");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("keyed-orders", 0);
    consumer.assign(&tpl).expect("assign");
    consumer.seek("keyed-orders", 0, Offset::Beginning).expect("seek");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some());
    let msg = result.unwrap().expect("msg ok");
    assert_eq!(msg.payload().order_id, 2);
    let key = msg.key().expect("should have key");
    assert_eq!(key, "order-2");
}

#[test]
fn test_typed_multiple_messages() {
    let config = make_config("typed-multi-broker");

    let producer: TypedProducer<String> = TypedProducer::new(&config).expect("producer");
    let consumer: TypedConsumer<String> = TypedConsumer::new(&config).expect("consumer");

    for i in 0..5 {
        let msg = Arc::new(format!("msg-{}", i));
        producer.send("multi-topic", msg, None).expect("send");
    }

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition("multi-topic", 0);
    consumer.assign(&tpl).expect("assign");
    consumer.seek("multi-topic", 0, Offset::Beginning).expect("seek");

    for i in 0..5 {
        let result = consumer.poll(std::time::Duration::from_secs(1));
        assert!(result.is_some(), "should receive message {}", i);
        let msg = result.unwrap().expect("msg ok");
        assert_eq!(*msg.payload(), format!("msg-{}", i));
    }
}

#[test]
fn test_typed_with_group() {
    let mut config = make_config("typed-group-broker");
    config.set("group.id", "typed-test-group");

    let producer: TypedProducer<OrderCreated> = TypedProducer::new(&config).expect("producer");
    let consumer: TypedConsumer<OrderCreated> = TypedConsumer::new(&config).expect("consumer");

    let order = Arc::new(OrderCreated {
        order_id: 99,
        product: "bolt".to_owned(),
        quantity: 50,
    });
    producer.send("group-topic", order, None).expect("send");

    consumer.subscribe(&["group-topic"]).expect("subscribe");

    let result = consumer.poll(std::time::Duration::from_secs(1));
    assert!(result.is_some());
    let msg = result.unwrap().expect("msg ok");
    assert_eq!(msg.payload().order_id, 99);
}

#[test]
fn test_typed_shared_broker_arc() {
    let config = make_config("typed-arc-broker");
    let producer: TypedProducer<String> = TypedProducer::new(&config).expect("producer");
    let consumer: TypedConsumer<String> = TypedConsumer::new(&config).expect("consumer");

    assert!(
        Arc::ptr_eq(producer.broker(), consumer.broker()),
        "typed producer and consumer should share the same broker"
    );
}

#[test]
fn test_typed_clone() {
    let config = make_config("typed-clone-broker");
    let producer: TypedProducer<String> = TypedProducer::new(&config).expect("producer");
    let cloned = producer.clone();

    let msg = Arc::new("cloned".to_owned());
    let meta = cloned.send("clone-topic", msg, None).expect("send");
    assert_eq!(meta.topic, "clone-topic");
}
