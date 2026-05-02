use rmemqueue::*;

#[tokio::test]
async fn test_future_producer_send() {    let mut config = RmqClientConfig::new();
    config.set("broker.id", "async-broker");
    config.set("default.num.partitions", "2");

    let producer = FutureProducer::new(&config).expect("create future producer");

    let record = FutureRecord::to("async-topic")
        .payload(b"async-hello")
        .key(b"async-key");

    let result = producer
        .send(record)
        .await
        .expect("async send should succeed");
    assert_eq!(result.topic, "async-topic");
    assert!(result.partition >= 0 && result.partition < 2);
    assert_eq!(result.offset, 0);
}

#[tokio::test]
async fn test_future_producer_multiple() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "async-multi-broker");
    config.set("default.num.partitions", "3");

    let producer = FutureProducer::new(&config).expect("create future producer");

    for i in 0..20 {
        let key = format!("key-{}", i);
        let payload = format!("payload-{}", i);
        let record = FutureRecord::to("async-multi-topic")
            .payload(payload.as_bytes())
            .key(key.as_bytes());

        let result = producer.send(record).await.expect("async send");
        assert_eq!(result.topic, "async-multi-topic");
        assert!(result.offset >= 0);
    }
}

#[tokio::test]
async fn test_future_producer_with_headers() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "async-header-broker");

    let producer = FutureProducer::new(&config).expect("create future producer");

    let headers = OwnedHeaders::new().insert(Header {
        key: "x-request-id".to_owned(),
        value: Some(b"abc-123".to_vec()),
    });

    let record = FutureRecord::to("async-header-topic")
        .payload(b"data" as &[u8])
        .key(b"" as &[u8])
        .headers(headers);

    let result = producer
        .send(record)
        .await
        .expect("async send with headers");
    assert_eq!(result.topic, "async-header-topic");
}

#[tokio::test]
async fn test_future_producer_clone() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "async-clone-broker");

    let producer = FutureProducer::new(&config).expect("create future producer");
    let cloned = producer.clone();

    let record = FutureRecord::to("async-clone-topic")
        .payload(b"cloned-data" as &[u8])
        .key(b"" as &[u8]);

    let result = cloned
        .send(record)
        .await
        .expect("send from cloned producer");
    assert_eq!(result.topic, "async-clone-topic");
}

#[tokio::test]
async fn test_stream_consumer_creation() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "stream-broker");
    config.set("group.id", "stream-group");

    let consumer = StreamConsumer::new(&config).expect("create stream consumer");
    consumer
        .subscribe(&["stream-topic"])
        .expect("subscribe should succeed");
}

#[tokio::test]
async fn test_stream_producer_send_owned() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "sp-broker");
    config.set("default.num.partitions", "2");

    let producer = StreamProducer::new(&config).expect("create stream producer");

    let record = OwnedRecord::to("sp-topic")
        .payload(b"hello".to_vec())
        .key(b"key".to_vec());

    let result = producer.send(record).expect("send owned record");
    assert_eq!(result.topic, "sp-topic");
    assert!(result.partition >= 0 && result.partition < 2);
    assert_eq!(result.offset, 0);
}

#[tokio::test]
async fn test_stream_producer_send_record() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "sp-record-broker");
    config.set("default.num.partitions", "3");

    let producer = StreamProducer::new(&config).expect("create stream producer");

    let record = FutureRecord::to("sp-record-topic")
        .payload(b"payload")
        .key(b"key");

    let result = producer.send_record(record).expect("send record");
    assert_eq!(result.topic, "sp-record-topic");
}

#[tokio::test]
async fn test_stream_producer_clone() {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "sp-clone-broker");

    let producer = StreamProducer::new(&config).expect("create stream producer");
    let cloned = producer.clone();

    let record = OwnedRecord::to("sp-clone-topic").payload(b"data".to_vec());
    let result = cloned.send(record).expect("send from clone");
    assert_eq!(result.topic, "sp-clone-topic");
}

#[tokio::test]
async fn test_producer_sink_basic() {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use futures_sink::Sink;

    let mut config = RmqClientConfig::new();
    config.set("broker.id", "sink-broker");
    config.set("default.num.partitions", "2");

    let producer = StreamProducer::new(&config).expect("create stream producer");
    let mut sink = producer.sink();

    let record = OwnedRecord::to("sink-topic")
        .payload(b"sink-data".to_vec())
        .key(b"sink-key".to_vec());

    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(Pin::new(&mut sink).poll_ready(&mut cx), Poll::Ready(Ok(()))));
    Pin::new(&mut sink).start_send(record).expect("start_send");
    assert!(matches!(Pin::new(&mut sink).poll_flush(&mut cx), Poll::Ready(Ok(()))));
    assert!(matches!(Pin::new(&mut sink).poll_close(&mut cx), Poll::Ready(Ok(()))));
}

#[tokio::test]
async fn test_stream_produce_consume_pipe() {
    use futures_util::StreamExt;

    let mut config = RmqClientConfig::new();
    config.set("broker.id", "pipe-broker");
    config.set("default.num.partitions", "1");

    let producer = StreamProducer::new(&config).expect("create stream producer");
    for i in 0..5 {
        let record = OwnedRecord::to("pipe-topic")
            .payload(format!("msg-{}", i).into_bytes());
        producer.send(record).expect("produce");
    }

    let mut consumer_config = RmqClientConfig::new();
    consumer_config.set("broker.id", "pipe-broker");

    let consumer = StreamConsumer::new(&consumer_config).expect("create stream consumer");
    consumer.subscribe(&["pipe-topic"]).expect("subscribe");

    let mut stream = consumer.stream();
    let mut received = Vec::new();
    for _ in 0..5 {
        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stream.next(),
        )
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("stream error");
        received.push(msg.payload().unwrap().to_vec());
    }
    assert_eq!(received.len(), 5);
}
