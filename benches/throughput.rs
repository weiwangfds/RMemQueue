use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rmemqueue::*;
use std::thread;

fn make_bench_config() -> RmqClientConfig {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "bench-broker");
    config.set("default.num.partitions", "4");
    config
}

fn bench_single_thread_produce(c: &mut Criterion) {
    let config = make_bench_config();
    let producer = BaseProducer::new(&config).expect("producer");

    c.bench_function("single_thread_produce_1msg", |b| {
        b.iter(|| {
            let record = BaseRecord::to("bench-topic")
                .payload(b"benchmark-payload")
                .key(b"bench-key");
            producer.send(record).expect("send")
        })
    });
}

fn bench_single_thread_produce_batch(c: &mut Criterion) {
    let config = make_bench_config();
    let producer = BaseProducer::new(&config).expect("producer");

    let mut group = c.benchmark_group("batch_produce");
    for batch_size in [100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    for i in 0..size {
                        let payload = format!("msg-{}", i);
                        let record = BaseRecord::to("bench-batch-topic")
                            .payload(payload.as_bytes())
                            .key(b"key");
                        producer.send(record).expect("send");
                    }
                })
            },
        );
    }
    group.finish();
}

fn bench_multi_thread_produce(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_thread_produce");
    for num_threads in [2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|t| {
                            let config = {
                                let mut c = RmqClientConfig::new();
                                c.set("broker.id", format!("bench-mt-{}", t));
                                c.set("default.num.partitions", "4");
                                c
                            };
                            thread::spawn(move || {
                                let producer = BaseProducer::new(&config).expect("producer");
                                for i in 0..1000 {
                                    let payload = format!("t{}-msg-{}", t, i);
                                    let record = BaseRecord::to("mt-bench-topic")
                                        .payload(payload.as_bytes())
                                        .key(b"" as &[u8]);
                                    producer.send(record).expect("send");
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().expect("thread panicked");
                    }
                })
            },
        );
    }
    group.finish();
}

fn bench_consumer_poll(c: &mut Criterion) {
    let mut config = RmqClientConfig::new();
    config.set("broker.id", "bench-consumer-broker");
    config.set("default.num.partitions", "1");
    config.set("group.id", "bench-consumer-group");

    let producer = BaseProducer::new(&config).expect("producer");
    for i in 0..10000 {
        let payload = format!("consume-msg-{}", i);
        let record: BaseRecord<[u8], [u8]> =
            BaseRecord::to("consume-bench-topic").payload(payload.as_bytes());
        producer.send(record).expect("send");
    }

    let consumer = BaseConsumer::new(&config).expect("consumer");
    consumer
        .subscribe(&["consume-bench-topic"])
        .expect("subscribe");

    c.bench_function("consumer_poll", |b| {
        b.iter(|| {
            let _ = consumer.poll(std::time::Duration::from_micros(1));
        })
    });
}

fn bench_headers_creation(c: &mut Criterion) {
    c.bench_function("headers_insert_10", |b| {
        b.iter(|| {
            let mut headers = OwnedHeaders::new();
            for i in 0..10 {
                headers = headers.insert(Header {
                    key: format!("h-{}", i),
                    value: Some(format!("v-{}", i).into_bytes()),
                });
            }
            headers
        })
    });
}

criterion_group!(
    benches,
    bench_single_thread_produce,
    bench_single_thread_produce_batch,
    bench_multi_thread_produce,
    bench_consumer_poll,
    bench_headers_creation,
);
criterion_main!(benches);
