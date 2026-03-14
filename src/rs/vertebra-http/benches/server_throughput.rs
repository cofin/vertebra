//! Throughput benchmark for the `vertebra-http` server.
//!
//! Target: >100,000 req/s on a single machine with HTTP/1.1 keep-alive.
//!
//! Run with:
//!   cd src/rs && cargo bench --package vertebra-http --bench server_throughput

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use std::time::Duration;

fn bench_router_lookup(c: &mut Criterion) {
    use vertebra_http::{route_table::RouteTable, router::NativeRouter};

    let mut table = RouteTable::new();
    for i in 0..100 {
        table.insert("GET", &format!("/api/v1/resource/{}", i));
    }
    let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());

    let mut group = c.benchmark_group("router");
    group.throughput(Throughput::Elements(1));

    group.bench_function("lookup_static_100_routes", |b| {
        b.iter(|| {
            let _ = router.lookup("GET", "/api/v1/resource/50");
        });
    });

    group.finish();
}

fn bench_server_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let addr = rt.block_on(async {
        use vertebra_http::{
            collector::{BatchCollector, PendingResponse},
            config::ServerConfig,
            dispatcher::Dispatcher,
            policy::DrainPolicy,
            route_table::RouteTable,
            router::NativeRouter,
            server,
        };

        let mut table = RouteTable::new();
        table.insert("GET", "/hello");
        let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());

        let (dispatcher, mut ring_rx) = Dispatcher::new(512);
        let dispatcher = Arc::new(dispatcher);
        let (collector, collector_tx) = BatchCollector::new(
            Arc::clone(&dispatcher),
            DrainPolicy::default_fixed(),
            4096,
        );

        tokio::spawn(collector.run());

        let dispatcher_clone = Arc::clone(&dispatcher);
        tokio::spawn(async move {
            while let Some(batch) = ring_rx.recv().await {
                for id in batch.correlation_ids {
                    dispatcher_clone.complete(
                        id,
                        PendingResponse {
                            status: 200,
                            headers_mp: bytes::Bytes::new(),
                            body: bytes::Bytes::from_static(b"ok"),
                        },
                    );
                }
            }
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let config = Arc::new({
            let mut c = ServerConfig::default();
            c.bind = addr;
            c
        });

        tokio::spawn(server::run_with_collector(
            config,
            router,
            dispatcher,
            collector_tx,
            std::future::pending::<()>(),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        addr
    });

    let client = reqwest::blocking::Client::builder()
        .pool_max_idle_per_host(32)
        .build()
        .unwrap();

    let url = format!("http://{}/hello", addr);

    let mut group = c.benchmark_group("server_throughput");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("http11_hello", |b| {
        b.iter(|| {
            let _ = client.get(&url).send().unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_router_lookup, bench_server_throughput);
criterion_main!(benches);
