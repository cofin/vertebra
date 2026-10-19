//! Throughput benchmark for the `vertebra-http` server.
//!
//! Two benchmarks:
//! - **direct_dispatch**: Direct channel path (no bridge, no arena). Target: >100k req/s.
//! - **bridge_dispatch**: Full bridge path (arena + slab + async response). Measures overhead.
//!
//! Run with:
//!   cd src/rs && cargo bench --package vertebra-http --bench server_throughput

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
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

fn bench_direct_dispatch(c: &mut Criterion) {
    use vertebra_http::{
        collector::{BatchCollector, PendingResponse},
        config::ServerConfig,
        dispatcher::Dispatcher,
        policy::DrainPolicy,
        route_table::RouteTable,
        router::NativeRouter,
        server,
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let addr = rt.block_on(async {
        let mut table = RouteTable::new();
        table.insert("GET", "/hello");
        let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());

        // Direct dispatch: no bridge, no arena
        let (dispatcher, mut direct_rx) = Dispatcher::new_direct(4096);
        let dispatcher = Arc::new(dispatcher);

        let (collector, collector_tx) =
            BatchCollector::new(Arc::clone(&dispatcher), DrainPolicy::default_fixed(), 4096);
        tokio::spawn(collector.run());

        // Direct dispatch worker — responds via oneshot, no bridge
        tokio::spawn(async move {
            while let Some(batch) = direct_rx.recv().await {
                for req in batch.requests {
                    if let Some(reply) = req.reply {
                        let _ = reply.send(PendingResponse {
                            status: 200,
                            headers_mp: bytes::Bytes::new(),
                            body: bytes::Bytes::from_static(b"ok"),
                        });
                    }
                }
            }
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let config = Arc::new(ServerConfig {
            bind: addr,
            ..ServerConfig::default()
        });

        tokio::spawn(server::run_with_collector(
            config,
            router,
            dispatcher,
            vec![collector_tx],
            std::future::pending::<()>(),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        addr
    });

    let requests_per_iter: u64 = 1000;
    let url = format!("http://{}/hello", addr);

    let mut group = c.benchmark_group("direct_dispatch");
    group.throughput(Throughput::Elements(requests_per_iter));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    group.bench_function("http11_concurrent_1000", |b| {
        b.to_async(&rt).iter(|| {
            let url = url.clone();
            async move {
                let client = reqwest::Client::builder()
                    .pool_max_idle_per_host(64)
                    .build()
                    .unwrap();

                let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
                let mut handles = Vec::with_capacity(requests_per_iter as usize);

                for _ in 0..requests_per_iter {
                    let client = client.clone();
                    let url = url.clone();
                    let sem = Arc::clone(&semaphore);
                    handles.push(tokio::spawn(async move {
                        let _permit = sem.acquire().await.unwrap();
                        let _ = client.get(&url).send().await.unwrap();
                    }));
                }

                for h in handles {
                    h.await.unwrap();
                }
            }
        });
    });

    group.finish();
}

fn bench_bridge_dispatch(c: &mut Criterion) {
    use vertebra_core::bridge::{BatchBridge, BridgeResponse};
    use vertebra_http::{
        collector::{BatchCollector, PendingResponse},
        config::ServerConfig,
        dispatcher::Dispatcher,
        policy::DrainPolicy,
        response::encode_response_headers,
        route_table::RouteTable,
        router::NativeRouter,
        server,
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let addr = rt.block_on(async {
        let mut table = RouteTable::new();
        table.insert("GET", "/hello");
        let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());

        let _ = BatchBridge::init(512, 1_048_576, 4, 1);
        let dispatcher = Arc::new(Dispatcher::new());
        let (collector, collector_tx) =
            BatchCollector::new(Arc::clone(&dispatcher), DrainPolicy::default_fixed(), 4096);

        tokio::spawn(collector.run());

        // Simulated Python worker (bridge path)
        tokio::spawn(async move {
            loop {
                let batch = tokio::task::spawn_blocking(|| {
                    BatchBridge::get().and_then(|b| b.poll_batch(Duration::from_millis(50)))
                })
                .await
                .unwrap();

                if let Some(snap) = batch {
                    let responses: Vec<BridgeResponse> = (0..snap.len())
                        .map(|i| BridgeResponse {
                            request_id: snap.node(i).correlation_id,
                            status: 200,
                            headers: Vec::new(),
                            body: bytes::Bytes::from_static(b"ok"),
                        })
                        .collect();
                    if let Some(bridge) = BatchBridge::get() {
                        let _ = bridge.submit_responses_async(responses).await;
                    }
                }
            }
        });

        // Async response reader (no spawn_blocking, no polling)
        let dispatcher_for_resp = Arc::clone(&dispatcher);
        tokio::spawn(async move {
            let bridge = BatchBridge::get().expect("bridge not initialized");
            while let Some(resps) = bridge.recv_responses().await {
                for resp in resps {
                    dispatcher_for_resp.complete(
                        resp.request_id,
                        PendingResponse {
                            status: resp.status,
                            headers_mp: encode_response_headers(&resp.headers),
                            body: resp.body,
                        },
                    );
                }
            }
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let config = Arc::new(ServerConfig {
            bind: addr,
            ..ServerConfig::default()
        });

        tokio::spawn(server::run_with_collector(
            config,
            router,
            dispatcher,
            vec![collector_tx],
            std::future::pending::<()>(),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        addr
    });

    let requests_per_iter: u64 = 1000;
    let url = format!("http://{}/hello", addr);

    let mut group = c.benchmark_group("bridge_dispatch");
    group.throughput(Throughput::Elements(requests_per_iter));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    group.bench_function("http11_concurrent_1000", |b| {
        b.to_async(&rt).iter(|| {
            let url = url.clone();
            async move {
                let client = reqwest::Client::builder()
                    .pool_max_idle_per_host(64)
                    .build()
                    .unwrap();

                let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
                let mut handles = Vec::with_capacity(requests_per_iter as usize);

                for _ in 0..requests_per_iter {
                    let client = client.clone();
                    let url = url.clone();
                    let sem = Arc::clone(&semaphore);
                    handles.push(tokio::spawn(async move {
                        let _permit = sem.acquire().await.unwrap();
                        let _ = client.get(&url).send().await.unwrap();
                    }));
                }

                for h in handles {
                    h.await.unwrap();
                }
            }
        });
    });

    group.finish();
}

fn bench_bridge_dispatch_adaptive(c: &mut Criterion) {
    use vertebra_core::bridge::{BatchBridge, BridgeResponse};
    use vertebra_http::{
        collector::{BatchCollector, PendingResponse},
        config::ServerConfig,
        dispatcher::Dispatcher,
        policy::DrainPolicy,
        response::encode_response_headers,
        route_table::RouteTable,
        router::NativeRouter,
        server,
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let addr = rt.block_on(async {
        let mut table = RouteTable::new();
        table.insert("GET", "/hello");
        let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());

        // NOTE: BatchBridge is a global singleton — if bench_bridge_dispatch ran
        // first, this init will fail. That's fine; the existing bridge is reused.
        let _ = BatchBridge::init(512, 1_048_576, 4, 1);
        let dispatcher = Arc::new(Dispatcher::new());
        let (collector, collector_tx) = BatchCollector::new(
            Arc::clone(&dispatcher),
            DrainPolicy::default_adaptive(),
            4096,
        );

        tokio::spawn(collector.run());

        // Simulated Python worker (bridge path)
        tokio::spawn(async move {
            loop {
                let batch = tokio::task::spawn_blocking(|| {
                    BatchBridge::get().and_then(|b| b.poll_batch(Duration::from_millis(50)))
                })
                .await
                .unwrap();

                if let Some(snap) = batch {
                    let responses: Vec<BridgeResponse> = (0..snap.len())
                        .map(|i| BridgeResponse {
                            request_id: snap.node(i).correlation_id,
                            status: 200,
                            headers: Vec::new(),
                            body: bytes::Bytes::from_static(b"ok"),
                        })
                        .collect();
                    if let Some(bridge) = BatchBridge::get() {
                        let _ = bridge.submit_responses_async(responses).await;
                    }
                }
            }
        });

        let dispatcher_for_resp = Arc::clone(&dispatcher);
        tokio::spawn(async move {
            let bridge = BatchBridge::get().expect("bridge not initialized");
            while let Some(resps) = bridge.recv_responses().await {
                for resp in resps {
                    dispatcher_for_resp.complete(
                        resp.request_id,
                        PendingResponse {
                            status: resp.status,
                            headers_mp: encode_response_headers(&resp.headers),
                            body: resp.body,
                        },
                    );
                }
            }
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let config = Arc::new(ServerConfig {
            bind: addr,
            ..ServerConfig::default()
        });

        tokio::spawn(server::run_with_collector(
            config,
            router,
            dispatcher,
            vec![collector_tx],
            std::future::pending::<()>(),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        addr
    });

    let requests_per_iter: u64 = 1000;
    let url = format!("http://{}/hello", addr);

    let mut group = c.benchmark_group("bridge_dispatch_adaptive");
    group.throughput(Throughput::Elements(requests_per_iter));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    group.bench_function("http11_concurrent_1000", |b| {
        b.to_async(&rt).iter(|| {
            let url = url.clone();
            async move {
                let client = reqwest::Client::builder()
                    .pool_max_idle_per_host(64)
                    .build()
                    .unwrap();

                let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
                let mut handles = Vec::with_capacity(requests_per_iter as usize);

                for _ in 0..requests_per_iter {
                    let client = client.clone();
                    let url = url.clone();
                    let sem = Arc::clone(&semaphore);
                    handles.push(tokio::spawn(async move {
                        let _permit = sem.acquire().await.unwrap();
                        let _ = client.get(&url).send().await.unwrap();
                    }));
                }

                for h in handles {
                    h.await.unwrap();
                }
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_router_lookup,
    bench_direct_dispatch,
    bench_bridge_dispatch,
    bench_bridge_dispatch_adaptive
);
criterion_main!(benches);
