//! Throughput benchmark for the `vertebra-http` server.
//!
//! Benchmarks:
//! - **router**: lookup performance across 100 routes.
//! - **direct_dispatch**: Direct channel path (no bridge, no arena). Target: >100k req/s.
//! - **bridge_dispatch**: Full bridge path (arena + slab + async response). Measures overhead.
//!
//! Run with:
//!   cd src/rs && cargo bench --package vertebra-http --bench server_throughput

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

fn bench_router_lookup(c: &mut Criterion) {
    use vertebra_http::routing::{RouteTable, Router};

    let mut table = RouteTable::new();
    for i in 0..100 {
        table.insert("GET", &format!("/api/v1/resource/{}", i));
    }
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());

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
        batcher::{BatchLimit, RequestBatcher},
        body::PendingResponse,
        config::ServerConfig,
        dispatcher::Dispatcher,
        routing::{RouteTable, Router},
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
        let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());

        // Direct dispatch: no bridge, no arena
        let (dispatcher, mut direct_rx) = Dispatcher::new_direct(4096);
        let dispatcher = Arc::new(dispatcher);

        let (collector, collector_tx) =
            RequestBatcher::new(Arc::clone(&dispatcher), BatchLimit::default(), 4096);
        tokio::spawn(collector.run());

        // Direct dispatch worker — responds via oneshot, no bridge
        tokio::spawn(async move {
            while let Some(batch) = direct_rx.recv().await {
                for req in batch.requests {
                    let _ = req.reply.send(PendingResponse {
                        status: 200,
                        headers_mp: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"ok"),
                        stream_rx: None,
                    });
                }
            }
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let config = Arc::new(ServerConfig {
            bind: addr,
            ..ServerConfig::default()
        });

        tokio::spawn(server::run_with_listener(
            listener,
            config,
            router,
            dispatcher,
            vec![collector_tx],
            std::future::pending::<()>(),
        ));

        tokio::time::sleep(Duration::from_millis(10)).await;
        addr
    });

    let requests_per_iter: u64 = 1000;
    let url = format!("http://{}/hello", addr);
    let client = rt.block_on(async {
        let c = reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .no_proxy()
            .http1_only()
            .build()
            .unwrap();
        let mut warm = Vec::with_capacity(32);
        for _ in 0..32 {
            let c = c.clone();
            let u = url.clone();
            warm.push(tokio::spawn(async move {
                let _ = c.get(&u).send().await.unwrap().bytes().await.unwrap();
            }));
        }
        for h in warm {
            h.await.unwrap();
        }
        c
    });

    let mut group = c.benchmark_group("direct_dispatch");
    group.throughput(Throughput::Elements(requests_per_iter));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    group.bench_function("http11_concurrent_1000", |b| {
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let url = url.clone();
            async move {
                let remaining =
                    Arc::new(std::sync::atomic::AtomicI64::new(requests_per_iter as i64));
                let mut workers = Vec::with_capacity(32);
                for _ in 0..32 {
                    let client = client.clone();
                    let url = url.clone();
                    let remaining = Arc::clone(&remaining);
                    workers.push(tokio::spawn(async move {
                        while remaining.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) > 0 {
                            let _ = client
                                .get(&url)
                                .send()
                                .await
                                .unwrap()
                                .bytes()
                                .await
                                .unwrap();
                        }
                    }));
                }
                for w in workers {
                    w.await.unwrap();
                }
            }
        });
    });

    group.finish();
}

async fn setup_bridge_server(limit: vertebra_http::batcher::BatchLimit) -> SocketAddr {
    use vertebra_core::bridge::{BatchBridge, BridgeResponse};
    use vertebra_http::{
        batcher::RequestBatcher,
        body::PendingResponse,
        config::ServerConfig,
        dispatcher::Dispatcher,
        response::encode_response_headers,
        routing::{RouteTable, Router},
        server,
    };

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());

    let _ = BatchBridge::init(512, 1_048_576, 4, 1);
    let dispatcher = Arc::new(Dispatcher::new());
    let (collector, collector_tx) = RequestBatcher::new(Arc::clone(&dispatcher), limit, 4096);

    tokio::spawn(collector.run());

    std::thread::spawn(move || {
        while let Some(bridge) = BatchBridge::get() {
            if let Some(snap) = bridge.poll_batch(Duration::from_millis(50)) {
                let responses: Vec<BridgeResponse> = (0..snap.len())
                    .map(|i| BridgeResponse {
                        request_id: snap.node(i).correlation_id,
                        status: 200,
                        headers: Vec::new(),
                        body: bytes::Bytes::from_static(b"ok"),
                    })
                    .collect();
                let _ = bridge.submit_responses(responses);
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
                        stream_rx: None,
                    },
                );
            }
        }
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = Arc::new(ServerConfig {
        bind: addr,
        ..ServerConfig::default()
    });

    tokio::spawn(server::run_with_listener(
        listener,
        config,
        router,
        dispatcher,
        vec![collector_tx],
        std::future::pending::<()>(),
    ));

    tokio::time::sleep(Duration::from_millis(10)).await;
    addr
}

fn bench_bridge_dispatch(c: &mut Criterion) {
    use vertebra_http::batcher::BatchLimit;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let addr = rt.block_on(setup_bridge_server(BatchLimit { max: 64 }));

    let requests_per_iter: u64 = 1000;
    let url = format!("http://{}/hello", addr);
    let client = rt.block_on(async {
        let c = reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .no_proxy()
            .http1_only()
            .build()
            .unwrap();
        let mut warm = Vec::with_capacity(32);
        for _ in 0..32 {
            let c = c.clone();
            let u = url.clone();
            warm.push(tokio::spawn(async move {
                let _ = c.get(&u).send().await.unwrap().bytes().await.unwrap();
            }));
        }
        for h in warm {
            h.await.unwrap();
        }
        c
    });

    let mut group = c.benchmark_group("bridge_dispatch");
    group.throughput(Throughput::Elements(requests_per_iter));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    group.bench_function("http11_concurrent_1000", |b| {
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let url = url.clone();
            async move {
                let remaining =
                    Arc::new(std::sync::atomic::AtomicI64::new(requests_per_iter as i64));
                let mut workers = Vec::with_capacity(32);
                for _ in 0..32 {
                    let client = client.clone();
                    let url = url.clone();
                    let remaining = Arc::clone(&remaining);
                    workers.push(tokio::spawn(async move {
                        while remaining.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) > 0 {
                            let _ = client
                                .get(&url)
                                .send()
                                .await
                                .unwrap()
                                .bytes()
                                .await
                                .unwrap();
                        }
                    }));
                }
                for w in workers {
                    w.await.unwrap();
                }
            }
        });
    });

    group.finish();
}

fn bench_bridge_dispatch_adaptive(c: &mut Criterion) {
    use vertebra_http::batcher::BatchLimit;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let addr = rt.block_on(setup_bridge_server(BatchLimit { max: 128 }));

    let requests_per_iter: u64 = 1000;
    let url = format!("http://{}/hello", addr);
    let client = rt.block_on(async {
        let c = reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .no_proxy()
            .http1_only()
            .build()
            .unwrap();
        let mut warm = Vec::with_capacity(32);
        for _ in 0..32 {
            let c = c.clone();
            let u = url.clone();
            warm.push(tokio::spawn(async move {
                let _ = c.get(&u).send().await.unwrap().bytes().await.unwrap();
            }));
        }
        for h in warm {
            h.await.unwrap();
        }
        c
    });

    let mut group = c.benchmark_group("bridge_dispatch_adaptive");
    group.throughput(Throughput::Elements(requests_per_iter));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    group.bench_function("http11_concurrent_1000", |b| {
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let url = url.clone();
            async move {
                let remaining =
                    Arc::new(std::sync::atomic::AtomicI64::new(requests_per_iter as i64));
                let mut workers = Vec::with_capacity(32);
                for _ in 0..32 {
                    let client = client.clone();
                    let url = url.clone();
                    let remaining = Arc::clone(&remaining);
                    workers.push(tokio::spawn(async move {
                        while remaining.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) > 0 {
                            let _ = client
                                .get(&url)
                                .send()
                                .await
                                .unwrap()
                                .bytes()
                                .await
                                .unwrap();
                        }
                    }));
                }
                for w in workers {
                    w.await.unwrap();
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
