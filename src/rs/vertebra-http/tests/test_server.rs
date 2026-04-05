//! Integration tests for the HTTP server.
//!
//! Uses the real `BatchBridge` path. A background task simulates a Python
//! worker by polling the bridge for requests and submitting responses.
//!
//! Tests are serialized via a static mutex because they share the global
//! `BatchBridge` singleton.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::oneshot;

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

/// Serialize server tests to prevent cross-test bridge interference.
static TEST_LOCK: std::sync::LazyLock<StdMutex<()>> =
    std::sync::LazyLock::new(|| StdMutex::new(()));

/// Ensure the global bridge is initialized (idempotent).
fn ensure_bridge() {
    let _ = BatchBridge::init(256, 1_048_576, 4, 1);
}

/// Drain any stale batches from the bridge.
fn drain_stale() {
    if let Some(bridge) = BatchBridge::get() {
        while bridge.poll_batch(Duration::from_millis(1)).is_some() {}
        // Response channel is tokio::sync::mpsc — drain via try_recv
        while bridge.try_recv_responses().is_some() {}
    }
}

/// Spawn a test server on a random port. Returns the bound address and a
/// shutdown sender.
async fn spawn_test_server() -> (std::net::SocketAddr, oneshot::Sender<()>) {
    ensure_bridge();
    drain_stale();

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    table.insert("POST", "/echo");

    let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());
    let dispatcher = Arc::new(Dispatcher::new());
    let (collector, collector_tx) =
        BatchCollector::new(Arc::clone(&dispatcher), DrainPolicy::default_fixed(), 128);

    tokio::spawn(collector.run());

    // Simulated Python worker: polls bridge for requests, returns responses.
    tokio::spawn(async move {
        loop {
            let snapshot = tokio::task::spawn_blocking(|| {
                BatchBridge::get().and_then(|b| b.poll_batch(Duration::from_millis(50)))
            })
            .await
            .unwrap();

            if let Some(batch) = snapshot {
                let responses: Vec<BridgeResponse> = (0..batch.len())
                    .map(|i| BridgeResponse {
                        request_id: batch.node(i).correlation_id,
                        status: 200,
                        headers: Vec::new(),
                        body: b"hello".to_vec().into(),
                    })
                    .collect();

                if let Some(bridge) = BatchBridge::get() {
                    let _ = bridge.submit_responses_async(responses).await;
                }
            }
        }
    });

    // Response reader: async recv from bridge, completes dispatcher futures.
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
                        body: bytes::Bytes::from(resp.body),
                    },
                );
            }
        }
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Bind to port 0; pass the discovered address back.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Drop the listener so `run_with_collector` can rebind it.
    drop(listener);

    let config = Arc::new(ServerConfig {
        bind: addr,
        ..ServerConfig::default()
    });

    tokio::spawn(async move {
        server::run_with_collector(config, router, dispatcher, vec![collector_tx], async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    // Brief yield to allow the server to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, shutdown_tx)
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_http11_hello() {
    let _guard = TEST_LOCK.lock().unwrap();
    let (addr, _shutdown) = spawn_test_server().await;
    let resp = reqwest::get(format!("http://{}/hello", addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_http11_not_found() {
    let _guard = TEST_LOCK.lock().unwrap();
    let (addr, _shutdown) = spawn_test_server().await;
    let resp = reqwest::get(format!("http://{}/nonexistent", addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_server_accepts_multiple_requests() {
    let _guard = TEST_LOCK.lock().unwrap();
    let (addr, _shutdown) = spawn_test_server().await;
    let client = reqwest::Client::new();
    for _ in 0..10 {
        let resp = client
            .get(format!("http://{}/hello", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}
