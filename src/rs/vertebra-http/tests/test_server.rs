//! Integration tests for the HTTP server.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use vertebra_http::{
    collector::{BatchCollector, PendingResponse},
    config::ServerConfig,
    dispatcher::Dispatcher,
    policy::DrainPolicy,
    route_table::RouteTable,
    router::NativeRouter,
    server,
};

/// Spawn a test server on a random port. Returns the bound address and a
/// shutdown sender.
async fn spawn_test_server() -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    table.insert("POST", "/echo");

    let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());
    let (dispatcher, mut ring_rx) = Dispatcher::new(64);
    let dispatcher = Arc::new(dispatcher);
    let (collector, collector_tx) = BatchCollector::new(
        Arc::clone(&dispatcher),
        DrainPolicy::default_fixed(),
        128,
    );

    tokio::spawn(collector.run());

    // Stub ring-reader: echo back with status 200 and body "hello".
    let dispatcher_clone = Arc::clone(&dispatcher);
    tokio::spawn(async move {
        while let Some(batch) = ring_rx.recv().await {
            for id in batch.correlation_ids {
                dispatcher_clone.complete(
                    id,
                    PendingResponse {
                        status: 200,
                        headers_mp: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"hello"),
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
    // (Tiny race window, acceptable for tests.)
    drop(listener);

    let config = Arc::new({
        let mut c = ServerConfig::default();
        c.bind = addr;
        c
    });

    tokio::spawn(async move {
        server::run_with_collector(
            config,
            router,
            dispatcher,
            collector_tx,
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    // Brief yield to allow the server to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, shutdown_tx)
}

#[tokio::test]
async fn test_http11_hello() {
    let (addr, _shutdown) = spawn_test_server().await;
    let resp = reqwest::get(format!("http://{}/hello", addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_http11_not_found() {
    let (addr, _shutdown) = spawn_test_server().await;
    let resp = reqwest::get(format!("http://{}/nonexistent", addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_server_accepts_multiple_requests() {
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
