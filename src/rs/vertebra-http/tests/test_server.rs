//! Integration tests for the HTTP server.
//!
//! Uses the real `BatchBridge` path. A background task simulates a Python
//! worker by polling the bridge for requests and submitting responses.
//!
//! Tests are serialized via a static mutex because they share the global
//! `BatchBridge` singleton.

use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use vertebra_core::bridge::{BatchBridge, BridgeResponse};
use vertebra_http::{
    backpressure::BackpressureConfig,
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

fn http_client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

struct TestServer {
    addr: std::net::SocketAddr,
    running: Arc<AtomicBool>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    collector_handle: JoinHandle<()>,
    worker_handle: JoinHandle<()>,
    resp_handle: JoinHandle<()>,
    server_handle: JoinHandle<()>,
}

impl TestServer {
    async fn shutdown(mut self) {
        self.running.store(false, Ordering::Release);
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.collector_handle.abort();
        self.resp_handle.abort();
        let _ = self.collector_handle.await;
        let _ = self.resp_handle.await;
        let _ = self.worker_handle.await;
        let _ = self.server_handle.await;
    }
}

/// Spawn a test server on a random port.
async fn spawn_test_server() -> TestServer {
    spawn_test_server_with(None).await
}

/// Spawn a test server with an optional backpressure config.
async fn spawn_test_server_with(bp: Option<BackpressureConfig>) -> TestServer {
    ensure_bridge();
    drain_stale();

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    table.insert("POST", "/echo");

    let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());
    let dispatcher = match bp {
        Some(cfg) => Arc::new(Dispatcher::new().with_backpressure(cfg)),
        None => Arc::new(Dispatcher::new()),
    };
    let (collector, collector_tx) =
        BatchCollector::new(Arc::clone(&dispatcher), DrainPolicy::default_fixed(), 128);

    let collector_handle = tokio::spawn(collector.run());
    let running = Arc::new(AtomicBool::new(true));
    let worker_running = Arc::clone(&running);

    // Simulated Python worker: polls bridge for requests while active, returns responses.
    let worker_handle = tokio::spawn(async move {
        while worker_running.load(Ordering::Acquire) {
            let snapshot = tokio::task::spawn_blocking(|| {
                BatchBridge::get().and_then(|b| b.poll_batch(Duration::from_millis(5)))
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
    let resp_handle = tokio::spawn(async move {
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

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Bind to port 0 and pass the bound listener directly to avoid re-bind races.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = Arc::new(ServerConfig {
        bind: addr,
        ..ServerConfig::default()
    });

    let server_handle = tokio::spawn(async move {
        server::run_with_listener(
            listener,
            config,
            router,
            dispatcher,
            vec![collector_tx],
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    TestServer {
        addr,
        running,
        shutdown_tx: Some(shutdown_tx),
        collector_handle,
        worker_handle,
        resp_handle,
        server_handle,
    }
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_http11_hello() {
    let _guard = TEST_LOCK.lock().unwrap();
    let srv = spawn_test_server().await;
    let client = http_client();
    let resp = client
        .get(format!("http://{}/hello", srv.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    srv.shutdown().await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_http11_not_found() {
    let _guard = TEST_LOCK.lock().unwrap();
    let srv = spawn_test_server().await;
    let client = http_client();
    let resp = client
        .get(format!("http://{}/nonexistent", srv.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    srv.shutdown().await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_server_accepts_multiple_requests() {
    let _guard = TEST_LOCK.lock().unwrap();
    let srv = spawn_test_server().await;
    let client = http_client();
    for _ in 0..10 {
        let resp = client
            .get(format!("http://{}/hello", srv.addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
    srv.shutdown().await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_health_endpoint_reports_snapshot() {
    let _guard = TEST_LOCK.lock().unwrap();
    let bp = BackpressureConfig {
        max_in_flight: 4,
        max_queue_depth: 4,
        max_queue_time: Duration::from_millis(25),
        ..BackpressureConfig::default()
    };
    let srv = spawn_test_server_with(Some(bp)).await;
    let client = http_client();

    // Drive one successful request so the accepted counter advances.
    let _ = client
        .get(format!("http://{}/hello", srv.addr))
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("http://{}/__vertebra/health", srv.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.starts_with("application/json"), "content-type: {ct}");

    let body = resp.text().await.unwrap();
    assert!(body.contains("\"status\":\"healthy\""), "body: {body}");
    assert!(body.contains("\"backpressure\""), "body: {body}");
    assert!(
        body.contains("\"circuit_state\":\"closed\""),
        "body: {body}"
    );
    srv.shutdown().await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_health_endpoint_works_without_backpressure() {
    let _guard = TEST_LOCK.lock().unwrap();
    let srv = spawn_test_server().await;
    let client = http_client();
    let resp = client
        .get(format!("http://{}/__vertebra/health", srv.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("\"status\":\"healthy\""), "body: {body}");
    srv.shutdown().await;
}
