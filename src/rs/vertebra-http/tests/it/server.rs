//! Integration tests for the HTTP server.
//!
//! Uses the real `BatchBridge` path. A background task simulates a Python
//! worker by polling the bridge for requests and submitting responses.
//!
//! Tests are serialized via a static mutex because they share the global
//! `BatchBridge` singleton.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::BRIDGE_TEST_LOCK as TEST_LOCK;
use vertebra_core::bridge::{BatchBridge, BridgeResponse};
use vertebra_http::{
    backpressure::BackpressureConfig,
    batcher::{BatchLimit, RequestBatcher},
    body::PendingResponse,
    config::ServerConfig,
    dispatcher::Dispatcher,
    routing::{RouteTable, Router},
    server,
};

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

    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let dispatcher = match bp {
        Some(cfg) => Arc::new(Dispatcher::new().with_backpressure(cfg)),
        None => Arc::new(Dispatcher::new()),
    };
    let (collector, collector_tx) =
        RequestBatcher::new(Arc::clone(&dispatcher), BatchLimit::default(), 128);

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
                        headers_raw: bytes::Bytes::new(),
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
                        headers_mp: resp.headers_raw,
                        body: resp.body,
                        stream_rx: None,
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

#[cfg(unix)]
#[tokio::test]
async fn test_bind_listener_so_reuseport_allows_dual_bind() {
    use std::os::fd::AsRawFd;

    fn get_int_sockopt(fd: i32, optname: i32) -> i32 {
        let mut val: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                optname,
                (&raw mut val).cast::<libc::c_void>(),
                &raw mut len,
            )
        };
        assert_eq!(rc, 0, "getsockopt failed");
        val
    }

    fn kernel_nodename() -> String {
        let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::uname(&raw mut uts) }, 0);
        unsafe { std::ffi::CStr::from_ptr(uts.nodename.as_ptr()) }
            .to_string_lossy()
            .into_owned()
    }

    let single =
        server::bind_listener("127.0.0.1:0".parse().unwrap(), false).expect("non-reuseport bind");
    assert_eq!(
        get_int_sockopt(single.as_raw_fd(), libc::SO_REUSEPORT),
        0,
        "reuse_port=false must leave SO_REUSEPORT disabled"
    );
    assert_ne!(
        get_int_sockopt(single.as_raw_fd(), libc::SO_REUSEADDR),
        0,
        "SO_REUSEADDR must be enabled"
    );
    drop(single);

    let l1 = server::bind_listener("127.0.0.1:0".parse().unwrap(), true)
        .expect("first SO_REUSEPORT bind");
    let addr = l1.local_addr().unwrap();
    assert_ne!(
        get_int_sockopt(l1.as_raw_fd(), libc::SO_REUSEPORT),
        0,
        "reuse_port=true must set SO_REUSEPORT on listener socket"
    );
    match server::bind_listener(addr, true) {
        Ok(l2) => {
            assert_eq!(l1.local_addr().unwrap(), addr);
            assert_eq!(l2.local_addr().unwrap(), addr);
            assert!(
                server::bind_listener(addr, false).is_err(),
                "non-SO_REUSEPORT bind must fail while SO_REUSEPORT listeners hold the port"
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
            let nodename = kernel_nodename();
            assert_eq!(
                nodename, "sandbox",
                "dual SO_REUSEPORT listen failed on native kernel ({nodename}): {err}"
            );
        }
        Err(err) => panic!("unexpected second SO_REUSEPORT bind error: {err}"),
    }

    let udp_single =
        server::bind_udp_listener("127.0.0.1:0".parse().unwrap(), false).expect("udp bind");
    assert_eq!(
        get_int_sockopt(udp_single.as_raw_fd(), libc::SO_REUSEPORT),
        0,
        "udp reuse_port=false must leave SO_REUSEPORT disabled"
    );
    assert_ne!(
        get_int_sockopt(udp_single.as_raw_fd(), libc::SO_REUSEADDR),
        0,
        "udp SO_REUSEADDR must be enabled"
    );
    drop(udp_single);

    let udp1 =
        server::bind_udp_listener("127.0.0.1:0".parse().unwrap(), true).expect("udp reuseport");
    let udp_addr = udp1.local_addr().unwrap();
    assert_ne!(
        get_int_sockopt(udp1.as_raw_fd(), libc::SO_REUSEPORT),
        0,
        "udp reuse_port=true must set SO_REUSEPORT"
    );
    let _udp2 = server::bind_udp_listener(udp_addr, true).expect("dual udp SO_REUSEPORT bind");
    if server::bind_udp_listener(udp_addr, false).is_ok() {
        let nodename = kernel_nodename();
        assert_eq!(
            nodename, "sandbox",
            "non-SO_REUSEPORT UDP bind must fail on native kernel ({nodename})"
        );
    }
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_server_executes_rust_cors_compression_and_static_mounts() {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use vertebra_http::compression::{CompressionBackend, CompressionOffloadConfig};
    use vertebra_http::config::StaticMountConfig;
    use vertebra_http::cors::CorsOffloadConfig;

    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale();

    let static_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        static_dir.path().join("app.js"),
        b"console.log('vertebra-static-fastpath');",
    )
    .unwrap();

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let dispatcher = Arc::new(Dispatcher::new());
    let (collector, collector_tx) =
        RequestBatcher::new(Arc::clone(&dispatcher), BatchLimit::default(), 128);
    let collector_handle = tokio::spawn(collector.run());

    let running = Arc::new(AtomicBool::new(true));
    let worker_running = Arc::clone(&running);
    let worker_hits = Arc::new(AtomicUsize::new(0));
    let worker_hits_inner = Arc::clone(&worker_hits);

    let payload_text = "vertebra-compressed-response-payload-".repeat(20);
    let expected_payload = payload_text.clone();

    let worker_handle = tokio::spawn(async move {
        while worker_running.load(Ordering::Acquire) {
            let snapshot = tokio::task::spawn_blocking(|| {
                BatchBridge::get().and_then(|b| b.poll_batch(Duration::from_millis(5)))
            })
            .await
            .unwrap();

            if let Some(batch) = snapshot {
                worker_hits_inner.fetch_add(batch.len(), Ordering::AcqRel);
                let headers_raw = vertebra_http::response::encode_response_headers(&[(
                    b"content-type".to_vec(),
                    b"text/plain".to_vec(),
                )]);
                let responses: Vec<BridgeResponse> = (0..batch.len())
                    .map(|i| BridgeResponse {
                        request_id: batch.node(i).correlation_id,
                        status: 200,
                        headers_raw: headers_raw.clone(),
                        body: bytes::Bytes::from(payload_text.clone().into_bytes()),
                    })
                    .collect();
                if let Some(bridge) = BatchBridge::get() {
                    let _ = bridge.submit_responses_async(responses).await;
                }
            }
        }
    });

    let dispatcher_for_resp = Arc::clone(&dispatcher);
    let resp_handle = tokio::spawn(async move {
        let bridge = BatchBridge::get().expect("bridge not initialized");
        while let Some(resps) = bridge.recv_responses().await {
            for resp in resps {
                dispatcher_for_resp.complete(
                    resp.request_id,
                    PendingResponse {
                        status: resp.status,
                        headers_mp: resp.headers_raw,
                        body: resp.body,
                        stream_rx: None,
                    },
                );
            }
        }
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let listener = server::bind_listener("127.0.0.1:0".parse().unwrap(), false).unwrap();
    let addr = listener.local_addr().unwrap();

    let mut extra_headers = HashMap::new();
    extra_headers.insert("x-static-mount".to_string(), "rust".to_string());

    let config = Arc::new(ServerConfig {
        bind: addr,
        cors: Some(CorsOffloadConfig {
            allow_origins: vec!["https://app.example".to_string()],
            allow_methods: vec!["GET".to_string(), "POST".to_string()],
            allow_headers: vec!["content-type".to_string()],
            allow_credentials: true,
            expose_headers: vec!["x-exposed".to_string()],
            max_age: 600,
            allow_origin_regex: None,
        }),
        compression: Some(CompressionOffloadConfig {
            backend: CompressionBackend::Zstd,
            minimum_size: 128,
            ..CompressionOffloadConfig::default()
        }),
        static_mounts: vec![StaticMountConfig {
            path_prefix: "/static".to_string(),
            directories: vec![static_dir.path().to_str().unwrap().to_string()],
            html_mode: false,
            send_as_attachment: false,
            headers: extra_headers,
        }],
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

    let srv = TestServer {
        addr,
        running,
        shutdown_tx: Some(shutdown_tx),
        collector_handle,
        worker_handle,
        resp_handle,
        server_handle,
    };

    let client = http_client();

    // 1. CORS OPTIONS preflight handled purely in Rust without touching worker.
    let preflight = client
        .request(reqwest::Method::OPTIONS, format!("http://{addr}/hello"))
        .header("Origin", "https://app.example")
        .header("Access-Control-Request-Method", "POST")
        .header("Access-Control-Request-Headers", "content-type")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 204);
    assert_eq!(
        preflight
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("https://app.example")
    );
    assert_eq!(worker_hits.load(Ordering::Acquire), 0);

    // 2. Static asset GET + HEAD handled purely in Rust without touching worker.
    let asset_resp = client
        .get(format!("http://{addr}/static/app.js"))
        .header("Origin", "https://app.example")
        .send()
        .await
        .unwrap();
    assert_eq!(asset_resp.status(), 200);
    assert_eq!(
        asset_resp
            .headers()
            .get("x-static-mount")
            .and_then(|v| v.to_str().ok()),
        Some("rust")
    );
    assert_eq!(
        asset_resp
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("https://app.example")
    );
    assert_eq!(
        asset_resp.bytes().await.unwrap().as_ref(),
        b"console.log('vertebra-static-fastpath');"
    );

    let head_resp = client
        .head(format!("http://{addr}/static/app.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(head_resp.status(), 200);
    assert!(head_resp.bytes().await.unwrap().is_empty());
    assert_eq!(worker_hits.load(Ordering::Acquire), 0);

    // 3. Worker response compressed with Zstd + CORS headers applied in Rust.
    let compressed_resp = client
        .get(format!("http://{addr}/hello"))
        .header("Origin", "https://app.example")
        .header("Accept-Encoding", "zstd")
        .send()
        .await
        .unwrap();
    assert_eq!(compressed_resp.status(), 200);
    assert_eq!(
        compressed_resp
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok()),
        Some("zstd")
    );
    assert_eq!(
        compressed_resp
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("https://app.example")
    );
    let raw_compressed = compressed_resp.bytes().await.unwrap();
    let decompressed = zstd::stream::decode_all(raw_compressed.as_ref()).unwrap();
    assert_eq!(decompressed, expected_payload.as_bytes());
    assert_eq!(worker_hits.load(Ordering::Acquire), 1);

    srv.shutdown().await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_server_enforces_payload_limits_profile_discovery_and_access_log() {
    use std::sync::atomic::AtomicUsize;
    use vertebra_http::access_log::AccessLogFormat;
    use vertebra_http::payload::PayloadLimits;
    use vertebra_http::profile::ProfileConfig;

    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale();

    let mut table = RouteTable::new();
    table.insert("POST", "/echo");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let dispatcher = Arc::new(Dispatcher::new());
    let (collector, collector_tx) =
        RequestBatcher::new(Arc::clone(&dispatcher), BatchLimit::default(), 128);
    let collector_handle = tokio::spawn(collector.run());

    let running = Arc::new(AtomicBool::new(true));
    let worker_running = Arc::clone(&running);
    let worker_hits = Arc::new(AtomicUsize::new(0));
    let worker_hits_inner = Arc::clone(&worker_hits);

    let worker_handle = tokio::spawn(async move {
        while worker_running.load(Ordering::Acquire) {
            let snapshot = tokio::task::spawn_blocking(|| {
                BatchBridge::get().and_then(|b| b.poll_batch(Duration::from_millis(5)))
            })
            .await
            .unwrap();

            if let Some(batch) = snapshot {
                worker_hits_inner.fetch_add(batch.len(), Ordering::AcqRel);
                let responses: Vec<BridgeResponse> = (0..batch.len())
                    .map(|i| BridgeResponse {
                        request_id: batch.node(i).correlation_id,
                        status: 200,
                        headers_raw: bytes::Bytes::new(),
                        body: batch.body_bytes_owned(i),
                    })
                    .collect();
                if let Some(bridge) = BatchBridge::get() {
                    let _ = bridge.submit_responses_async(responses).await;
                }
            }
        }
    });

    let dispatcher_for_resp = Arc::clone(&dispatcher);
    let resp_handle = tokio::spawn(async move {
        let bridge = BatchBridge::get().expect("bridge not initialized");
        while let Some(resps) = bridge.recv_responses().await {
            for resp in resps {
                dispatcher_for_resp.complete(
                    resp.request_id,
                    PendingResponse {
                        status: resp.status,
                        headers_mp: resp.headers_raw,
                        body: resp.body,
                        stream_rx: None,
                    },
                );
            }
        }
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let listener = server::bind_listener("127.0.0.1:0".parse().unwrap(), false).unwrap();
    let addr = listener.local_addr().unwrap();

    let config = Arc::new(ServerConfig {
        bind: addr,
        payload_limits: PayloadLimits {
            max_body_size: 256,
            max_decompressed_size: 512,
        },
        profile: ProfileConfig {
            enabled: true,
            bearer_token: Some("secret-prof-token".to_string()),
        },
        access_log: Some(AccessLogFormat::Json),
        discovery: std::collections::HashMap::from([
            (
                "/.well-known/agent-card.json".to_string(),
                r#"{"name":"vertebra-agent","version":"1.0"}"#.to_string(),
            ),
            (
                "/.well-known/mcp.json".to_string(),
                r#"{"mcpVersion":"2026-07-28"}"#.to_string(),
            ),
        ]),
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

    let srv = TestServer {
        addr,
        running,
        shutdown_tx: Some(shutdown_tx),
        collector_handle,
        worker_handle,
        resp_handle,
        server_handle,
    };

    let client = http_client();

    let prof_unauth = client
        .get(format!("http://{addr}/debug/pprof/profile"))
        .send()
        .await
        .unwrap();
    assert_eq!(prof_unauth.status(), 401);

    let prof_ok = client
        .get(format!("http://{addr}/debug/pprof/profile"))
        .header("Authorization", "Bearer secret-prof-token")
        .send()
        .await
        .unwrap();
    assert_eq!(prof_ok.status(), 200);
    let prof_json = prof_ok.text().await.unwrap();
    assert!(prof_json.contains("\"profile\":\"cpu\""));

    let card_resp = client
        .get(format!("http://{addr}/.well-known/agent-card.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(card_resp.status(), 200);
    let etag = card_resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();
    let card_304 = client
        .get(format!("http://{addr}/.well-known/agent-card.json"))
        .header("If-None-Match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(card_304.status(), 304);

    let too_large = client
        .post(format!("http://{addr}/echo"))
        .body(vec![b'a'; 300])
        .send()
        .await
        .unwrap();
    assert_eq!(too_large.status(), 413);
    assert_eq!(worker_hits.load(Ordering::Acquire), 0);

    let chunks: Vec<Result<hyper::body::Frame<bytes::Bytes>, std::io::Error>> = vec![
        Ok(hyper::body::Frame::data(bytes::Bytes::from(vec![
            b'x';
            150
        ]))),
        Ok(hyper::body::Frame::data(bytes::Bytes::from(vec![
            b'y';
            150
        ]))),
    ];
    let stream_too_large = client
        .post(format!("http://{addr}/echo"))
        .body(reqwest::Body::wrap(http_body_util::StreamBody::new(
            futures_util::stream::iter(chunks),
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(stream_too_large.status(), 413);
    assert_eq!(worker_hits.load(Ordering::Acquire), 0);

    let valid_echo = client
        .post(format!("http://{addr}/echo"))
        .body(vec![b'z'; 128])
        .send()
        .await
        .unwrap();
    assert_eq!(valid_echo.status(), 200);
    assert_eq!(valid_echo.bytes().await.unwrap().len(), 128);
    assert_eq!(worker_hits.load(Ordering::Acquire), 1);

    srv.shutdown().await;
}

#[cfg(not(feature = "http3"))]
#[tokio::test]
async fn test_run_with_listener_fails_loud_without_http3_feature() {
    use vertebra_http::config::TlsConfig;
    use vertebra_http::http3::Http3Config;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = Arc::new(ServerConfig {
        bind: listener.local_addr().unwrap(),
        tls: Some(TlsConfig::default()),
        http3: Http3Config {
            enabled: true,
            ..Default::default()
        },
        ..ServerConfig::default()
    });
    let mut table = RouteTable::new();
    table.insert("POST", "/echo");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        server::run_with_listener(
            listener,
            cfg,
            router,
            Arc::new(Dispatcher::new()),
            vec![tx],
            std::future::pending::<()>(),
        ),
    )
    .await
    .expect("validate_http3 must return without waiting for shutdown");
    let err = res.expect_err("http3.enabled without http3 feature must fail");
    assert!(
        err.to_string().contains("http3"),
        "expected error mentioning http3 feature, got: {err}"
    );
}

#[tokio::test]
async fn test_handle_buffered_worker_dispatch_and_payload_limits() {
    use bytes::Bytes;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use http::{Request, StatusCode};
    use std::io::Write as _;
    use vertebra_http::batcher::PendingRequest;
    use vertebra_http::http3::Http3Config;
    use vertebra_http::payload::PayloadLimits;
    use vertebra_http::response::handle_buffered;
    use vertebra_http::server::ServerState;

    let mut table = RouteTable::new();
    table.insert("POST", "/echo");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (collector_tx, mut collector_rx) = tokio::sync::mpsc::channel::<PendingRequest>(16);
    tokio::spawn(async move {
        while let Some(pending) = collector_rx.recv().await {
            let mut out = b"h3-worker:".to_vec();
            out.extend_from_slice(&pending.body);
            let _ = pending
                .reply
                .send(PendingResponse::unary(200, Bytes::new(), Bytes::from(out)));
        }
    });

    let cfg = Arc::new(ServerConfig {
        payload_limits: PayloadLimits {
            max_body_size: 32,
            max_decompressed_size: 16,
        },
        http3: Http3Config {
            enabled: true,
            ..Default::default()
        },
        ..ServerConfig::default()
    });
    let state = Arc::new(ServerState::new(
        router,
        Arc::new(Dispatcher::new()),
        cfg,
        vec![collector_tx],
    ));

    let valid_req = Request::builder()
        .method("POST")
        .uri("/echo")
        .body(Bytes::from_static(b"ok"))
        .unwrap();
    let valid_resp = handle_buffered(valid_req, Arc::clone(&state))
        .await
        .unwrap();
    assert_eq!(valid_resp.status(), StatusCode::OK);
    assert!(
        valid_resp.headers().get("alt-svc").is_none(),
        "alt-svc must not be emitted on 200 worker responses without bound http3_port"
    );
    let body = http_body_util::BodyExt::collect(valid_resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body.as_ref(), b"h3-worker:ok");

    let cl_overflow = Request::builder()
        .method("POST")
        .uri("/echo")
        .header("content-length", "999")
        .body(Bytes::from_static(b"hi"))
        .unwrap();
    assert_eq!(
        handle_buffered(cl_overflow, Arc::clone(&state))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    let body_overflow = Request::builder()
        .method("POST")
        .uri("/echo")
        .body(Bytes::from(vec![b'a'; 33]))
        .unwrap();
    let overflow_resp = handle_buffered(body_overflow, Arc::clone(&state))
        .await
        .unwrap();
    assert_eq!(overflow_resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(overflow_resp.headers().get("alt-svc").is_none());

    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(b"zip").unwrap();
    let gz_bytes = gz.finish().unwrap();
    let gz_req = Request::builder()
        .method("POST")
        .uri("/echo")
        .header("content-encoding", "gzip")
        .body(Bytes::from(gz_bytes))
        .unwrap();
    let gz_resp = handle_buffered(gz_req, Arc::clone(&state)).await.unwrap();
    assert_eq!(gz_resp.status(), StatusCode::OK);
    let gz_out = http_body_util::BodyExt::collect(gz_resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(gz_out.as_ref(), b"h3-worker:zip");

    let zstd_bytes = zstd::stream::encode_all(std::io::Cursor::new(b"zst"), 3).unwrap();
    let zstd_req = Request::builder()
        .method("POST")
        .uri("/echo")
        .header("content-encoding", "zstd")
        .body(Bytes::from(zstd_bytes))
        .unwrap();
    let zstd_resp = handle_buffered(zstd_req, Arc::clone(&state)).await.unwrap();
    assert_eq!(zstd_resp.status(), StatusCode::OK);
    let zstd_out = http_body_util::BodyExt::collect(zstd_resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(zstd_out.as_ref(), b"h3-worker:zst");

    let bomb_bytes = zstd::stream::encode_all(std::io::Cursor::new(&[b'x'; 64][..]), 3).unwrap();
    assert!(bomb_bytes.len() <= 32);
    let bomb_req = Request::builder()
        .method("POST")
        .uri("/echo")
        .header("content-encoding", "zstd")
        .body(Bytes::from(bomb_bytes))
        .unwrap();
    assert_eq!(
        handle_buffered(bomb_req, Arc::clone(&state))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}
