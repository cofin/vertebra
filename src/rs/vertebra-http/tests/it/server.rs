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
        // SAFETY: `fd` is an open socket FD from `AsRawFd`, `val` is a stack `c_int`, and
        // `len` is initialized to `size_of::<libc::c_int>()` as both input and output size.
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
        // SAFETY: `libc::utsname` is a plain C byte-array struct where all-zero is valid.
        let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
        // SAFETY: `&raw mut uts` points to an aligned, mutable stack `libc::utsname`.
        assert_eq!(unsafe { libc::uname(&raw mut uts) }, 0);
        // SAFETY: `libc::uname` returned `0`, so `uts.nodename` holds a NUL-terminated C string.
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
        .get(format!("http://{addr}/debug/pprof/profile?duration_ms=25"))
        .header("Authorization", "Bearer secret-prof-token")
        .send()
        .await
        .unwrap();
    #[cfg(not(all(unix, feature = "profiling")))]
    {
        assert_eq!(prof_ok.status(), 404);
        let prof_json = prof_ok.text().await.unwrap();
        assert_eq!(prof_json, "{\"error\":\"profiling feature disabled\"}");
    }
    #[cfg(all(unix, feature = "profiling"))]
    {
        assert_eq!(prof_ok.status(), 200);
        assert!(!prof_ok.bytes().await.unwrap().is_empty());
    }

    let prof_heap = client
        .get(format!("http://{addr}/debug/pprof/heap"))
        .header("Authorization", "Bearer secret-prof-token")
        .send()
        .await
        .unwrap();
    assert_eq!(prof_heap.status(), 404);

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
    let (dispatcher, _direct_rx) = Dispatcher::new_direct(16);
    let state = Arc::new(ServerState::new(
        router,
        Arc::new(dispatcher),
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

#[tokio::test]
async fn test_graceful_drain_finishes_in_flight_and_terminates_stalled_socket() {
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut table = RouteTable::new();
    table.insert("GET", "/slow");
    table.insert("GET", "/hello");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (dispatcher, _direct_rx) = Dispatcher::new_direct(16);
    let dispatcher = Arc::new(dispatcher);
    let (collector_tx, mut collector_rx) =
        tokio::sync::mpsc::channel::<vertebra_http::batcher::PendingRequest>(16);

    let (slow_started_tx, slow_started_rx) = oneshot::channel::<()>();
    let mut slow_started_tx = Some(slow_started_tx);
    let worker_handle = tokio::spawn(async move {
        while let Some(pending) = collector_rx.recv().await {
            if pending.path_and_query.path() == "/slow" {
                if let Some(tx) = slow_started_tx.take() {
                    let _ = tx.send(());
                }
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    let _ = pending.reply.send(PendingResponse::unary(
                        200,
                        Bytes::new(),
                        Bytes::from_static(b"slow-done"),
                    ));
                });
            } else {
                let _ = pending.reply.send(PendingResponse::unary(
                    200,
                    Bytes::new(),
                    Bytes::from_static(b"hello"),
                ));
            }
        }
    });

    let listener = server::bind_listener("127.0.0.1:0".parse().unwrap(), false).unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let config = Arc::new(ServerConfig {
        bind: addr,
        drain_timeout: Duration::from_millis(250),
        header_read_timeout: Duration::from_secs(10),
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

    let client = http_client();
    let slow_req_task = tokio::spawn(async move {
        let resp = client
            .get(format!("http://{addr}/slow"))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap();
        (status, body)
    });

    slow_started_rx.await.unwrap();

    let mut stalled = TcpStream::connect(addr).await.unwrap();
    stalled
        .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();

    shutdown_tx.send(()).unwrap();

    let (slow_status, slow_body) = slow_req_task.await.unwrap();
    assert_eq!(slow_status, 200);
    assert_eq!(slow_body, "slow-done");

    let mut buf = [0u8; 64];
    let stalled_read = tokio::time::timeout(Duration::from_secs(2), stalled.read(&mut buf))
        .await
        .expect("stalled socket must terminate when drain_timeout expires");
    match stalled_read {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("expected EOF or reset on stalled socket, read {n} bytes"),
    }

    tokio::time::timeout(Duration::from_secs(2), server_handle)
        .await
        .expect("server must finish after drain_timeout")
        .unwrap();
    worker_handle.abort();
}

#[tokio::test]
async fn test_max_connections_caps_concurrent_connections_until_client_closes() {
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (dispatcher, _direct_rx) = Dispatcher::new_direct(16);
    let dispatcher = Arc::new(dispatcher);
    let (collector_tx, mut collector_rx) =
        tokio::sync::mpsc::channel::<vertebra_http::batcher::PendingRequest>(16);

    let worker_handle = tokio::spawn(async move {
        while let Some(pending) = collector_rx.recv().await {
            let _ = pending.reply.send(PendingResponse::unary(
                200,
                Bytes::new(),
                Bytes::from_static(b"hello"),
            ));
        }
    });

    let listener = server::bind_listener("127.0.0.1:0".parse().unwrap(), false).unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let config = Arc::new(ServerConfig {
        bind: addr,
        max_connections: 2,
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

    async fn send_keepalive_hello(stream: &mut TcpStream) {
        stream
            .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 256];
        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "unexpected EOF waiting for keep-alive response");
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(5).any(|w| w == b"hello") {
                break;
            }
        }
        assert!(String::from_utf8_lossy(&buf).contains("200 OK"));
    }

    let mut c1 = TcpStream::connect(addr).await.unwrap();
    send_keepalive_hello(&mut c1).await;

    let mut c2 = TcpStream::connect(addr).await.unwrap();
    send_keepalive_hello(&mut c2).await;

    let mut c3 = TcpStream::connect(addr).await.unwrap();
    c3.write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();

    let mut c3_buf = Vec::new();
    let blocked =
        tokio::time::timeout(Duration::from_millis(120), c3.read_to_end(&mut c3_buf)).await;
    assert!(
        blocked.is_err(),
        "3rd connection must wait while 2 max_connections permits are held"
    );

    drop(c1);

    tokio::time::timeout(Duration::from_secs(2), c3.read_to_end(&mut c3_buf))
        .await
        .expect("3rd connection should proceed once 1st closes")
        .unwrap();
    let c3_resp = String::from_utf8_lossy(&c3_buf);
    assert!(c3_resp.contains("200 OK"));
    assert!(c3_resp.ends_with("hello"));

    drop(c2);
    drop(c3);
    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), server_handle)
        .await
        .unwrap()
        .unwrap();
    worker_handle.abort();
}

#[tokio::test]
async fn test_header_read_timeout_closes_slowloris_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (dispatcher, _direct_rx) = Dispatcher::new_direct(16);
    let dispatcher = Arc::new(dispatcher);
    let (collector_tx, _collector_rx) = tokio::sync::mpsc::channel(4);

    let listener = server::bind_listener("127.0.0.1:0".parse().unwrap(), false).unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let config = Arc::new(ServerConfig {
        bind: addr,
        header_read_timeout: Duration::from_millis(120),
        keepalive_timeout: Duration::from_secs(10),
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

    let mut slowloris = TcpStream::connect(addr).await.unwrap();
    slowloris
        .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();

    let mut buf = [0u8; 64];
    let res = tokio::time::timeout(Duration::from_secs(2), slowloris.read(&mut buf))
        .await
        .expect("slowloris connection must close at header_read_timeout");
    match res {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("expected connection close, got {n} bytes"),
    }

    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), server_handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_h2c_prior_knowledge_serves_cleartext_http2() {
    use bytes::Bytes;

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    table.insert("POST", "/echo");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (dispatcher, _direct_rx) = Dispatcher::new_direct(16);
    let dispatcher = Arc::new(dispatcher);
    let (collector_tx, mut collector_rx) =
        tokio::sync::mpsc::channel::<vertebra_http::batcher::PendingRequest>(16);

    let worker_handle = tokio::spawn(async move {
        while let Some(pending) = collector_rx.recv().await {
            let body = if pending.path_and_query.path() == "/echo" {
                pending.body
            } else {
                Bytes::from_static(b"h2c-hello")
            };
            let _ = pending
                .reply
                .send(PendingResponse::unary(200, Bytes::new(), body));
        }
    });

    let listener = server::bind_listener("127.0.0.1:0".parse().unwrap(), false).unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let config = Arc::new(ServerConfig {
        bind: addr,
        enable_h2c_prior_knowledge: true,
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

    let client = reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .build()
        .unwrap();

    let resp = client
        .get(format!("http://{addr}/hello"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    assert_eq!(resp.text().await.unwrap(), "h2c-hello");

    let echo = client
        .post(format!("http://{addr}/echo"))
        .body("prior-knowledge-payload")
        .send()
        .await
        .unwrap();
    assert_eq!(echo.status(), 200);
    assert_eq!(echo.version(), reqwest::Version::HTTP_2);
    assert_eq!(echo.text().await.unwrap(), "prior-knowledge-payload");

    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), server_handle)
        .await
        .unwrap()
        .unwrap();
    worker_handle.abort();
}

#[tokio::test]
async fn test_handle_with_peer_forwards_client_ip_to_access_logger() {
    use bytes::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use std::io::Write;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use vertebra_http::access_log::{AccessLogFormat, AccessLogger};
    use vertebra_http::response::handle_with_peer;
    use vertebra_http::server::ServerState;

    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (collector_tx, mut collector_rx) =
        tokio::sync::mpsc::channel::<vertebra_http::batcher::PendingRequest>(4);

    tokio::spawn(async move {
        while let Some(pending) = collector_rx.recv().await {
            let _ = pending.reply.send(PendingResponse::unary(
                200,
                Bytes::new(),
                Bytes::from_static(b"ok"),
            ));
        }
    });

    let captured = Arc::new(Mutex::new(Vec::new()));
    let logger = Arc::new(AccessLogger::with_writer(
        AccessLogFormat::Json,
        SharedWriter(Arc::clone(&captured)),
    ));

    let (dispatcher, _direct_rx) = Dispatcher::new_direct(4);
    let mut state = ServerState::new(
        router,
        Arc::new(dispatcher),
        Arc::new(ServerConfig::default()),
        vec![collector_tx],
    );
    state.access_logger = Some(Arc::clone(&logger));
    let state = Arc::new(state);

    let (mut client_io, server_io) = tokio::io::duplex(4096);
    let peer: SocketAddr = "203.0.113.42:54321".parse().unwrap();
    let srv_state = Arc::clone(&state);
    let conn_task = tokio::spawn(async move {
        http1::Builder::new()
            .serve_connection(
                TokioIo::new(server_io),
                service_fn(move |req| {
                    let s = Arc::clone(&srv_state);
                    async move { handle_with_peer(req, peer, s).await }
                }),
            )
            .await
            .unwrap();
    });

    client_io
        .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut raw = Vec::new();
    client_io.read_to_end(&mut raw).await.unwrap();
    assert!(String::from_utf8_lossy(&raw).contains("200 OK"));
    conn_task.await.unwrap();

    assert!(logger.flush(Duration::from_secs(2)));
    let log_str = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    assert!(
        log_str.contains("\"remote_addr\":\"203.0.113.42\""),
        "expected peer IP in access log, got: {log_str}"
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_batch_bridge_publish_pubsub_delivers_sse_event() {
    let _guard = TEST_LOCK.lock().unwrap();
    let srv = spawn_test_server().await;
    let client = http_client();

    let mut resp = client
        .get(format!("http://{}/__vertebra/sse/alerts", srv.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let bridge = BatchBridge::get().expect("bridge initialized");
    let event_id = bridge
        .publish_pubsub("alerts", b"{\"status\":\"ok\"}", Some("alert-1"))
        .expect("pubsub event id");
    assert_eq!(event_id, 1);
    assert_eq!(
        bridge.publish_pubsub("alerts", b"{\"status\":\"ok\"}", Some("alert-1")),
        None
    );

    let chunk = tokio::time::timeout(Duration::from_secs(2), resp.chunk())
        .await
        .expect("timed out waiting for SSE chunk")
        .unwrap()
        .expect("non-empty SSE chunk");
    let text = String::from_utf8_lossy(&chunk);
    assert!(text.contains("id: 1"), "missing id in SSE frame: {text}");
    assert!(
        text.contains("event: alerts"),
        "missing event in SSE frame: {text}"
    );
    assert!(
        text.contains("data: {\"status\":\"ok\"}"),
        "missing data in SSE frame: {text}"
    );

    drop(resp);
    srv.shutdown().await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_server_enforces_multipart_limits_and_boundary_before_worker_dispatch() {
    use std::sync::atomic::AtomicUsize;
    use vertebra_http::multipart::MultipartLimits;

    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale();

    let mut table = RouteTable::new();
    table.insert("POST", "/upload");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let dispatcher = Arc::new(Dispatcher::new());
    let (collector, collector_tx) =
        RequestBatcher::new(Arc::clone(&dispatcher), BatchLimit::default(), 64);
    let collector_handle = tokio::spawn(collector.run());

    let running = Arc::new(AtomicBool::new(true));
    let worker_running = Arc::clone(&running);
    let worker_hits = Arc::new(AtomicUsize::new(0));
    let worker_hits_bg = Arc::clone(&worker_hits);

    let worker_handle = tokio::spawn(async move {
        while worker_running.load(Ordering::Acquire) {
            let snapshot = tokio::task::spawn_blocking(|| {
                BatchBridge::get().and_then(|b| b.poll_batch(Duration::from_millis(5)))
            })
            .await
            .unwrap();

            if let Some(batch) = snapshot {
                worker_hits_bg.fetch_add(batch.len(), Ordering::AcqRel);
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

    let resp_dispatcher = Arc::clone(&dispatcher);
    let resp_handle = tokio::spawn(async move {
        let bridge = BatchBridge::get().expect("bridge not initialized");
        while let Some(resps) = bridge.recv_responses().await {
            for resp in resps {
                resp_dispatcher.complete(
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
        multipart_limits: MultipartLimits {
            max_fields: 1,
            max_file_size: 8,
            max_total_size: 256,
        },
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
    let url = format!("http://{addr}/upload");

    let missing_boundary = client
        .post(&url)
        .header("content-type", "multipart/form-data")
        .body("not-multipart")
        .send()
        .await
        .unwrap();
    assert_eq!(missing_boundary.status(), 400);
    assert_eq!(worker_hits.load(Ordering::Acquire), 0);

    let boundary = "----VertebraBoundaryTest";
    let malformed = client
        .post(&url)
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body("corrupt-stream-without-closing-boundary")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
    assert_eq!(worker_hits.load(Ordering::Acquire), 0);

    let too_many_fields = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"a\"\r\n\r\n\
         1\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"b\"\r\n\r\n\
         2\r\n\
         --{boundary}--\r\n"
    );
    let fields_resp = client
        .post(&url)
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(too_many_fields)
        .send()
        .await
        .unwrap();
    assert_eq!(fields_resp.status(), 413);
    assert_eq!(worker_hits.load(Ordering::Acquire), 0);

    let file_too_large = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"doc\"; filename=\"f.bin\"\r\n\
         Content-Type: application/octet-stream\r\n\r\n\
         0123456789abcdef\r\n\
         --{boundary}--\r\n"
    );
    let file_resp = client
        .post(&url)
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(file_too_large)
        .send()
        .await
        .unwrap();
    assert_eq!(file_resp.status(), 413);
    assert_eq!(worker_hits.load(Ordering::Acquire), 0);

    let valid_body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"doc\"; filename=\"ok.bin\"\r\n\
         Content-Type: application/octet-stream\r\n\r\n\
         ok1234\r\n\
         --{boundary}--\r\n"
    );
    let ok_resp = client
        .post(&url)
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(valid_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), 200);
    assert_eq!(ok_resp.text().await.unwrap(), valid_body);
    assert_eq!(worker_hits.load(Ordering::Acquire), 1);

    srv.shutdown().await;
}
