use std::convert::Infallible;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::{Either, Full};
use hyper::body::Frame;
use tempfile::NamedTempFile;
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc, oneshot, watch};

use vertebra_http::batcher::PendingRequest;
use vertebra_http::body::{FrameReceiver, PendingResponse};
use vertebra_http::config::{ServerConfig, TlsConfig};
use vertebra_http::dispatcher::Dispatcher;
use vertebra_http::http3::{
    Http3Config, build_quic_server_config, format_alt_svc_header, inject_alt_svc_header,
    serve_http3_endpoint, strip_hop_by_hop_headers,
};
use vertebra_http::payload::PayloadLimits;
use vertebra_http::routing::{RouteTable, Router};
use vertebra_http::server;

async fn connect_h3_client(
    addr: SocketAddr,
    cert_der: &[u8],
) -> (
    quinn::Endpoint,
    h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    tokio::task::JoinHandle<()>,
) {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(cert_der.to_vec()))
        .unwrap();
    let mut client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_tls.alpn_protocols = vec![b"h3".to_vec()];

    let quic_client_cfg = quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).unwrap();
    let mut client_endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client_endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic_client_cfg)));

    let conn = client_endpoint
        .connect(addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut driver, send_req) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .unwrap();
    let driver_task = tokio::spawn(async move {
        let _ = futures_util::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });
    (client_endpoint, send_req, driver_task)
}

#[test]
fn test_alt_svc_and_hop_by_hop_header_helpers() {
    assert_eq!(
        format_alt_svc_header(8443, 86_400),
        "h3=\":8443\"; ma=86400"
    );

    let mut headers = HeaderMap::new();
    inject_alt_svc_header(&mut headers, 8443, 86_400);
    assert_eq!(
        headers.get("alt-svc").unwrap(),
        &HeaderValue::from_static("h3=\":8443\"; ma=86400")
    );
    inject_alt_svc_header(&mut headers, 9443, 60);
    assert_eq!(
        headers.get("alt-svc").unwrap(),
        &HeaderValue::from_static("h3=\":8443\"; ma=86400"),
        "inject_alt_svc_header must preserve an existing alt-svc value"
    );

    headers.insert("connection", HeaderValue::from_static("keep-alive"));
    headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
    headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
    headers.insert("x-custom", HeaderValue::from_static("kept"));
    strip_hop_by_hop_headers(&mut headers);
    assert!(headers.get("connection").is_none());
    assert!(headers.get("keep-alive").is_none());
    assert!(headers.get("transfer-encoding").is_none());
    assert_eq!(headers.get("x-custom").unwrap(), "kept");
}

#[tokio::test]
async fn test_http3_quic_unary_and_streaming_roundtrip() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = cert_params.self_signed(&key_pair).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(cert.der().to_vec());
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der()),
    );

    let tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .unwrap();

    let h3_cfg = Http3Config {
        enabled: true,
        alt_svc_max_age: 86_400,
        max_concurrent_bidi_streams: 128,
        keep_alive_interval_ms: 5_000,
    };

    let quic_server_config = build_quic_server_config(tls_config, &h3_cfg).unwrap();
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let endpoint = quinn::Endpoint::server(quic_server_config, bind_addr).unwrap();
    let local_addr = endpoint.local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let handler = Arc::new(|req: Request<Bytes>| async move {
        if req.uri().path() == "/stream" {
            let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(8);
            tokio::spawn(async move {
                let _ = tx
                    .send(Ok(Frame::data(Bytes::from_static(b"chunk-1|"))))
                    .await;
                let _ = tx
                    .send(Ok(Frame::data(Bytes::from_static(b"chunk-2"))))
                    .await;
                let mut trailers = HeaderMap::new();
                trailers.insert("x-stream-status", HeaderValue::from_static("complete"));
                let _ = tx.send(Ok(Frame::trailers(trailers))).await;
            });
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain")
                .body(Either::Right(FrameReceiver::new(rx)))
                .unwrap()
        } else {
            let mut body = b"echo:".to_vec();
            body.extend_from_slice(req.body());
            Response::builder()
                .status(StatusCode::OK)
                .header("x-protocol", "h3")
                .header("connection", "keep-alive")
                .body(Either::Left(Full::new(Bytes::from(body))))
                .unwrap()
        }
    });

    let server_task = tokio::spawn(async move {
        serve_http3_endpoint(
            endpoint,
            local_addr.port(),
            h3_cfg,
            16,
            handler,
            shutdown_rx,
        )
        .await
        .unwrap();
    });

    let (client_endpoint, mut send_req, driver_task) =
        connect_h3_client(local_addr, cert_der.as_ref()).await;

    let unary_req = Request::builder()
        .method("POST")
        .uri("https://localhost/echo")
        .body(())
        .unwrap();
    let mut unary_stream = send_req.send_request(unary_req).await.unwrap();
    unary_stream
        .send_data(Bytes::from_static(b"hello-quic"))
        .await
        .unwrap();
    unary_stream.finish().await.unwrap();

    let unary_resp = unary_stream.recv_response().await.unwrap();
    assert_eq!(unary_resp.status(), StatusCode::OK);
    assert_eq!(unary_resp.headers().get("x-protocol").unwrap(), "h3");
    assert!(unary_resp.headers().get("alt-svc").is_some());
    assert!(
        unary_resp.headers().get("connection").is_none(),
        "hop-by-hop connection header must be stripped on HTTP/3"
    );

    let mut received = Vec::new();
    while let Some(mut chunk) = unary_stream.recv_data().await.unwrap() {
        use bytes::Buf as _;
        received.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    assert_eq!(received, b"echo:hello-quic");

    let stream_req = Request::builder()
        .method("GET")
        .uri("https://localhost/stream")
        .body(())
        .unwrap();
    let mut h3_stream = send_req.send_request(stream_req).await.unwrap();
    h3_stream.finish().await.unwrap();

    let stream_resp = h3_stream.recv_response().await.unwrap();
    assert_eq!(stream_resp.status(), StatusCode::OK);
    let mut streamed_bytes = Vec::new();
    while let Some(mut chunk) = h3_stream.recv_data().await.unwrap() {
        use bytes::Buf as _;
        streamed_bytes.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    assert_eq!(streamed_bytes, b"chunk-1|chunk-2");
    let trailers = h3_stream
        .recv_trailers()
        .await
        .unwrap()
        .expect("trailers expected");
    assert_eq!(trailers.get("x-stream-status").unwrap(), "complete");

    let oversized_cl_req = Request::builder()
        .method("POST")
        .uri("https://localhost/echo")
        .header("content-length", "128")
        .body(())
        .unwrap();
    let mut oversized_cl_stream = send_req.send_request(oversized_cl_req).await.unwrap();
    let oversized_cl_resp = oversized_cl_stream.recv_response().await.unwrap();
    assert_eq!(oversized_cl_resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let streamed_oversized_req = Request::builder()
        .method("POST")
        .uri("https://localhost/echo")
        .body(())
        .unwrap();
    let mut streamed_oversized = send_req.send_request(streamed_oversized_req).await.unwrap();
    let _ = streamed_oversized
        .send_data(Bytes::from(vec![b'z'; 32]))
        .await;
    let _ = streamed_oversized.finish().await;
    let streamed_oversized_resp = streamed_oversized.recv_response().await.unwrap();
    assert_eq!(
        streamed_oversized_resp.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    drop(send_req);
    client_endpoint.close(0u32.into(), b"done");
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("serve_http3_endpoint must drain within timeout");
    let _ = driver_task.await;
}

fn generate_test_tls_files() -> (NamedTempFile, NamedTempFile, Vec<u8>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = cert_params.self_signed(&key_pair).unwrap();

    let mut cert_file = NamedTempFile::new().unwrap();
    cert_file.write_all(cert.pem().as_bytes()).unwrap();
    cert_file.flush().unwrap();

    let mut key_file = NamedTempFile::new().unwrap();
    key_file
        .write_all(key_pair.serialize_pem().as_bytes())
        .unwrap();
    key_file.flush().unwrap();

    (cert_file, key_file, cert.der().to_vec())
}

fn spawn_echo_and_stream_worker() -> mpsc::Sender<PendingRequest> {
    let (tx, mut rx) = mpsc::channel::<PendingRequest>(32);
    tokio::spawn(async move {
        while let Some(pending) = rx.recv().await {
            let path = pending.path_and_query.path();
            if path == "/stream" {
                let (frame_tx, frame_rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(8);
                tokio::spawn(async move {
                    let _ = frame_tx
                        .send(Ok(Frame::data(Bytes::from_static(b"stream-1|"))))
                        .await;
                    let _ = frame_tx
                        .send(Ok(Frame::data(Bytes::from_static(b"stream-2"))))
                        .await;
                    let mut trailers = HeaderMap::new();
                    trailers.insert("x-stream-done", HeaderValue::from_static("yes"));
                    let _ = frame_tx.send(Ok(Frame::trailers(trailers))).await;
                });
                let _ = pending
                    .reply
                    .send(PendingResponse::streaming(200, Bytes::new(), frame_rx));
            } else {
                let mut out = b"h3-worker:".to_vec();
                out.extend_from_slice(&pending.body);
                let _ =
                    pending
                        .reply
                        .send(PendingResponse::unary(200, Bytes::new(), Bytes::from(out)));
            }
        }
    });
    tx
}

#[tokio::test]
async fn test_run_with_listener_serves_live_http3_and_advertises_alt_svc() {
    let (cert_file, key_file, cert_der) = generate_test_tls_files();

    let mut table = RouteTable::new();
    table.insert("POST", "/echo");
    table.insert("GET", "/stream");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let dispatcher = Arc::new(Dispatcher::new());
    let collector_tx = spawn_echo_and_stream_worker();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = Arc::new(ServerConfig {
        bind: addr,
        payload_limits: PayloadLimits {
            max_body_size: 64,
            max_decompressed_size: 128,
        },
        tls: Some(TlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
            ..Default::default()
        }),
        http3: Http3Config {
            enabled: true,
            alt_svc_max_age: 7200,
            max_concurrent_bidi_streams: 64,
            keep_alive_interval_ms: 5_000,
        },
        ..ServerConfig::default()
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
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

    let reqwest_cert = reqwest::Certificate::from_der(&cert_der).unwrap();
    let https_client = reqwest::Client::builder()
        .add_root_certificate(reqwest_cert)
        .resolve("localhost", addr)
        .no_proxy()
        .build()
        .unwrap();

    let tcp_resp = https_client
        .post(format!("https://localhost:{}/echo", addr.port()))
        .body("via-tcp")
        .send()
        .await
        .unwrap();
    assert_eq!(tcp_resp.status(), reqwest::StatusCode::OK);
    let expected_alt_svc = format!("h3=\":{}\"; ma=7200", addr.port());
    assert_eq!(
        tcp_resp.headers().get("alt-svc").unwrap().to_str().unwrap(),
        expected_alt_svc
    );
    assert_eq!(
        tcp_resp.bytes().await.unwrap().as_ref(),
        b"h3-worker:via-tcp"
    );

    let (client_endpoint, mut send_req, driver_task) = connect_h3_client(addr, &cert_der).await;

    let unary_req = Request::builder()
        .method("POST")
        .uri("https://localhost/echo")
        .body(())
        .unwrap();
    let mut unary_stream = send_req.send_request(unary_req).await.unwrap();
    unary_stream
        .send_data(Bytes::from_static(b"via-h3"))
        .await
        .unwrap();
    unary_stream.finish().await.unwrap();

    let unary_resp = unary_stream.recv_response().await.unwrap();
    assert_eq!(unary_resp.status(), StatusCode::OK);
    assert_eq!(
        unary_resp.headers().get("alt-svc").unwrap(),
        expected_alt_svc.as_str()
    );
    let mut received = Vec::new();
    while let Some(mut chunk) = unary_stream.recv_data().await.unwrap() {
        use bytes::Buf as _;
        received.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    assert_eq!(received, b"h3-worker:via-h3");

    let stream_req = Request::builder()
        .method("GET")
        .uri("https://localhost/stream")
        .body(())
        .unwrap();
    let mut h3_stream = send_req.send_request(stream_req).await.unwrap();
    h3_stream.finish().await.unwrap();

    let stream_resp = h3_stream.recv_response().await.unwrap();
    assert_eq!(stream_resp.status(), StatusCode::OK);
    let mut streamed = Vec::new();
    while let Some(mut chunk) = h3_stream.recv_data().await.unwrap() {
        use bytes::Buf as _;
        streamed.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    assert_eq!(streamed, b"stream-1|stream-2");
    let trailers = h3_stream
        .recv_trailers()
        .await
        .unwrap()
        .expect("stream trailers expected");
    assert_eq!(trailers.get("x-stream-done").unwrap(), "yes");

    let sse_req = Request::builder()
        .method("GET")
        .uri("https://localhost/__vertebra/sse/news")
        .body(())
        .unwrap();
    let sse_stream = send_req.send_request(sse_req).await.unwrap();
    let (mut sse_stream, ()) = (sse_stream, ());
    sse_stream.finish().await.unwrap();
    let sse_resp = sse_stream.recv_response().await.unwrap();
    assert_eq!(sse_resp.status(), StatusCode::OK);
    assert!(
        sse_resp.headers().get("connection").is_none(),
        "SSE HTTP/3 response must strip hop-by-hop connection header"
    );
    drop(sse_stream);

    drop(send_req);
    client_endpoint.close(0u32.into(), b"done");
    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(5), server_handle)
        .await
        .expect("server must drain cleanly on shutdown")
        .unwrap();
    let _ = driver_task.await;
}

#[tokio::test]
async fn test_http3_shutdown_drains_inflight_request() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (cert_file, key_file, cert_der) = generate_test_tls_files();

    let mut table = RouteTable::new();
    table.insert("GET", "/drain");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let dispatcher = Arc::new(Dispatcher::new());

    let (collector_tx, mut collector_rx) = mpsc::channel::<PendingRequest>(8);
    let started_tx = Arc::new(Notify::new());
    let release_tx = Arc::new(Notify::new());
    let started_worker = Arc::clone(&started_tx);
    let release_worker = Arc::clone(&release_tx);
    tokio::spawn(async move {
        while let Some(pending) = collector_rx.recv().await {
            started_worker.notify_one();
            release_worker.notified().await;
            let _ = pending.reply.send(PendingResponse::unary(
                200,
                Bytes::new(),
                Bytes::from_static(b"drained-ok"),
            ));
        }
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Arc::new(ServerConfig {
        bind: addr,
        tls: Some(TlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
            ..Default::default()
        }),
        http3: Http3Config {
            enabled: true,
            ..Default::default()
        },
        ..ServerConfig::default()
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
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

    let (client_endpoint, mut send_req, driver_task) = connect_h3_client(addr, &cert_der).await;
    let req = Request::builder()
        .method("GET")
        .uri("https://localhost/drain")
        .body(())
        .unwrap();
    let mut stream = send_req.send_request(req).await.unwrap();
    stream.finish().await.unwrap();

    started_tx.notified().await;
    let _ = shutdown_tx.send(());
    release_tx.notify_one();

    let resp = tokio::time::timeout(Duration::from_secs(5), stream.recv_response())
        .await
        .expect("in-flight HTTP/3 response timed out during shutdown")
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await.unwrap() {
        use bytes::Buf as _;
        body.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    assert_eq!(body, b"drained-ok");

    drop(send_req);
    client_endpoint.close(0u32.into(), b"done");
    tokio::time::timeout(Duration::from_secs(5), server_handle)
        .await
        .expect("server must finish draining after in-flight HTTP/3 request")
        .unwrap();
    let _ = driver_task.await;
}

#[tokio::test]
async fn test_run_with_listener_omits_alt_svc_when_http3_disabled_and_fails_loud_without_tls() {
    let (cert_file, key_file, cert_der) = generate_test_tls_files();

    let mut table = RouteTable::new();
    table.insert("POST", "/echo");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let dispatcher = Arc::new(Dispatcher::new());
    let collector_tx = spawn_echo_and_stream_worker();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let disabled_cfg = Arc::new(ServerConfig {
        bind: addr,
        tls: Some(TlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
            ..Default::default()
        }),
        http3: Http3Config {
            enabled: false,
            ..Default::default()
        },
        ..ServerConfig::default()
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        server::run_with_listener(
            listener,
            disabled_cfg,
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

    let reqwest_cert = reqwest::Certificate::from_der(&cert_der).unwrap();
    let https_client = reqwest::Client::builder()
        .add_root_certificate(reqwest_cert)
        .resolve("localhost", addr)
        .no_proxy()
        .build()
        .unwrap();

    let tcp_resp = https_client
        .post(format!("https://localhost:{}/echo", addr.port()))
        .body("no-h3")
        .send()
        .await
        .unwrap();
    assert_eq!(tcp_resp.status(), reqwest::StatusCode::OK);
    assert!(
        tcp_resp.headers().get("alt-svc").is_none(),
        "alt-svc must be omitted when http3.enabled is false"
    );
    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(5), server_handle)
        .await
        .expect("server must shut down within timeout")
        .unwrap();

    let fail_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let invalid_cfg = Arc::new(ServerConfig {
        bind: fail_listener.local_addr().unwrap(),
        tls: None,
        http3: Http3Config {
            enabled: true,
            ..Default::default()
        },
        ..ServerConfig::default()
    });
    let mut table = RouteTable::new();
    table.insert("POST", "/echo");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (tx, _rx) = mpsc::channel(4);
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        server::run_with_listener(
            fail_listener,
            invalid_cfg,
            router,
            Arc::new(Dispatcher::new()),
            vec![tx],
            std::future::pending::<()>(),
        ),
    )
    .await
    .expect("invalid HTTP/3 config without TLS must fail immediately");
    let err = res.expect_err("http3.enabled=true with tls=None must fail loud");
    assert!(
        err.to_string().contains("TLS"),
        "expected error mentioning TLS, got: {err}"
    );
}
