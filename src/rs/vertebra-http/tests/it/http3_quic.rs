#![cfg(feature = "http3")]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::{Either, Full};
use hyper::body::Frame;
use tokio::sync::{mpsc, watch};

use vertebra_http::body::FrameReceiver;
use vertebra_http::http3::{
    Http3Config, build_quic_server_config, format_alt_svc_header, inject_alt_svc_header,
    serve_http3_endpoint,
};

#[test]
fn test_inject_alt_svc_header_formats_port_and_max_age() {
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
                .body(Either::Left(Full::new(Bytes::from(body))))
                .unwrap()
        }
    });

    let server_task = tokio::spawn(async move {
        serve_http3_endpoint(endpoint, local_addr.port(), h3_cfg, handler, shutdown_rx)
            .await
            .unwrap();
    });

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der).unwrap();
    let mut client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_tls.alpn_protocols = vec![b"h3".to_vec()];

    let quic_client_cfg = quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).unwrap();
    let mut client_endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client_endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic_client_cfg)));

    let conn = client_endpoint
        .connect(local_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut driver, mut send_req) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = futures_util::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

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

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}
