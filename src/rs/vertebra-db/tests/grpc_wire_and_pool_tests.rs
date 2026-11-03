use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming};
use hyper::server::conn::http2 as server_http2;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use vertebra_db::grpc::{
    DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE, GrpcChannelPool, GrpcCode, GrpcErrorDetail,
    GrpcPoolConfig, GrpcRequestMetadata, GrpcStatus, LpmFrameDecoder, ProtoReader, ProtoWriter,
    WireType, encode_lpm_frame, format_grpc_timeout, generate_request_uuid_v4,
};

struct ChannelBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.rx.poll_recv(cx)
    }
}

#[test]
fn proto_writer_and_reader_roundtrip_all_wire_types() {
    let mut writer = ProtoWriter::new();
    writer.write_varint(1, 9_876_543_210_u64);
    writer.write_i64(2, -42_000_000_000_i64);
    writer.write_bool(3, true);
    writer.write_sint32(4, -123_456);
    writer.write_sint64(5, -9_876_543_210_123_i64);
    writer.write_fixed32(6, 0xdead_beef);
    writer.write_fixed64(7, 0x0123_4567_89ab_cdef);
    writer.write_f32(8, 3.25_f32);
    writer.write_f64(9, -2718.281828_f64);
    writer.write_str(10, "projects/p1/instances/i1/databases/d1");
    writer.write_bytes(11, &[0x00, 0xff, 0x10, 0x20]);
    writer.write_nested(12, |nested| {
        nested.write_u32(1, 777);
        nested.write_str(2, "nested-value");
    });
    writer.write_packed_i64(13, &[-100, 0, 100, 999_999_999]);
    writer.write_packed_f64(14, &[1.5, -2.5, 42.125]);

    // Unknown fields for wire types 0 (Varint), 1 (Fixed64), 2 (LengthDelimited), and 5 (Fixed32)
    writer.write_varint(90, 12345);
    writer.write_fixed64(91, 0x1122_3344_5566_7788);
    writer.write_bytes(92, b"unknown-bytes");
    writer.write_fixed32(93, 0x99aa_bbcc);
    writer.write_str(15, "tail-marker");

    let encoded = writer.finish();
    let mut reader = ProtoReader::new(&encoded);

    let mut f1 = None;
    let mut f2 = None;
    let mut f3 = None;
    let mut f4 = None;
    let mut f5 = None;
    let mut f6 = None;
    let mut f7 = None;
    let mut f8 = None;
    let mut f9 = None;
    let mut f10 = None;
    let mut f11 = None;
    let mut f12 = None;
    let mut f13 = None;
    let mut f14 = None;
    let mut f15 = None;

    while let Some((field, wire_type)) = reader.next_tag().expect("valid tag") {
        match (field, wire_type) {
            (1, WireType::Varint) => f1 = Some(reader.read_varint().expect("u64")),
            (2, WireType::Varint) => f2 = Some(reader.read_i64().expect("i64")),
            (3, WireType::Varint) => f3 = Some(reader.read_bool().expect("bool")),
            (4, WireType::Varint) => f4 = Some(reader.read_sint32().expect("sint32")),
            (5, WireType::Varint) => f5 = Some(reader.read_sint64().expect("sint64")),
            (6, WireType::Fixed32) => f6 = Some(reader.read_fixed32().expect("fixed32")),
            (7, WireType::Fixed64) => f7 = Some(reader.read_fixed64().expect("fixed64")),
            (8, WireType::Fixed32) => f8 = Some(reader.read_f32().expect("f32")),
            (9, WireType::Fixed64) => f9 = Some(reader.read_f64().expect("f64")),
            (10, WireType::LengthDelimited) => {
                f10 = Some(reader.read_str().expect("str").to_string());
            }
            (11, WireType::LengthDelimited) => {
                f11 = Some(reader.read_bytes().expect("bytes").to_vec());
            }
            (12, WireType::LengthDelimited) => {
                let mut sub = reader.read_nested().expect("nested");
                let mut n_id = 0;
                let mut n_val = String::new();
                while let Some((nf, nwt)) = sub.next_tag().expect("nested tag") {
                    match (nf, nwt) {
                        (1, WireType::Varint) => n_id = sub.read_u32().expect("nested u32"),
                        (2, WireType::LengthDelimited) => {
                            n_val = sub.read_str().expect("nested str").to_string();
                        }
                        _ => sub.skip_field(nwt).expect("nested skip"),
                    }
                }
                f12 = Some((n_id, n_val));
            }
            (13, WireType::LengthDelimited) => {
                f13 = Some(reader.read_packed_i64().expect("packed i64"));
            }
            (14, WireType::LengthDelimited) => {
                f14 = Some(reader.read_packed_f64().expect("packed f64"));
            }
            (15, WireType::LengthDelimited) => {
                f15 = Some(reader.read_str().expect("tail str").to_string());
            }
            (_, wt) => {
                reader.skip_field(wt).expect("skip unknown field");
            }
        }
    }

    assert_eq!(f1, Some(9_876_543_210));
    assert_eq!(f2, Some(-42_000_000_000));
    assert_eq!(f3, Some(true));
    assert_eq!(f4, Some(-123_456));
    assert_eq!(f5, Some(-9_876_543_210_123));
    assert_eq!(f6, Some(0xdead_beef));
    assert_eq!(f7, Some(0x0123_4567_89ab_cdef));
    assert_eq!(f8, Some(3.25_f32));
    assert_eq!(f9, Some(-2718.281828_f64));
    assert_eq!(
        f10.as_deref(),
        Some("projects/p1/instances/i1/databases/d1")
    );
    assert_eq!(f11, Some(vec![0x00, 0xff, 0x10, 0x20]));
    assert_eq!(f12, Some((777, "nested-value".to_string())));
    assert_eq!(f13, Some(vec![-100, 0, 100, 999_999_999]));
    assert_eq!(f14, Some(vec![1.5, -2.5, 42.125]));
    assert_eq!(f15.as_deref(), Some("tail-marker"));
}

#[test]
fn request_uuid_v4_rfc4122_layout() {
    assert_eq!(DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE, 128 * 1024 * 1024);
    assert_eq!(format_grpc_timeout(Duration::from_millis(1500)), "1500m");

    let mut seen = std::collections::HashSet::new();
    for _ in 0..32 {
        let id = generate_request_uuid_v4();
        assert_eq!(id.len(), 36);
        let bytes = id.as_bytes();
        assert_eq!(bytes[8], b'-');
        assert_eq!(bytes[13], b'-');
        assert_eq!(bytes[18], b'-');
        assert_eq!(bytes[23], b'-');
        assert_eq!(bytes[14], b'4');
        assert!(matches!(bytes[19], b'8' | b'9' | b'a' | b'b'));
        for (idx, &ch) in bytes.iter().enumerate() {
            if matches!(idx, 8 | 13 | 18 | 23) {
                assert_eq!(ch, b'-');
            } else {
                assert!(
                    ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch),
                    "expected lowercase hex at {idx}, got {}",
                    ch as char
                );
            }
        }
        assert!(seen.insert(id), "UUIDv4 collision detected");
    }
}

async fn handle_test_grpc_request(
    req: Request<Incoming>,
) -> Result<Response<ChannelBody>, Infallible> {
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();

    match path.as_str() {
        "/test.v1.Service/UnaryEcho" => {
            let auth = headers
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let routing = headers
                .get("x-goog-request-params")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let quota = headers
                .get("x-goog-user-project")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let req_id = headers
                .get("x-goog-spanner-request-id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let timeout = headers
                .get("grpc-timeout")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();

            let body_bytes = req
                .into_body()
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            let mut dec = LpmFrameDecoder::new(DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE);
            dec.push_chunk(&body_bytes);
            let msg = dec.next_frame().ok().flatten().unwrap_or_default();
            let mut r = ProtoReader::new(&msg);
            let mut input_text = String::new();
            while let Some((f, wt)) = r.next_tag().unwrap_or(None) {
                if f == 1 && wt == WireType::LengthDelimited {
                    input_text = r.read_str().unwrap_or_default().to_string();
                } else {
                    let _ = r.skip_field(wt);
                }
            }

            let mut w = ProtoWriter::new();
            w.write_str(1, &input_text);
            w.write_str(2, &auth);
            w.write_str(3, &routing);
            w.write_str(4, &quota);
            w.write_str(5, &req_id);
            w.write_str(6, &timeout);
            let framed =
                encode_lpm_frame(&w.finish(), false, DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE).unwrap();

            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                let _ = tx.send(Ok(Frame::data(framed))).await;
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", HeaderValue::from_static("0"));
                trailers.insert(
                    "x-server-trailers",
                    HeaderValue::from_static("unary-ok-trailer"),
                );
                let _ = tx.send(Ok(Frame::trailers(trailers))).await;
            });

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/grpc+proto")
                .body(ChannelBody { rx })
                .unwrap())
        }
        "/test.v1.Service/ServerStreamLarge" => {
            let _ = req.into_body().collect().await;
            let six_mib_payload = vec![0x5a_u8; 6 * 1024 * 1024];
            let mut w1 = ProtoWriter::with_capacity(six_mib_payload.len() + 16);
            w1.write_bytes(1, &six_mib_payload);
            let frame1 =
                encode_lpm_frame(&w1.finish(), false, DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE)
                    .unwrap();

            let mut w2 = ProtoWriter::new();
            w2.write_str(1, &"gzip-stream-payload-".repeat(256));
            let frame2 =
                encode_lpm_frame(&w2.finish(), true, DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE).unwrap();

            let (tx, rx) = mpsc::channel(64);
            tokio::spawn(async move {
                // Split the 6 MiB LPM frame across 32 KiB chunks to exercise streaming reassembly
                for chunk in frame1.chunks(32 * 1024) {
                    if tx
                        .send(Ok(Frame::data(Bytes::copy_from_slice(chunk))))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = tx.send(Ok(Frame::data(frame2))).await;
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", HeaderValue::from_static("0"));
                let _ = tx.send(Ok(Frame::trailers(trailers))).await;
            });

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/grpc+proto")
                .header("grpc-encoding", "gzip")
                .body(ChannelBody { rx })
                .unwrap())
        }
        "/test.v1.Service/BidiChat" => {
            let mut incoming = req.into_body();
            let (tx, rx) = mpsc::channel(16);
            tokio::spawn(async move {
                let mut dec = LpmFrameDecoder::new(DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE);
                while let Some(Ok(frame)) = incoming.frame().await {
                    if let Ok(data) = frame.into_data() {
                        dec.push_chunk(&data);
                        while let Ok(Some(msg)) = dec.next_frame() {
                            let mut r = ProtoReader::new(&msg);
                            let mut val = 0u64;
                            while let Some((f, wt)) = r.next_tag().unwrap_or(None) {
                                if f == 1 && wt == WireType::Varint {
                                    val = r.read_varint().unwrap_or(0);
                                } else {
                                    let _ = r.skip_field(wt);
                                }
                            }
                            let mut w = ProtoWriter::new();
                            w.write_varint(1, val * 10);
                            let out_frame = encode_lpm_frame(
                                &w.finish(),
                                false,
                                DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE,
                            )
                            .unwrap();
                            if tx.send(Ok(Frame::data(out_frame))).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", HeaderValue::from_static("0"));
                let _ = tx.send(Ok(Frame::trailers(trailers))).await;
            });

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/grpc+proto")
                .body(ChannelBody { rx })
                .unwrap())
        }
        "/test.v1.Service/TrailersOnlyQuotaError" => {
            let _ = req.into_body().collect().await;
            let mut meta = HashMap::new();
            meta.insert("quota_limit".to_string(), "ReadRowsPerMinute".to_string());
            let details = vec![
                GrpcErrorDetail::RetryInfo {
                    retry_delay_ms: 250,
                },
                GrpcErrorDetail::ResourceInfo {
                    resource_type: "bigquery.googleapis.com/ReadSession".to_string(),
                    resource_name: "projects/p/locations/us/sessions/s1".to_string(),
                    owner: "project:p".to_string(),
                    description: "session quota exceeded".to_string(),
                },
                GrpcErrorDetail::ErrorInfo {
                    reason: "RATE_LIMIT_EXCEEDED".to_string(),
                    domain: "googleapis.com".to_string(),
                    metadata: meta,
                },
                GrpcErrorDetail::BadRequest {
                    field_violations: vec![(
                        "parent".to_string(),
                        "invalid project format".to_string(),
                    )],
                },
            ];
            let details_b64 = GrpcStatus::encode_status_details_bin(
                GrpcCode::ResourceExhausted,
                "Quota exceeded for ReadRows",
                &details,
            );
            let (_tx, rx) = mpsc::channel(1);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/grpc+proto")
                .header("grpc-status", "8")
                .header("grpc-message", "Quota%20exceeded%20for%20ReadRows")
                .header("grpc-status-details-bin", details_b64)
                .body(ChannelBody { rx })
                .unwrap())
        }
        "/test.v1.Service/MidStreamStatusError" => {
            let _ = req.into_body().collect().await;
            let mut w = ProtoWriter::new();
            w.write_str(1, "first-chunk-ok");
            let frame1 =
                encode_lpm_frame(&w.finish(), false, DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE).unwrap();
            let details = vec![GrpcErrorDetail::RetryInfo {
                retry_delay_ms: 250,
            }];
            let details_b64 = GrpcStatus::encode_status_details_bin(
                GrpcCode::ResourceExhausted,
                "Stream rate limited",
                &details,
            );

            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                let _ = tx.send(Ok(Frame::data(frame1))).await;
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", HeaderValue::from_static("8"));
                trailers.insert(
                    "grpc-message",
                    HeaderValue::from_static("Stream%20rate%20limited"),
                );
                trailers.insert(
                    "grpc-status-details-bin",
                    HeaderValue::from_str(&details_b64).unwrap(),
                );
                let _ = tx.send(Ok(Frame::trailers(trailers))).await;
            });

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/grpc+proto")
                .body(ChannelBody { rx })
                .unwrap())
        }
        "/test.v1.Service/OversizedFrame" => {
            let _ = req.into_body().collect().await;
            // Craft an LPM header advertising a 2 MiB frame while client limit is 64 KiB
            let mut raw = BytesMut::with_capacity(16);
            raw.extend_from_slice(&[0u8, 0x00, 0x20, 0x00, 0x00]);
            raw.extend_from_slice(b"partial");
            let (tx, rx) = mpsc::channel(2);
            tokio::spawn(async move {
                let _ = tx.send(Ok(Frame::data(raw.freeze()))).await;
            });
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/grpc+proto")
                .body(ChannelBody { rx })
                .unwrap())
        }
        _ => {
            let (_tx, rx) = mpsc::channel(1);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/grpc+proto")
                .header("grpc-status", "12")
                .header("grpc-message", "Unimplemented")
                .body(ChannelBody { rx })
                .unwrap())
        }
    }
}

#[tokio::test]
async fn grpc_h2c_unary_server_stream_bidi_and_large_6mib_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind h2c");
    let addr = listener.local_addr().expect("local_addr");

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut builder = server_http2::Builder::new(TokioExecutor::new());
                builder
                    .initial_stream_window_size(8 * 1024 * 1024)
                    .initial_connection_window_size(16 * 1024 * 1024);
                let _ = builder
                    .serve_connection(TokioIo::new(stream), service_fn(handle_test_grpc_request))
                    .await;
            });
        }
    });

    let mut cfg = GrpcPoolConfig::new(format!("http://{addr}"));
    cfg.pool_size = 2;
    let pool = GrpcChannelPool::new(cfg).expect("create h2c pool");

    let req_uuid = generate_request_uuid_v4();
    let meta = GrpcRequestMetadata {
        bearer_token: Some("test-access-token-1".to_string()),
        routing_params: Some(
            " session=projects/p1/instances/i1/databases/d1/sessions/s1 "
                .trim()
                .to_string(),
        ),
        quota_project: Some("quota-proj-123".to_string()),
        spanner_request_id: Some(format!("1.{req_uuid}.1.1.1")),
        timeout: Some(Duration::from_millis(1500)),
        compress_gzip: false,
        extra_headers: Vec::new(),
    };

    // 1. Unary call + metadata propagation
    let mut u_req = ProtoWriter::new();
    u_req.write_str(1, "hello-spanner");
    let (u_resp, resp_headers) = pool
        .unary("/test.v1.Service/UnaryEcho", u_req.finish(), &meta)
        .await
        .expect("unary call");
    assert_eq!(
        resp_headers
            .get("x-server-trailers")
            .and_then(|v| v.to_str().ok()),
        Some("unary-ok-trailer")
    );
    let mut r = ProtoReader::new(&u_resp);
    let mut fields = HashMap::new();
    while let Some((f, wt)) = r.next_tag().expect("tag") {
        if wt == WireType::LengthDelimited {
            fields.insert(f, r.read_str().expect("str").to_string());
        } else {
            r.skip_field(wt).expect("skip");
        }
    }
    assert_eq!(fields.get(&1).map(String::as_str), Some("hello-spanner"));
    assert_eq!(
        fields.get(&2).map(String::as_str),
        Some("Bearer test-access-token-1")
    );
    assert_eq!(
        fields.get(&3).map(String::as_str),
        Some("session=projects/p1/instances/i1/databases/d1/sessions/s1")
    );
    assert_eq!(fields.get(&4).map(String::as_str), Some("quota-proj-123"));
    assert_eq!(
        fields.get(&5).map(String::as_str),
        Some(format!("1.{req_uuid}.1.1.1").as_str())
    );
    assert_eq!(fields.get(&6).map(String::as_str), Some("1500m"));

    // 2. Server-streaming call with a 6 MiB uncompressed frame (> 4 MiB default limit) + gzip frame
    let mut stream = pool
        .server_streaming("/test.v1.Service/ServerStreamLarge", Bytes::new(), &meta)
        .await
        .expect("server_streaming");
    let msg1 = stream
        .next_message()
        .await
        .expect("stream frame 1")
        .expect("some frame 1");
    let mut r1 = ProtoReader::new(&msg1);
    assert_eq!(
        r1.next_tag().expect("tag"),
        Some((1, WireType::LengthDelimited))
    );
    let large_slice = r1.read_bytes().expect("6 MiB slice");
    assert_eq!(large_slice.len(), 6 * 1024 * 1024);
    assert!(large_slice.iter().all(|&b| b == 0x5a));

    let msg2 = stream
        .next_message()
        .await
        .expect("stream frame 2")
        .expect("some frame 2");
    let mut r2 = ProtoReader::new(&msg2);
    assert_eq!(
        r2.next_tag().expect("tag"),
        Some((1, WireType::LengthDelimited))
    );
    assert_eq!(
        r2.read_str().expect("gzip decoded str"),
        "gzip-stream-payload-".repeat(256)
    );
    assert_eq!(stream.next_message().await.expect("eof"), None);

    // 3. Bidi-streaming call
    let (tx, rx) = mpsc::channel(8);
    for val in [3_u64, 7_u64, 21_u64] {
        let mut bw = ProtoWriter::new();
        bw.write_varint(1, val);
        tx.send(bw.finish()).await.expect("send bidi req");
    }
    drop(tx);

    let mut bidi_stream = pool
        .bidi_streaming("/test.v1.Service/BidiChat", rx, &meta)
        .await
        .expect("bidi_streaming");
    let mut received = Vec::new();
    while let Some(frame) = bidi_stream.next_message().await.expect("bidi frame") {
        let mut br = ProtoReader::new(&frame);
        while let Some((f, wt)) = br.next_tag().expect("tag") {
            if f == 1 && wt == WireType::Varint {
                received.push(br.read_varint().expect("varint"));
            } else {
                br.skip_field(wt).expect("skip");
            }
        }
    }
    assert_eq!(received, vec![30, 70, 210]);
}

#[tokio::test]
async fn grpc_tls_h2_alpn_and_rich_google_rpc_status_trailers() {
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("generate self-signed cert");
    let cert_pem = certified.cert.pem().into_bytes();
    let cert_der: CertificateDer<'static> = certified.cert.into();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut server_tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("tls versions")
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("single cert");
    server_tls.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(server_tls));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind tls");
    let addr = listener.local_addr().expect("local_addr");

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(tls_stream) = acceptor.accept(stream).await {
                    let mut builder = server_http2::Builder::new(TokioExecutor::new());
                    builder
                        .initial_stream_window_size(8 * 1024 * 1024)
                        .initial_connection_window_size(16 * 1024 * 1024);
                    let _ = builder
                        .serve_connection(
                            TokioIo::new(tls_stream),
                            service_fn(handle_test_grpc_request),
                        )
                        .await;
                }
            });
        }
    });

    let mut cfg = GrpcPoolConfig::new(format!("https://localhost:{}", addr.port()));
    cfg.ca_pem = Some(cert_pem);
    cfg.max_decoding_message_size = 64 * 1024;
    let pool = GrpcChannelPool::new(cfg).expect("create tls h2 pool");
    let meta = GrpcRequestMetadata::default();

    // 1. TLS h2 unary echo succeeds over ALPN h2
    let mut req = ProtoWriter::new();
    req.write_str(1, "tls-h2-ok");
    let (resp, _) = pool
        .unary("/test.v1.Service/UnaryEcho", req.finish(), &meta)
        .await
        .expect("tls unary");
    let mut r = ProtoReader::new(&resp);
    assert_eq!(
        r.next_tag().expect("tag"),
        Some((1, WireType::LengthDelimited))
    );
    assert_eq!(r.read_str().expect("str"), "tls-h2-ok");

    // 2. Trailers-Only RESOURCE_EXHAUSTED response with RetryInfo, ResourceInfo, ErrorInfo, BadRequest
    let status = pool
        .unary(
            "/test.v1.Service/TrailersOnlyQuotaError",
            Bytes::new(),
            &meta,
        )
        .await
        .expect_err("expected RESOURCE_EXHAUSTED");
    assert_eq!(status.code, GrpcCode::ResourceExhausted);
    assert_eq!(status.message, "Quota exceeded for ReadRows");
    assert_eq!(status.retry_delay_ms(), Some(250));
    assert_eq!(status.details.len(), 4);
    assert!(status.details.contains(&GrpcErrorDetail::RetryInfo {
        retry_delay_ms: 250
    }));
    assert!(status.details.contains(&GrpcErrorDetail::ResourceInfo {
        resource_type: "bigquery.googleapis.com/ReadSession".to_string(),
        resource_name: "projects/p/locations/us/sessions/s1".to_string(),
        owner: "project:p".to_string(),
        description: "session quota exceeded".to_string(),
    }));
    assert!(status.details.contains(&GrpcErrorDetail::BadRequest {
        field_violations: vec![("parent".to_string(), "invalid project format".to_string())],
    }));

    // 3. Mid-stream trailing RESOURCE_EXHAUSTED after first valid message
    let mut mid_stream = pool
        .server_streaming("/test.v1.Service/MidStreamStatusError", Bytes::new(), &meta)
        .await
        .expect("open mid-stream");
    let first_chunk = mid_stream
        .next_message()
        .await
        .expect("first message ok")
        .expect("some first message");
    let mut mr = ProtoReader::new(&first_chunk);
    assert_eq!(
        mr.next_tag().expect("tag"),
        Some((1, WireType::LengthDelimited))
    );
    assert_eq!(mr.read_str().expect("str"), "first-chunk-ok");

    let mid_err = mid_stream
        .next_message()
        .await
        .expect_err("trailing status error");
    assert_eq!(mid_err.code, GrpcCode::ResourceExhausted);
    assert_eq!(mid_err.message, "Stream rate limited");
    assert_eq!(mid_err.retry_delay_ms(), Some(250));

    // 4. Oversized LPM frame (> 64 KiB max_decoding_message_size) rejected before buffering
    let mut over_stream = pool
        .server_streaming("/test.v1.Service/OversizedFrame", Bytes::new(), &meta)
        .await
        .expect("open oversized stream");
    let over_err = over_stream
        .next_message()
        .await
        .expect_err("oversized frame must fail");
    assert_eq!(over_err.code, GrpcCode::ResourceExhausted);

    // 5. Gzip decompression bomb (> max_decoding_message_size after decompression) rejected
    let bomb_raw = vec![b'A'; 128 * 1024];
    let compressed_bomb = encode_lpm_frame(&bomb_raw, true, 1024 * 1024).expect("compress bomb");
    assert!(
        compressed_bomb.len() < 4096,
        "compressed frame fits inside 64 KiB wire header check"
    );
    let mut bomb_decoder = LpmFrameDecoder::with_encoding(64 * 1024, Some("gzip"));
    bomb_decoder.push_chunk(&compressed_bomb);
    let bomb_err = bomb_decoder
        .next_frame()
        .expect_err("gzip decompression bomb must fail");
    assert_eq!(bomb_err.code, GrpcCode::ResourceExhausted);
}
