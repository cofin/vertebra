//! Behavioral integration tests for zero-GIL health/readiness probes, request payload decompression, and multipart parsing.

use std::io::Write;

use bytes::Bytes;
use http::StatusCode;
use vertebra_http::health::{HealthEndpoint, HealthState};
use vertebra_http::multipart::{MultipartLimits, MultipartPartValue, parse_multipart_body};
use vertebra_http::payload::{PayloadError, PayloadLimits, decode_request_payload};

#[test]
fn health_and_readiness_state_machine_transitions() {
    let endpoint = HealthEndpoint::new("/healthz", "/readyz");
    assert_eq!(endpoint.state(), HealthState::Healthy);

    let live_ok = endpoint
        .try_intercept("/healthz")
        .expect("intercept /healthz");
    assert_eq!(live_ok.status(), StatusCode::OK);
    let livez_ok = endpoint.try_intercept("/livez").expect("intercept /livez");
    assert_eq!(livez_ok.status(), StatusCode::OK);
    let ready_ok = endpoint
        .try_intercept("/readyz")
        .expect("intercept /readyz");
    assert_eq!(ready_ok.status(), StatusCode::OK);
    assert!(endpoint.try_intercept("/api/v1/items").is_none());

    endpoint.set_state(HealthState::Draining);
    assert_eq!(endpoint.state(), HealthState::Draining);
    let live_draining = endpoint
        .try_intercept("/healthz")
        .expect("intercept /healthz");
    assert_eq!(live_draining.status(), StatusCode::OK);
    let ready_draining = endpoint
        .try_intercept("/readyz")
        .expect("intercept /readyz");
    assert_eq!(ready_draining.status(), StatusCode::SERVICE_UNAVAILABLE);

    endpoint.set_state(HealthState::ShuttingDown);
    let live_down = endpoint
        .try_intercept("/healthz")
        .expect("intercept /healthz");
    assert_eq!(live_down.status(), StatusCode::SERVICE_UNAVAILABLE);
    let ready_down = endpoint
        .try_intercept("/readyz")
        .expect("intercept /readyz");
    assert_eq!(ready_down.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn request_payload_decompression_and_413_bomb_protection() {
    let limits = PayloadLimits {
        max_body_size: 1024,
        max_decompressed_size: 2048,
    };
    let plain = b"hello vertebra ingress fastpath";

    let passthrough = decode_request_payload(plain, None, &limits).expect("identity payload");
    assert_eq!(passthrough.as_ref(), plain);

    let mut gz_enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz_enc.write_all(plain).unwrap();
    let gz_bytes = gz_enc.finish().unwrap();
    let gz_out = decode_request_payload(&gz_bytes, Some("gzip"), &limits).expect("gzip decode");
    assert_eq!(gz_out.as_ref(), plain);

    let mut zlib_enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    zlib_enc.write_all(plain).unwrap();
    let deflate_bytes = zlib_enc.finish().unwrap();
    let deflate_out =
        decode_request_payload(&deflate_bytes, Some("deflate"), &limits).expect("deflate decode");
    assert_eq!(deflate_out.as_ref(), plain);

    let mut br_bytes = Vec::new();
    {
        let mut br_enc = brotli::CompressorWriter::new(&mut br_bytes, 4096, 5, 22);
        br_enc.write_all(plain).unwrap();
    }
    let br_out = decode_request_payload(&br_bytes, Some("br"), &limits).expect("br decode");
    assert_eq!(br_out.as_ref(), plain);

    let zst_bytes = zstd::stream::encode_all(&plain[..], 3).unwrap();
    let zst_out = decode_request_payload(&zst_bytes, Some("zstd"), &limits).expect("zstd decode");
    assert_eq!(zst_out.as_ref(), plain);

    let oversized_raw = vec![b'a'; 1025];
    let raw_err = decode_request_payload(&oversized_raw, None, &limits).unwrap_err();
    assert_eq!(raw_err.status_code(), StatusCode::PAYLOAD_TOO_LARGE);

    let bomb_plain = vec![b'x'; 4096];
    let bomb_zst = zstd::stream::encode_all(&bomb_plain[..], 3).unwrap();
    assert!(bomb_zst.len() < limits.max_body_size);
    let bomb_err = decode_request_payload(&bomb_zst, Some("zstd"), &limits).unwrap_err();
    assert!(matches!(
        bomb_err,
        PayloadError::DecompressedTooLarge { .. }
    ));
    assert_eq!(bomb_err.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn multipart_parsing_and_limit_enforcement() {
    let boundary = "----VertebraBoundary7MA4YWxkTrZu0gW";
    let content_type = format!("multipart/form-data; boundary={boundary}");
    let body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"title\"\r\n\r\n\
         quarterly-report\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"document\"; filename=\"report.bin\"\r\n\
         Content-Type: application/octet-stream\r\n\r\n\
         BINARY_PAYLOAD_123\r\n\
         --{boundary}--\r\n"
    );

    let limits = MultipartLimits {
        max_fields: 8,
        max_file_size: 1024,
        max_total_size: 4096,
    };

    let parsed = parse_multipart_body(Bytes::from(body.clone()), &content_type, &limits)
        .await
        .expect("valid multipart body");
    assert_eq!(parsed.parts.len(), 2);
    assert_eq!(parsed.parts[0].name, "title");
    assert_eq!(
        parsed.parts[0].value,
        MultipartPartValue::Text("quarterly-report".to_string())
    );
    assert_eq!(parsed.parts[1].name, "document");
    assert_eq!(parsed.parts[1].filename.as_deref(), Some("report.bin"));
    assert_eq!(
        parsed.parts[1].content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(
        parsed.parts[1].value,
        MultipartPartValue::File(Bytes::from_static(b"BINARY_PAYLOAD_123"))
    );

    let strict_file_limits = MultipartLimits {
        max_fields: 8,
        max_file_size: 4,
        max_total_size: 4096,
    };
    let file_err = parse_multipart_body(
        Bytes::from(body.clone()),
        &content_type,
        &strict_file_limits,
    )
    .await
    .unwrap_err();
    assert_eq!(file_err.status_code(), StatusCode::PAYLOAD_TOO_LARGE);

    let strict_field_limits = MultipartLimits {
        max_fields: 1,
        max_file_size: 1024,
        max_total_size: 4096,
    };
    let field_err = parse_multipart_body(Bytes::from(body), &content_type, &strict_field_limits)
        .await
        .unwrap_err();
    assert_eq!(field_err.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lockfree_backpressure_multithreaded_cap_and_sliding_window_recovery() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use vertebra_http::backpressure::{
        BackpressureConfig, BackpressureController, CircuitBreakerConfig, CircuitState,
        RejectionReason,
    };

    let controller = Arc::new(BackpressureController::new(BackpressureConfig {
        max_in_flight: 4,
        max_queue_depth: 0,
        max_queue_time: Duration::from_millis(10),
        circuit_breaker: CircuitBreakerConfig {
            failure_rate_threshold: 0.5,
            min_samples: 8,
            window: Duration::from_millis(120),
            half_open_probe_count: 3,
        },
        ..BackpressureConfig::default()
    }));

    let peak_observed = Arc::new(AtomicUsize::new(0));
    let active_now = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(16);

    for _ in 0..16 {
        let c = Arc::clone(&controller);
        let peak = Arc::clone(&peak_observed);
        let active = Arc::clone(&active_now);
        handles.push(tokio::spawn(async move {
            let mut local_accepted = 0usize;
            let mut local_rejected = 0usize;
            for _ in 0..50 {
                match c.try_acquire().await {
                    Ok(permit) => {
                        local_accepted += 1;
                        let cur = active.fetch_add(1, Ordering::AcqRel) + 1;
                        peak.fetch_max(cur, Ordering::AcqRel);
                        tokio::task::yield_now().await;
                        active.fetch_sub(1, Ordering::AcqRel);
                        c.record_success();
                        drop(permit);
                    }
                    Err(RejectionReason::Saturated) => {
                        local_rejected += 1;
                    }
                    Err(other) => panic!("unexpected rejection reason: {other:?}"),
                }
            }
            (local_accepted, local_rejected)
        }));
    }

    let mut total_accepted = 0usize;
    let mut total_rejected = 0usize;
    for h in handles {
        let (a, r) = h.await.unwrap();
        total_accepted += a;
        total_rejected += r;
    }

    assert_eq!(total_accepted + total_rejected, 16 * 50);
    assert!(peak_observed.load(Ordering::Acquire) <= 4);
    assert_eq!(controller.snapshot().in_flight, 0);
    assert_eq!(controller.snapshot().circuit_state, CircuitState::Closed);

    // Advance past prior success window, then trip circuit with failures.
    controller.advance_circuit_clock_for_test(Duration::from_millis(150));
    for _ in 0..3 {
        controller.record_success();
    }
    for _ in 0..5 {
        controller.record_failure();
    }
    assert_eq!(controller.snapshot().circuit_state, CircuitState::Open);
    assert_eq!(
        controller.try_acquire().await.unwrap_err(),
        RejectionReason::CircuitOpen
    );

    // Advance past cooldown -> HalfOpen, admit exactly 3 probes across concurrent tasks.
    controller.advance_circuit_clock_for_test(Duration::from_millis(140));
    let mut probes = Vec::new();
    for _ in 0..3 {
        probes.push(controller.try_acquire().await.expect("probe permit"));
    }
    assert_eq!(
        controller.try_acquire().await.unwrap_err(),
        RejectionReason::CircuitOpen
    );
    for p in probes {
        controller.record_success();
        drop(p);
    }
    assert_eq!(controller.snapshot().circuit_state, CircuitState::Closed);
}

#[test]
fn encode_header_map_compact_preserves_duplicates_and_clamps_bounds() {
    use http::{HeaderMap, HeaderValue};
    use vertebra_core::transport::iter_raw_headers;
    use vertebra_http::response::encode_header_map_compact;

    let mut headers = HeaderMap::new();
    headers.append("set-cookie", HeaderValue::from_static("a=1; Path=/"));
    headers.append("set-cookie", HeaderValue::from_static("b=2; HttpOnly"));
    headers.insert("x-trace-id", HeaderValue::from_static("trace-99"));

    let encoded = encode_header_map_compact(&headers);
    let decoded: Vec<(&[u8], &[u8])> = iter_raw_headers(&encoded).collect();
    assert_eq!(
        decoded,
        vec![
            (&b"set-cookie"[..], &b"a=1; Path=/"[..]),
            (&b"set-cookie"[..], &b"b=2; HttpOnly"[..]),
            (&b"x-trace-id"[..], &b"trace-99"[..]),
        ]
    );
}

#[tokio::test]
async fn dispatch_requests_preserves_double_buffer_allocation() {
    use std::sync::Arc;
    use tokio::sync::oneshot;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
    use vertebra_http::batcher::PendingRequest;
    use vertebra_http::dispatcher::Dispatcher;

    let pair = Arc::new(RingPair::create(RingPairConfig::default()).unwrap());
    let dispatcher = Dispatcher::new_shm(vec![pair], 64);

    let mut requests = Vec::with_capacity(64);
    let ptr_before = requests.as_ptr();
    let cap_before = requests.capacity();

    for i in 0..4u64 {
        let (tx, _rx) = oneshot::channel();
        requests.push(PendingRequest {
            handler_id: i,
            method_tag: 0,
            http_version_tag: 1,
            flags: 0,
            path_and_query: http::uri::PathAndQuery::from_static("/fast"),
            headers_mp: Bytes::new(),
            body: Bytes::new(),
            reply: tx,
        });
    }

    let dispatched = dispatcher.dispatch_requests(&mut requests);
    assert_eq!(dispatched, 4);
    assert!(requests.is_empty());
    assert_eq!(requests.capacity(), cap_before);
    assert_eq!(requests.as_ptr(), ptr_before);
}

#[test]
fn offload_config_json_parses_into_server_config() {
    use vertebra_http::compression::CompressionBackend;
    use vertebra_http::config::ServerConfig;

    let mut cfg = ServerConfig::default();
    let raw = r#"{
        "compression": {
            "backend": "zstd",
            "minimum_size": 256,
            "gzip_compress_level": 6,
            "brotli_quality": 4,
            "zstd_level": 3,
            "exclude_patterns": ["/metrics"],
            "exclude_opt_key": "skip_compression",
            "excluded_paths": ["/raw"]
        },
        "cors": {
            "allow_origins": ["https://app.example"],
            "allow_methods": ["GET", "POST"],
            "allow_headers": ["content-type", "authorization"],
            "allow_credentials": true,
            "expose_headers": ["x-request-id"],
            "max_age": 600,
            "allow_origin_regex": null
        },
        "static_mounts": [
            {
                "path_prefix": "/static",
                "directories": ["/tmp"],
                "html_mode": false,
                "send_as_attachment": false,
                "headers": {"x-asset": "1"}
            }
        ],
        "openapi": null
    }"#;

    cfg.apply_offload_json(raw).expect("valid offload json");
    let comp = cfg.compression.expect("compression populated");
    assert_eq!(comp.backend, CompressionBackend::Zstd);
    assert_eq!(comp.minimum_size, 256);
    let cors = cfg.cors.expect("cors populated");
    assert_eq!(cors.allow_origins, vec!["https://app.example"]);
    assert!(cors.allow_credentials);
    assert_eq!(cfg.static_mounts.len(), 1);
    assert_eq!(cfg.static_mounts[0].path_prefix, "/static");
}

#[test]
fn cors_and_compression_preserve_vary_skip_incompressible_types_and_align_version_tags() {
    use http::{HeaderMap, HeaderValue, Version, header};
    use vertebra_http::batcher::http_version_tag;
    use vertebra_http::compression::{CompressionBackend, CompressionOffloadConfig};
    use vertebra_http::cors::CorsOffloadConfig;

    let cors = CorsOffloadConfig {
        allow_origins: vec!["https://app.example".to_string()],
        allow_methods: vec!["GET".to_string(), "POST".to_string()],
        allow_credentials: true,
        ..CorsOffloadConfig::default()
    };
    let comp = CompressionOffloadConfig {
        backend: CompressionBackend::Zstd,
        minimum_size: 64,
        ..CompressionOffloadConfig::default()
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::VARY, HeaderValue::from_static("Accept-Language"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    cors.apply_response_headers("https://app.example", &mut headers);
    let json_body = Bytes::from(vec![b'j'; 256]);
    let compressed =
        comp.compress_if_eligible("/api/data", Some("zstd"), &mut headers, json_body.clone());
    assert!(compressed.len() < json_body.len());

    cors.apply_response_headers("https://app.example", &mut headers);
    let vary_str = headers
        .get(header::VARY)
        .and_then(|v| v.to_str().ok())
        .expect("Vary header present");
    let tokens: Vec<&str> = vary_str.split(',').map(str::trim).collect();
    assert_eq!(tokens, vec!["Accept-Language", "Origin", "Accept-Encoding"]);

    let mut png_headers = HeaderMap::new();
    png_headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("image/png"));
    let png_body = Bytes::from(vec![0x89; 512]);
    let untouched = comp.compress_if_eligible(
        "/logo.png",
        Some("zstd"),
        &mut png_headers,
        png_body.clone(),
    );
    assert_eq!(untouched, png_body);
    assert!(png_headers.get(header::CONTENT_ENCODING).is_none());

    assert_eq!(http_version_tag(Version::HTTP_10), 0);
    assert_eq!(http_version_tag(Version::HTTP_11), 1);
    assert_eq!(http_version_tag(Version::HTTP_2), 2);
    assert_eq!(http_version_tag(Version::HTTP_3), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backpressure_queued_waiters_wake_without_lost_notifications() {
    use std::sync::Arc;
    use std::time::Duration;
    use vertebra_http::backpressure::{BackpressureConfig, BackpressureController};

    let controller = Arc::new(BackpressureController::new(BackpressureConfig {
        max_in_flight: 2,
        max_queue_depth: 32,
        max_queue_time: Duration::from_millis(500),
        ..BackpressureConfig::default()
    }));

    let mut handles = Vec::with_capacity(16);
    for _ in 0..16 {
        let c = Arc::clone(&controller);
        handles.push(tokio::spawn(async move {
            for _ in 0..30 {
                let permit = c.try_acquire().await.expect("queued waiter must wake");
                tokio::task::yield_now().await;
                c.record_success();
                drop(permit);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(controller.snapshot().in_flight, 0);
}

#[tokio::test]
async fn valid_multipart_upload_dispatches_intact_to_worker() {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use vertebra_http::batcher::{BatchLimit, PendingResponse, RequestBatcher};
    use vertebra_http::config::ServerConfig;
    use vertebra_http::dispatcher::Dispatcher;
    use vertebra_http::multipart::MultipartLimits;
    use vertebra_http::routing::{RouteTable, Router};
    use vertebra_http::server;

    let mut table = RouteTable::new();
    table.insert("POST", "/upload");
    let router = Arc::new(Router::from_entries(table.into_entries()).unwrap());
    let (dispatcher, mut direct_rx) = Dispatcher::new_direct(8);
    let dispatcher = Arc::new(dispatcher);
    let (collector, collector_tx) =
        RequestBatcher::new(Arc::clone(&dispatcher), BatchLimit::default(), 16);
    let collector_handle = tokio::spawn(collector.run());

    let boundary = "----VertebraFastpathBoundary";
    let multipart_body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"title\"\r\n\r\n\
         spec-report\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"file\"; filename=\"data.bin\"\r\n\
         Content-Type: application/octet-stream\r\n\r\n\
         PAYLOAD_INTACT_BYTES\r\n\
         --{boundary}--\r\n"
    );
    let expected_body = Bytes::from(multipart_body.clone());

    let worker_handle = tokio::spawn(async move {
        let mut batch = direct_rx.recv().await.expect("worker batch");
        assert_eq!(batch.requests.len(), 1);
        let req = batch.requests.pop().unwrap();
        let body = req.body.clone();
        let _ = req.reply.send(PendingResponse {
            status: 200,
            headers_mp: Bytes::new(),
            body,
            stream_rx: None,
        });
    });

    let listener = server::bind_listener("127.0.0.1:0".parse().unwrap(), false).unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Arc::new(ServerConfig {
        bind: addr,
        multipart_limits: MultipartLimits {
            max_fields: 4,
            max_file_size: 1024,
            max_total_size: 4096,
        },
        drain_timeout: Duration::from_millis(200),
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

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let resp = client
        .post(format!("http://{addr}/upload"))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(multipart_body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let echoed = resp.bytes().await.unwrap();
    assert_eq!(echoed, expected_body);

    worker_handle.await.unwrap();
    let _ = shutdown_tx.send(());
    server_handle.await.unwrap();
    collector_handle.abort();
}
