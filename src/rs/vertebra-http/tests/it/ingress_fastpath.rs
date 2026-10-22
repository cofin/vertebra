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
