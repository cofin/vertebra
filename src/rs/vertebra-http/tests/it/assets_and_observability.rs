//! Behavioral integration tests for static assets (`ETag`, `304`, `Range`, `.zst`/`.br`/`.gz`), compiled access logging, response compression, and pprof endpoints.

use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use tempfile::tempdir;
use vertebra_http::access_log::{AccessLogEntry, AccessLogFormat, AccessLogger};
use vertebra_http::assets::{AssetManager, StaticAssetServeConfig};
use vertebra_http::compression::{CompressionConfig, ResponseCodec, compress_response_body};
use vertebra_http::profile::{ProfileConfig, ProfileEndpoint};

#[tokio::test]
async fn static_assets_etag_304_precompressed_sidecars_and_range_206() {
    let dir = tempdir().unwrap();
    let raw_js = b"console.log('vertebra static asset payload 0123456789');";
    std::fs::write(dir.path().join("app.a1b2c3d4.js"), raw_js).unwrap();
    std::fs::write(dir.path().join("app.a1b2c3d4.js.zst"), b"ZSTD_SIDECAR").unwrap();
    std::fs::write(dir.path().join("app.a1b2c3d4.js.br"), b"BROTLI_SIDECAR").unwrap();
    std::fs::write(dir.path().join("video.mp4"), b"0123456789abcdef").unwrap();

    let cfg = StaticAssetServeConfig::default();
    let mgr = AssetManager::with_config(Some(dir.path().to_str().unwrap()), None, cfg);

    let mut headers = HeaderMap::new();
    let first = mgr
        .serve_static_request("/app.a1b2c3d4.js", &headers)
        .await
        .expect("serve hashed js");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("public, max-age=31536000, immutable")
    );
    let etag = first
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    headers.insert("if-none-match", HeaderValue::from_str(&etag).unwrap());
    let not_modified = mgr
        .serve_static_request("/app.a1b2c3d4.js", &headers)
        .await
        .expect("304 response");
    assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        not_modified
            .headers()
            .get("vary")
            .and_then(|v| v.to_str().ok()),
        Some("Accept-Encoding")
    );

    let mut zst_headers = HeaderMap::new();
    zst_headers.insert(
        "accept-encoding",
        HeaderValue::from_static("gzip, zstd, br"),
    );
    let zst_resp = mgr
        .serve_static_request("/app.a1b2c3d4.js", &zst_headers)
        .await
        .expect("serve zst sidecar");
    assert_eq!(
        zst_resp
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok()),
        Some("zstd")
    );
    let zst_etag = zst_resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();
    assert!(zst_etag.ends_with("-zstd\""));
    let zst_body = zst_resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(zst_body.as_ref(), b"ZSTD_SIDECAR");

    let mut q_zero_headers = HeaderMap::new();
    q_zero_headers.insert(
        "accept-encoding",
        HeaderValue::from_static("zstd;q=0, br;q=1.0"),
    );
    let br_resp = mgr
        .serve_static_request("/app.a1b2c3d4.js", &q_zero_headers)
        .await
        .expect("serve br sidecar when zstd has q=0");
    assert_eq!(
        br_resp
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok()),
        Some("br")
    );
    let br_etag = br_resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();
    assert!(br_etag.ends_with("-br\""));
    let br_body = br_resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(br_body.as_ref(), b"BROTLI_SIDECAR");

    let mut range_headers = HeaderMap::new();
    range_headers.insert("range", HeaderValue::from_static("bytes=4-9"));
    let partial = mgr
        .serve_static_request("/video.mp4", &range_headers)
        .await
        .expect("206 response");
    assert_eq!(partial.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        partial
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok()),
        Some("bytes 4-9/16")
    );
    let partial_body = partial.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(partial_body.as_ref(), b"456789");

    let mut bad_range_headers = HeaderMap::new();
    bad_range_headers.insert("range", HeaderValue::from_static("bytes=50-60"));
    let invalid_range = mgr
        .serve_static_request("/video.mp4", &bad_range_headers)
        .await
        .expect("416 response");
    assert_eq!(invalid_range.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[test]
fn compiled_access_logger_formats_and_background_channel() {
    let logger = AccessLogger::new(AccessLogFormat::Json);
    let entry = AccessLogEntry {
        remote_addr: "127.0.0.1",
        method: "GET",
        path: "/api/health",
        protocol: "HTTP/2.0",
        status: 200,
        response_bytes: 128,
        duration_micros: 420,
        referer: "-",
        user_agent: "vertebra-test/1.0",
    };
    let line = logger.format_entry(&entry);
    assert!(line.contains("\"method\":\"GET\""));
    assert!(line.contains("\"path\":\"/api/health\""));
    assert!(line.contains("\"status\":200"));
    assert!(line.contains("\"duration_us\":420"));

    logger.record(&entry);
    let drained = logger.drain_buffered();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0], line);
}

#[test]
fn response_compression_negotiation_and_threshold() {
    let cfg = CompressionConfig {
        minimum_size: 16,
        ..CompressionConfig::default()
    };
    let small = b"tiny";
    assert!(compress_response_body(small, Some("zstd, br, gzip"), "text/plain", &cfg).is_none());

    let payload = b"compressible response payload repeated for vertebra http compression test";
    let (codec, compressed) = compress_response_body(
        payload,
        Some("gzip, zstd;q=1.0, br;q=0.9"),
        "application/json",
        &cfg,
    )
    .expect("compress with zstd");
    assert_eq!(codec, ResponseCodec::Zstd);
    let roundtrip = zstd::stream::decode_all(&compressed[..]).unwrap();
    assert_eq!(roundtrip.as_slice(), payload);
}

#[test]
fn profile_endpoint_bearer_token_guard() {
    let endpoint = ProfileEndpoint::new(ProfileConfig {
        enabled: true,
        bearer_token: Some("secret-token-123".to_string()),
    });

    let no_auth = HeaderMap::new();
    let denied = endpoint
        .try_handle("/debug/pprof/profile", &no_auth)
        .expect("intercept pprof");
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let mut valid_auth = HeaderMap::new();
    valid_auth.insert(
        "authorization",
        HeaderValue::from_static("Bearer secret-token-123"),
    );
    let cpu_ok = endpoint
        .try_handle("/debug/pprof/profile", &valid_auth)
        .expect("intercept cpu pprof");
    assert_eq!(cpu_ok.status(), StatusCode::OK);

    let heap_ok = endpoint
        .try_handle("/debug/pprof/heap", &valid_auth)
        .expect("intercept heap pprof");
    assert_eq!(heap_ok.status(), StatusCode::OK);
}
