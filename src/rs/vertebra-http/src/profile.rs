//! Token-guarded CPU and heap diagnostic profiling endpoints (`profile.rs`).

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Response, StatusCode};
use http_body_util::Full;
use vertebra_core::util::constant_time_eq;

const MIN_PROFILE_DURATION: Duration = Duration::from_millis(10);
const MAX_PROFILE_DURATION: Duration = Duration::from_millis(5_000);
const DEFAULT_PROFILE_DURATION: Duration = Duration::from_millis(1_000);

/// Configuration for opt-in `/debug/pprof/*` diagnostic endpoints.
#[derive(Debug, Clone, Default)]
pub struct ProfileConfig {
    /// Whether `/debug/pprof/profile` and `/debug/pprof/heap` are active.
    pub enabled: bool,
    /// Required `Authorization: Bearer <token>` secret when `enabled` is true.
    pub bearer_token: Option<String>,
}

impl ProfileConfig {
    /// Verify whether the incoming `Authorization` header matches the configured bearer token in constant time.
    pub fn is_authorized(&self, headers: &HeaderMap) -> bool {
        let Some(ref expected) = self.bearer_token else {
            return false;
        };
        let Some(raw_auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) else {
            return false;
        };
        raw_auth
            .strip_prefix("Bearer ")
            .is_some_and(|token| constant_time_eq(token.trim().as_bytes(), expected.as_bytes()))
    }
}

/// Zero-GIL profiling endpoint handler for `/debug/pprof/profile` and `/debug/pprof/heap`.
pub struct ProfileEndpoint {
    config: ProfileConfig,
}

impl ProfileEndpoint {
    pub fn new(config: ProfileConfig) -> Self {
        Self { config }
    }

    fn is_authorized(&self, headers: &HeaderMap) -> bool {
        self.config.is_authorized(headers)
    }

    fn preflight(
        &self,
        uri_str: &str,
        headers: &HeaderMap,
    ) -> Option<Result<Duration, Response<Full<Bytes>>>> {
        if !self.config.enabled {
            return None;
        }
        let (path, query) = match uri_str.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (uri_str, None),
        };
        let is_heap = match path {
            "/debug/pprof/profile" => false,
            "/debug/pprof/heap" => true,
            _ => return None,
        };
        if !self.is_authorized(headers) {
            return Some(Err(json_error_response(
                StatusCode::UNAUTHORIZED,
                b"{\"error\":\"unauthorized\"}",
                Some("Bearer realm=\"vertebra-pprof\""),
            )));
        }
        if is_heap {
            return Some(Err(json_error_response(
                StatusCode::NOT_FOUND,
                b"{\"error\":\"heap profiling not supported\"}",
                None,
            )));
        }
        Some(Ok(parse_profile_duration(query)))
    }

    /// Synchronously intercept `/debug/pprof/profile` or `/debug/pprof/heap` requests when enabled.
    pub fn try_handle(&self, uri_str: &str, headers: &HeaderMap) -> Option<Response<Full<Bytes>>> {
        let duration = match self.preflight(uri_str, headers)? {
            Ok(duration) => duration,
            Err(resp) => return Some(resp),
        };
        Some(handle_cpu_profile_sync(duration))
    }

    /// Asynchronously intercept `/debug/pprof/profile` or `/debug/pprof/heap` requests,
    /// offloading blocking `pprof` CPU sampling off the Tokio reactor.
    pub async fn try_handle_async(
        &self,
        uri_str: &str,
        headers: &HeaderMap,
    ) -> Option<Response<Full<Bytes>>> {
        let duration = match self.preflight(uri_str, headers)? {
            Ok(duration) => duration,
            Err(resp) => return Some(resp),
        };
        Some(handle_cpu_profile_async(duration).await)
    }
}

fn json_error_response(
    status: StatusCode,
    body: &'static [u8],
    www_authenticate: Option<&'static str>,
) -> Response<Full<Bytes>> {
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store");
    if let Some(auth_header) = www_authenticate {
        builder = builder.header("www-authenticate", auth_header);
    }
    builder
        .body(Full::new(Bytes::from_static(body)))
        .expect("build pprof error response")
}

fn parse_profile_duration(query: Option<&str>) -> Duration {
    let Some(query) = query else {
        return DEFAULT_PROFILE_DURATION;
    };
    let mut selected = None;
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        match key {
            "duration_ms" => {
                if let Ok(ms) = value.parse::<u64>() {
                    selected = Some(Duration::from_millis(ms));
                }
            }
            "seconds" => {
                if let Ok(secs) = value.parse::<f64>()
                    && secs.is_finite()
                {
                    let ms = (secs.max(0.0) * 1000.0).round().clamp(
                        MIN_PROFILE_DURATION.as_millis() as f64,
                        MAX_PROFILE_DURATION.as_millis() as f64,
                    ) as u64;
                    selected = Some(Duration::from_millis(ms));
                }
            }
            _ => {}
        }
    }
    selected
        .unwrap_or(DEFAULT_PROFILE_DURATION)
        .clamp(MIN_PROFILE_DURATION, MAX_PROFILE_DURATION)
}

#[cfg(not(all(unix, feature = "profiling")))]
fn handle_cpu_profile_sync(_duration: Duration) -> Response<Full<Bytes>> {
    json_error_response(
        StatusCode::NOT_FOUND,
        b"{\"error\":\"profiling feature disabled\"}",
        None,
    )
}

#[cfg(not(all(unix, feature = "profiling")))]
async fn handle_cpu_profile_async(duration: Duration) -> Response<Full<Bytes>> {
    handle_cpu_profile_sync(duration)
}

#[cfg(all(unix, feature = "profiling"))]
fn handle_cpu_profile_sync(duration: Duration) -> Response<Full<Bytes>> {
    match collect_cpu_profile(duration) {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/octet-stream")
            .header("x-content-type-options", "nosniff")
            .header("cache-control", "no-store")
            .body(Full::new(Bytes::from(bytes)))
            .expect("build pprof 200 response"),
        Err(_) => json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            b"{\"error\":\"cpu profile capture failed\"}",
            None,
        ),
    }
}

#[cfg(all(unix, feature = "profiling"))]
async fn handle_cpu_profile_async(duration: Duration) -> Response<Full<Bytes>> {
    match tokio::task::spawn_blocking(move || collect_cpu_profile(duration)).await {
        Ok(Ok(bytes)) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/octet-stream")
            .header("x-content-type-options", "nosniff")
            .header("cache-control", "no-store")
            .body(Full::new(Bytes::from(bytes)))
            .expect("build pprof 200 response"),
        Ok(Err(_)) | Err(_) => json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            b"{\"error\":\"cpu profile capture failed\"}",
            None,
        ),
    }
}

#[cfg(all(unix, feature = "profiling"))]
fn collect_cpu_profile(duration: Duration) -> Result<Vec<u8>, String> {
    use pprof::protos::Message;
    use std::sync::Mutex;

    static PROFILE_LOCK: Mutex<()> = Mutex::new(());
    let _lock = PROFILE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let builder = pprof::ProfilerGuardBuilder::default().frequency(99);
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    let builder = builder.blocklist(&["libc", "libgcc", "pthread", "vdso"]);

    let guard = builder.build().map_err(|e| e.to_string())?;
    std::thread::sleep(duration);
    let report = guard.report().build().map_err(|e| e.to_string())?;
    let profile = report.pprof().map_err(|e| e.to_string())?;
    let mut encoded = Vec::new();
    profile
        .write_to_vec(&mut encoded)
        .map_err(|e| e.to_string())?;
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;
    use http_body_util::BodyExt;

    fn endpoint_with_token(token: &str, enabled: bool) -> ProfileEndpoint {
        ProfileEndpoint::new(ProfileConfig {
            enabled,
            bearer_token: Some(token.to_string()),
        })
    }

    fn auth_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    #[test]
    fn disabled_and_unmatched_paths_return_none() {
        let disabled = endpoint_with_token("secret", false);
        assert!(
            disabled
                .try_handle("/debug/pprof/profile", &HeaderMap::new())
                .is_none()
        );

        let enabled = endpoint_with_token("secret", true);
        assert!(enabled.try_handle("/health", &HeaderMap::new()).is_none());
    }

    #[test]
    fn auth_checked_before_heap_or_feature_gating() {
        let endpoint = endpoint_with_token("secret-token", true);
        let missing = HeaderMap::new();
        let unauth_profile = endpoint
            .try_handle("/debug/pprof/profile", &missing)
            .expect("intercept profile");
        assert_eq!(unauth_profile.status(), StatusCode::UNAUTHORIZED);

        let unauth_heap = endpoint
            .try_handle("/debug/pprof/heap", &missing)
            .expect("intercept heap");
        assert_eq!(unauth_heap.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn authorized_heap_always_returns_404() {
        let endpoint = endpoint_with_token("secret-token", true);
        let headers = auth_headers("secret-token");
        let sync_resp = endpoint
            .try_handle("/debug/pprof/heap", &headers)
            .expect("intercept heap sync");
        assert_eq!(sync_resp.status(), StatusCode::NOT_FOUND);

        let async_resp = endpoint
            .try_handle_async("/debug/pprof/heap", &headers)
            .await
            .expect("intercept heap async");
        assert_eq!(async_resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn duration_query_is_clamped_between_10ms_and_5000ms() {
        assert_eq!(parse_profile_duration(None), DEFAULT_PROFILE_DURATION);
        assert_eq!(
            parse_profile_duration(Some("duration_ms=1")),
            Duration::from_millis(10)
        );
        assert_eq!(
            parse_profile_duration(Some("duration_ms=99999")),
            Duration::from_millis(5_000)
        );
        assert_eq!(
            parse_profile_duration(Some("duration_ms=250")),
            Duration::from_millis(250)
        );
        assert_eq!(
            parse_profile_duration(Some("seconds=0")),
            Duration::from_millis(10)
        );
        assert_eq!(
            parse_profile_duration(Some("seconds=30")),
            Duration::from_millis(5_000)
        );
        assert_eq!(
            parse_profile_duration(Some("seconds=0.25")),
            Duration::from_millis(250)
        );
        assert_eq!(
            parse_profile_duration(Some("seconds=NaN")),
            DEFAULT_PROFILE_DURATION
        );
    }

    #[cfg(not(all(unix, feature = "profiling")))]
    #[tokio::test]
    async fn unfeatured_profile_returns_404_json() {
        let endpoint = endpoint_with_token("secret-token", true);
        let headers = auth_headers("secret-token");
        let resp = endpoint
            .try_handle_async("/debug/pprof/profile?duration_ms=20", &headers)
            .await
            .expect("intercept profile");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"{\"error\":\"profiling feature disabled\"}");
    }

    #[cfg(all(unix, feature = "profiling"))]
    #[tokio::test]
    async fn featured_profile_captures_non_empty_protobuf_off_reactor() {
        use pprof::protos::Message;

        let endpoint = endpoint_with_token("secret-token", true);
        let headers = auth_headers("secret-token");
        let resp = endpoint
            .try_handle_async("/debug/pprof/profile?duration_ms=30", &headers)
            .await
            .expect("intercept profile");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/octet-stream")
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(!body.is_empty());
        let decoded = pprof::protos::Profile::parse_from_bytes(&body)
            .expect("response body must be valid pprof protobuf");
        assert!(!decoded.string_table.is_empty());
    }
}
