//! Token-guarded CPU and heap diagnostic profiling endpoints (`profile.rs`).

use bytes::Bytes;
use http::{HeaderMap, Response, StatusCode};
use http_body_util::Full;

/// Configuration for opt-in `/debug/pprof/*` diagnostic endpoints.
#[derive(Debug, Clone, Default)]
pub struct ProfileConfig {
    /// Whether `/debug/pprof/profile` and `/debug/pprof/heap` are active.
    pub enabled: bool,
    /// Required `Authorization: Bearer <token>` secret when `enabled` is true.
    pub bearer_token: Option<String>,
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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

    /// Intercept `/debug/pprof/profile` or `/debug/pprof/heap` requests when enabled.
    pub fn try_handle(&self, path: &str, headers: &HeaderMap) -> Option<Response<Full<Bytes>>> {
        if !self.config.enabled {
            return None;
        }
        if path != "/debug/pprof/profile" && path != "/debug/pprof/heap" {
            return None;
        }
        if !self.is_authorized(headers) {
            return Some(
                Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header("www-authenticate", "Bearer realm=\"vertebra-pprof\"")
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from_static(
                        b"{\"error\":\"unauthorized\"}",
                    )))
                    .expect("build pprof 401 response"),
            );
        }

        let payload = if path == "/debug/pprof/profile" {
            format!(
                "{{\"profile\":\"cpu\",\"pid\":{},\"threads\":{}}}",
                std::process::id(),
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
            )
        } else {
            format!(
                "{{\"profile\":\"heap\",\"pid\":{},\"allocator\":\"rust\"}}",
                std::process::id()
            )
        };

        Some(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .header("cache-control", "no-store")
                .body(Full::new(Bytes::from(payload)))
                .expect("build pprof 200 response"),
        )
    }
}
