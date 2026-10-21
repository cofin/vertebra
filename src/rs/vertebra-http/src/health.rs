//! Lock-free atomic health and readiness probe interception (`health.rs`).

use std::sync::atomic::{AtomicU8, Ordering};

use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::Full;

/// Lifecycle health and readiness state of the HTTP server.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    /// Server is accepting and routing both liveness and readiness probes (`200 OK`).
    Healthy = 0,
    /// Server is draining active connections: liveness returns `200 OK`, readiness returns `503`.
    Draining = 1,
    /// Server is shutting down: both liveness and readiness return `503`.
    ShuttingDown = 2,
}

impl HealthState {
    #[inline]
    pub fn from_u8(val: u8) -> Self {
        match val {
            0 => Self::Healthy,
            1 => Self::Draining,
            _ => Self::ShuttingDown,
        }
    }
}

/// Zero-GIL health and readiness probe handler backed by an `AtomicU8`.
pub struct HealthEndpoint {
    state: AtomicU8,
    liveness_path: String,
    readiness_path: String,
}

impl Default for HealthEndpoint {
    fn default() -> Self {
        Self::new("/healthz", "/readyz")
    }
}

impl HealthEndpoint {
    /// Create a new `HealthEndpoint` for the configured liveness and readiness paths.
    pub fn new(liveness_path: impl Into<String>, readiness_path: impl Into<String>) -> Self {
        Self {
            state: AtomicU8::new(HealthState::Healthy as u8),
            liveness_path: liveness_path.into(),
            readiness_path: readiness_path.into(),
        }
    }

    /// Read the current `HealthState` with `Ordering::Acquire`.
    #[inline]
    pub fn state(&self) -> HealthState {
        HealthState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Transition to a new `HealthState` with `Ordering::Release`.
    #[inline]
    pub fn set_state(&self, next: HealthState) {
        self.state.store(next as u8, Ordering::Release);
    }

    /// Intercept a request path if it matches the liveness or readiness endpoint.
    pub fn try_intercept(&self, path: &str) -> Option<Response<Full<Bytes>>> {
        let current = self.state();
        if path == self.liveness_path || path == "/__vertebra/health" || path == "/livez" {
            let (status, body) = match current {
                HealthState::Healthy => (
                    StatusCode::OK,
                    Bytes::from_static(b"{\"status\":\"healthy\"}"),
                ),
                HealthState::Draining => (
                    StatusCode::OK,
                    Bytes::from_static(b"{\"status\":\"draining\"}"),
                ),
                HealthState::ShuttingDown => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"{\"status\":\"shutting_down\"}"),
                ),
            };
            return Some(
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .header("cache-control", "no-store")
                    .body(Full::new(body))
                    .expect("build liveness response"),
            );
        }

        if path == self.readiness_path {
            let (status, body) = match current {
                HealthState::Healthy => (
                    StatusCode::OK,
                    Bytes::from_static(b"{\"status\":\"ready\"}"),
                ),
                HealthState::Draining => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"{\"status\":\"draining\"}"),
                ),
                HealthState::ShuttingDown => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"{\"status\":\"shutting_down\"}"),
                ),
            };
            return Some(
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .header("cache-control", "no-store")
                    .body(Full::new(body))
                    .expect("build readiness response"),
            );
        }

        None
    }
}
