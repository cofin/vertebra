//! Wiring between Vertebra's `GcpTokenCache`, canonical gRPC/GAX status metadata, and the
//! official Google Cloud Rust clients.

use std::sync::Arc;

use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag, anonymous,
};
use google_cloud_auth::errors::CredentialsError;
use google_cloud_gax::error::rpc::StatusDetails;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use http::{Extensions, HeaderMap};
use vertebra_core::gcp_auth::GcpTokenCache;

const QUOTA_PROJECT_HEADER: HeaderName = HeaderName::from_static("x-goog-user-project");

const CODE_NAMES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

pub use crate::install_rustls_ring_provider;

/// Return the canonical uppercase gRPC status name for `code` (`0..=16`), or `"UNKNOWN"`.
pub fn code_name(code: i32) -> &'static str {
    if (0..=16).contains(&code) {
        CODE_NAMES[code as usize]
    } else {
        "UNKNOWN"
    }
}

/// Parse an ASCII gRPC status name into its canonical numeric code (`0..=16`), or `2` (`UNKNOWN`).
pub fn code_number(name: &str) -> i32 {
    let trimmed = name.trim();
    for (idx, candidate) in CODE_NAMES.iter().enumerate() {
        if candidate.eq_ignore_ascii_case(trimmed) {
            return idx as i32;
        }
    }
    2
}

/// Map an HTTP response status code onto its canonical gRPC status code (`0..=16`).
pub fn http_code(status: u16) -> i32 {
    match status {
        400 => 3,
        401 => 16,
        403 => 7,
        404 => 5,
        409 => 10,
        412 => 9,
        416 => 11,
        429 => 8,
        499 => 1,
        500 => 13,
        501 => 12,
        503 => 14,
        504 => 4,
        _ => 2,
    }
}

/// Normalized Google Cloud / gRPC status metadata shared across standalone GCP driver crates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpErrorInfo {
    pub code: i32,
    pub message: String,
    pub http_status: Option<u16>,
    pub retry_delay_ms: Option<u64>,
    pub reason: Option<String>,
}

impl GcpErrorInfo {
    /// Construct a [`GcpErrorInfo`] from a canonical gRPC code and message.
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            http_status: None,
            retry_delay_ms: None,
            reason: None,
        }
    }
}

/// Extract canonical gRPC status code, message, HTTP status, `RetryInfo`, and `ErrorInfo` from a
/// `google_cloud_gax` client error.
pub fn gax_error_info(err: &google_cloud_gax::error::Error) -> GcpErrorInfo {
    let http_status = err.http_status_code();
    if let Some(status) = err.status() {
        let code = code_number(status.code.name());
        let message = if status.message.is_empty() {
            err.to_string()
        } else {
            status.message.clone()
        };
        let mut retry_delay_ms = None;
        let mut reason = None;
        for detail in &status.details {
            match detail {
                StatusDetails::RetryInfo(retry) if retry_delay_ms.is_none() => {
                    if let Some(delay) = &retry.retry_delay {
                        let secs = delay.seconds().max(0) as u64;
                        let nanos = delay.nanos().max(0) as u64;
                        retry_delay_ms = Some(secs * 1000 + nanos / 1_000_000);
                    }
                }
                StatusDetails::ErrorInfo(info) if reason.is_none() && !info.reason.is_empty() => {
                    reason = Some(info.reason.clone());
                }
                _ => {}
            }
        }
        GcpErrorInfo {
            code,
            message,
            http_status,
            retry_delay_ms,
            reason,
        }
    } else {
        let code = if err.is_authentication() {
            16
        } else if err.is_timeout() {
            4
        } else if let Some(http) = http_status {
            http_code(http)
        } else if err.is_transport() || err.is_io() || err.is_connect() {
            14
        } else {
            2
        };
        GcpErrorInfo {
            code,
            message: err.to_string(),
            http_status,
            retry_delay_ms: None,
            reason: None,
        }
    }
}

/// `google-cloud-auth` credentials backed by one shared [`GcpTokenCache`].
#[derive(Clone, Debug)]
pub struct GcpTokenCredentials {
    cache: Arc<GcpTokenCache>,
}

impl GcpTokenCredentials {
    /// Wrap `cache` so official clients reuse its tokens and refresh policy.
    pub fn new(cache: Arc<GcpTokenCache>) -> Self {
        Self { cache }
    }
}

impl CredentialsProvider for GcpTokenCredentials {
    async fn headers(
        &self,
        _extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        let snapshot = self
            .cache
            .get_or_refresh()
            .await
            .map_err(|message| CredentialsError::from_msg(true, message))?;
        let mut headers = HeaderMap::with_capacity(2);
        let bearer = HeaderValue::from_str(&format!("Bearer {}", snapshot.access_token))
            .map_err(|err| CredentialsError::from_source(false, err))?;
        headers.insert(AUTHORIZATION, bearer);
        if let Some(project) = snapshot
            .quota_project_id
            .as_deref()
            .filter(|p| !p.is_empty())
        {
            let value = HeaderValue::from_str(project)
                .map_err(|err| CredentialsError::from_source(false, err))?;
            headers.insert(QUOTA_PROJECT_HEADER, value);
        }
        Ok(CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}

/// Return credentials for an official client: the shared cache when present, anonymous otherwise.
pub fn official_credentials(cache: Option<Arc<GcpTokenCache>>) -> Credentials {
    match cache {
        Some(cache) => GcpTokenCredentials::new(cache).into(),
        None => anonymous::Builder::new().build(),
    }
}

#[cfg(test)]
mod tests {
    use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};

    use super::{code_name, code_number, gax_error_info, http_code};

    #[test]
    fn canonical_status_and_http_code_mappings() {
        assert_eq!(code_name(0), "OK");
        assert_eq!(code_name(10), "ABORTED");
        assert_eq!(code_name(16), "UNAUTHENTICATED");
        assert_eq!(code_name(99), "UNKNOWN");
        assert_eq!(code_number("not_found"), 5);
        assert_eq!(code_number("UNRECOGNIZED"), 2);
        assert_eq!(http_code(429), 8);
        assert_eq!(http_code(503), 14);
        assert_eq!(http_code(418), 2);
    }

    #[test]
    fn raw_http_errors_preserve_status_classification() {
        for status in [400, 401, 403, 404, 409, 429, 500, 503, 504] {
            let error = google_cloud_gax::error::Error::http(
                status,
                http::HeaderMap::new(),
                bytes::Bytes::from_static(b"request failed"),
            );
            let info = gax_error_info(&error);
            assert_eq!(info.code, http_code(status), "HTTP {status}");
            assert_eq!(info.http_status, Some(status));
        }
    }

    #[test]
    fn gax_error_info_extracts_retry_delay_and_error_reason() {
        let status = Status::default()
            .set_code(Code::Aborted)
            .set_message("conflict")
            .set_details([
                StatusDetails::RetryInfo(
                    google_cloud_rpc::model::RetryInfo::new().set_retry_delay(
                        google_cloud_wkt::Duration::new(1, 250_000_000).expect("duration"),
                    ),
                ),
                StatusDetails::ErrorInfo(
                    google_cloud_rpc::model::ErrorInfo::new().set_reason("ABORTED_BY_SPANNER"),
                ),
            ]);
        let info = gax_error_info(&google_cloud_gax::error::Error::service(status));
        assert_eq!(info.code, 10);
        assert_eq!(info.message, "conflict");
        assert_eq!(info.retry_delay_ms, Some(1250));
        assert_eq!(info.reason.as_deref(), Some("ABORTED_BY_SPANNER"));
    }
}
