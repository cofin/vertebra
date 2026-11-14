//! Presigned URL signing and verification for HMAC-SHA256, S3 SigV4, Azure SAS, and GCS V4 (local RSA + IAM signBlob).

use std::collections::HashMap;
use std::fmt;
use std::fmt::Write as _;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, TimeZone, Utc};
use hmac::{Hmac, Mac};
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use url::Url;
use vertebra_core::gcp_auth::{
    GcpTokenCache, SharedGcpTokenCacheHandle, extract_adc_service_account_key,
};
use vertebra_core::util::{constant_time_eq, sha256_hex};

use crate::store::StorageError;

pub const MIN_PRESIGN_EXPIRES_SECS: u64 = 1;
pub const MAX_PRESIGN_EXPIRES_SECS: u64 = 604_800;

type HmacSha256 = Hmac<Sha256>;

/// Typed error returned by multi-cloud URL and header signing operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SigningError {
    InvalidExpiry(String),
    InvalidKey(String),
    InvalidInput(String),
    RemoteSigning(String),
}

impl fmt::Display for SigningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidExpiry(msg)
            | Self::InvalidKey(msg)
            | Self::InvalidInput(msg)
            | Self::RemoteSigning(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for SigningError {}

impl From<SigningError> for StorageError {
    fn from(err: SigningError) -> Self {
        match err {
            SigningError::InvalidExpiry(msg)
            | SigningError::InvalidKey(msg)
            | SigningError::InvalidInput(msg) => Self::InvalidInput(msg),
            SigningError::RemoteSigning(msg) => Self::Signing(msg),
        }
    }
}

impl From<SigningError> for pyo3::PyErr {
    fn from(err: SigningError) -> Self {
        match err {
            SigningError::InvalidExpiry(msg)
            | SigningError::InvalidKey(msg)
            | SigningError::InvalidInput(msg) => pyo3::exceptions::PyValueError::new_err(msg),
            SigningError::RemoteSigning(msg) => pyo3::exceptions::PyRuntimeError::new_err(msg),
        }
    }
}

/// Validate that `expires_in_secs` falls within `1..=604_800` (7 days).
pub fn validate_expires_in_secs(expires_in_secs: u64) -> Result<u64, SigningError> {
    if !(MIN_PRESIGN_EXPIRES_SECS..=MAX_PRESIGN_EXPIRES_SECS).contains(&expires_in_secs) {
        return Err(SigningError::InvalidExpiry(format!(
            "expires_in_secs must be in {MIN_PRESIGN_EXPIRES_SECS}..={MAX_PRESIGN_EXPIRES_SECS}, got {expires_in_secs}"
        )));
    }
    Ok(expires_in_secs)
}

/// Percent-encode `input` per RFC 3986 (`%20` for spaces, uppercase hex, preserving `-._~` and optionally `/`).
pub(crate) fn rfc3986_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char);
            }
            b'/' if !encode_slash => {
                out.push('/');
            }
            _ => {
                let _ = write!(&mut out, "%{b:02X}");
            }
        }
    }
    out
}

fn percent_decode_utf8(input: &str) -> Result<String, SigningError> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] == b'%' {
            if idx + 2 >= bytes.len() {
                return Err(SigningError::InvalidInput(format!(
                    "incomplete percent-encoding in `{input}`"
                )));
            }
            let hex_pair = std::str::from_utf8(&bytes[idx + 1..idx + 3]).map_err(|_| {
                SigningError::InvalidInput(format!("invalid percent-encoding in `{input}`"))
            })?;
            let decoded = u8::from_str_radix(hex_pair, 16).map_err(|_| {
                SigningError::InvalidInput(format!("invalid hex escape `%{hex_pair}` in `{input}`"))
            })?;
            out.push(decoded);
            idx += 3;
        } else {
            out.push(bytes[idx]);
            idx += 1;
        }
    }
    String::from_utf8(out).map_err(|e| {
        SigningError::InvalidInput(format!("decoded URI path is not valid UTF-8: {e}"))
    })
}

pub(crate) fn parse_rsa_private_key(pem: &str) -> Result<RsaPrivateKey, SigningError> {
    let trimmed = pem.trim();
    if trimmed.is_empty() {
        return Err(SigningError::InvalidKey(
            "RSA private key PEM must not be empty".to_string(),
        ));
    }
    RsaPrivateKey::from_pkcs8_pem(trimmed)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(trimmed))
        .map_err(|e| SigningError::InvalidKey(format!("invalid RSA private key PEM: {e}")))
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Vec<u8>, SigningError> {
    if key.is_empty() {
        return Err(SigningError::InvalidKey(
            "HMAC signing key must not be empty".to_string(),
        ));
    }
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| SigningError::InvalidKey(format!("invalid HMAC key length: {e}")))?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

#[derive(Clone, Debug)]
pub struct HmacSigner {
    secret: Vec<u8>,
}

impl HmacSigner {
    pub fn new(secret: impl AsRef<[u8]>) -> Self {
        Self {
            secret: secret.as_ref().to_vec(),
        }
    }

    pub fn sign_url_checked(
        &self,
        base_url: &str,
        method: &str,
        expires_in_secs: u64,
        now_unix: i64,
    ) -> Result<String, SigningError> {
        validate_expires_in_secs(expires_in_secs)?;
        let mut url = Url::parse(base_url)
            .map_err(|e| SigningError::InvalidInput(format!("invalid URL `{base_url}`: {e}")))?;
        let expires_at = now_unix
            .checked_add(expires_in_secs as i64)
            .ok_or_else(|| SigningError::InvalidExpiry("expiry timestamp overflow".to_string()))?;
        let upper_method = method.trim().to_ascii_uppercase();
        let canonical = format!("{upper_method}\n{}\n{expires_at}", url.path());
        let sig = hex::encode(hmac_sha256(&self.secret, canonical.as_bytes())?);
        url.query_pairs_mut()
            .append_pair("X-Vtb-Expires", &expires_at.to_string())
            .append_pair("X-Vtb-Method", &upper_method)
            .append_pair("X-Vtb-Signature", &sig);
        Ok(url.into())
    }

    pub fn sign_url(
        &self,
        base_url: &str,
        method: &str,
        expires_in_secs: u64,
        now_unix: i64,
    ) -> Result<String, StorageError> {
        self.sign_url_checked(base_url, method, expires_in_secs, now_unix)
            .map_err(StorageError::from)
    }

    pub fn verify_url_checked(
        &self,
        signed_url: &str,
        method: &str,
        now_unix: i64,
    ) -> Result<bool, SigningError> {
        let parsed = Url::parse(signed_url)
            .map_err(|e| SigningError::InvalidInput(format!("invalid URL `{signed_url}`: {e}")))?;
        let mut expires_at: Option<i64> = None;
        let mut signed_method: Option<String> = None;
        let mut signature: Option<String> = None;

        for (k, v) in parsed.query_pairs() {
            match k.as_ref() {
                "X-Vtb-Expires" => expires_at = v.parse::<i64>().ok(),
                "X-Vtb-Method" => signed_method = Some(v.into_owned()),
                "X-Vtb-Signature" => signature = Some(v.into_owned()),
                _ => {}
            }
        }

        let (Some(exp), Some(m), Some(sig)) = (expires_at, signed_method, signature) else {
            return Ok(false);
        };
        if now_unix > exp || !m.eq_ignore_ascii_case(method.trim()) {
            return Ok(false);
        }
        let canonical = format!("{}\n{}\n{}", m.to_ascii_uppercase(), parsed.path(), exp);
        let expected = hex::encode(hmac_sha256(&self.secret, canonical.as_bytes())?);
        Ok(constant_time_eq(sig.as_bytes(), expected.as_bytes()))
    }

    pub fn verify_url(
        &self,
        signed_url: &str,
        method: &str,
        now_unix: i64,
    ) -> Result<bool, StorageError> {
        self.verify_url_checked(signed_url, method, now_unix)
            .map_err(StorageError::from)
    }
}

#[derive(Clone, Debug)]
pub struct S3SignConfig {
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: Option<String>,
    pub virtual_hosted_style: bool,
}

impl S3SignConfig {
    /// Sign an S3 / Cloudflare R2 / S3-compatible SigV4 presigned URL with RFC 3986 path encoding and `1..=604_800` expiry validation.
    pub fn sign_v4_url(
        &self,
        method: &str,
        object_path: &str,
        expires_in_secs: u64,
        now_unix: i64,
    ) -> Result<String, SigningError> {
        validate_expires_in_secs(expires_in_secs)?;
        let bucket = self.bucket.trim();
        if bucket.is_empty() {
            return Err(SigningError::InvalidInput("empty S3 bucket".to_string()));
        }
        let region = self.region.trim();
        if region.is_empty() {
            return Err(SigningError::InvalidInput("empty S3 region".to_string()));
        }
        if self.access_key_id.trim().is_empty() || self.secret_access_key.is_empty() {
            return Err(SigningError::InvalidKey(
                "missing S3 access_key_id or secret_access_key".to_string(),
            ));
        }
        let trimmed_path = object_path.trim_start_matches('/');
        if trimmed_path.is_empty() {
            return Err(SigningError::InvalidInput(
                "empty S3 object_path".to_string(),
            ));
        }

        let timestamp: DateTime<Utc> = Utc
            .timestamp_opt(now_unix, 0)
            .single()
            .ok_or_else(|| SigningError::InvalidExpiry("invalid unix timestamp".to_string()))?;
        let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
        let datestamp = timestamp.format("%Y%m%d").to_string();

        let base_endpoint = self
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let base_url = Url::parse(&base_endpoint).map_err(|e| {
            SigningError::InvalidInput(format!("invalid S3 endpoint `{base_endpoint}`: {e}"))
        })?;
        self.reject_ip_literal_virtual_host(&base_url)?;
        let host_only = base_url
            .host_str()
            .ok_or_else(|| SigningError::InvalidInput("missing S3 host".to_string()))?;

        let endpoint_prefix = base_url.path().trim_matches('/');
        let encoded_bucket = rfc3986_encode(bucket, true);
        let encoded_object = rfc3986_encode(trimmed_path, false);

        let effective_host_only = if self.virtual_hosted_style {
            let bucket_dot = format!("{encoded_bucket}.");
            if host_only.starts_with(&bucket_dot) {
                host_only.to_string()
            } else {
                format!("{encoded_bucket}.{host_only}")
            }
        } else {
            host_only.to_string()
        };
        let host = match base_url.port() {
            Some(port)
                if (base_url.scheme() == "http" && port != 80)
                    || (base_url.scheme() == "https" && port != 443) =>
            {
                format!("{effective_host_only}:{port}")
            }
            _ => effective_host_only,
        };

        let canonical_uri = if self.virtual_hosted_style {
            if endpoint_prefix.is_empty() {
                format!("/{encoded_object}")
            } else {
                format!(
                    "/{}/{encoded_object}",
                    rfc3986_encode(endpoint_prefix, false)
                )
            }
        } else if endpoint_prefix.is_empty() {
            format!("/{encoded_bucket}/{encoded_object}")
        } else {
            format!(
                "/{}/{encoded_bucket}/{encoded_object}",
                rfc3986_encode(endpoint_prefix, false)
            )
        };
        let full_url = format!("{}://{host}{canonical_uri}", base_url.scheme());

        let credential_scope = format!("{datestamp}/{region}/s3/aws4_request");
        let credential = format!("{}/{credential_scope}", self.access_key_id.trim());

        let mut query_params = [
            (
                "X-Amz-Algorithm".to_string(),
                "AWS4-HMAC-SHA256".to_string(),
            ),
            ("X-Amz-Credential".to_string(), credential),
            ("X-Amz-Date".to_string(), amz_date.clone()),
            ("X-Amz-Expires".to_string(), expires_in_secs.to_string()),
            ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
        ];
        query_params.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_querystring = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", rfc3986_encode(k, true), rfc3986_encode(v, true)))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_request = format!(
            "{}\n{}\n{}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
            method.trim().to_ascii_uppercase(),
            canonical_uri,
            canonical_querystring,
            host
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            credential_scope,
            sha256_hex(canonical_request.as_bytes())
        );

        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_access_key).as_bytes(),
            datestamp.as_bytes(),
        )?;
        let k_region = hmac_sha256(&k_date, region.as_bytes())?;
        let k_service = hmac_sha256(&k_region, b"s3")?;
        let k_signing = hmac_sha256(&k_service, b"aws4_request")?;
        let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes())?);

        Ok(format!(
            "{full_url}?{canonical_querystring}&X-Amz-Signature={signature}"
        ))
    }

    pub fn verify_v4_url(
        &self,
        method: &str,
        signed_url: &str,
        now_unix: i64,
    ) -> Result<bool, SigningError> {
        let parsed = Url::parse(signed_url).map_err(|e| {
            SigningError::InvalidInput(format!("invalid signed URL `{signed_url}`: {e}"))
        })?;
        let params: HashMap<String, String> = parsed
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        let Some(amz_date) = params.get("X-Amz-Date") else {
            return Ok(false);
        };
        let Some(expires_str) = params.get("X-Amz-Expires") else {
            return Ok(false);
        };
        let Some(signature) = params.get("X-Amz-Signature") else {
            return Ok(false);
        };
        let Ok(expires_secs) = expires_str.parse::<u64>() else {
            return Ok(false);
        };
        if validate_expires_in_secs(expires_secs).is_err() {
            return Ok(false);
        }

        let signed_dt = chrono::NaiveDateTime::parse_from_str(amz_date, "%Y%m%dT%H%M%SZ")
            .map_err(|e| SigningError::InvalidInput(format!("invalid X-Amz-Date: {e}")))?;
        let signed_unix = signed_dt.and_utc().timestamp();
        if now_unix > signed_unix.saturating_add(expires_secs as i64) {
            return Ok(false);
        }

        let encoded_bucket = rfc3986_encode(self.bucket.trim(), true);
        let parsed_endpoint = self
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|ep| !ep.is_empty())
            .and_then(|ep| Url::parse(ep).ok());
        if let Some(ref endpoint_url) = parsed_endpoint {
            self.reject_ip_literal_virtual_host(endpoint_url)?;
        }
        let endpoint_prefix = parsed_endpoint
            .map(|u| u.path().trim_matches('/').to_string())
            .unwrap_or_default();
        let path_prefix = if self.virtual_hosted_style {
            if endpoint_prefix.is_empty() {
                "/".to_string()
            } else {
                format!("/{}/", rfc3986_encode(&endpoint_prefix, false))
            }
        } else if endpoint_prefix.is_empty() {
            format!("/{encoded_bucket}/")
        } else {
            format!(
                "/{}/{encoded_bucket}/",
                rfc3986_encode(&endpoint_prefix, false)
            )
        };
        let Some(raw_encoded_key) = parsed.path().strip_prefix(&path_prefix) else {
            return Ok(false);
        };
        if raw_encoded_key.is_empty() {
            return Ok(false);
        }
        let decoded_key = percent_decode_utf8(raw_encoded_key)?;

        let regenerated = self.sign_v4_url(method, &decoded_key, expires_secs, signed_unix)?;
        let regen_parsed = Url::parse(&regenerated).map_err(|e| {
            SigningError::InvalidInput(format!("invalid regenerated URL `{regenerated}`: {e}"))
        })?;
        if parsed.scheme() != regen_parsed.scheme()
            || parsed.host_str() != regen_parsed.host_str()
            || parsed.port_or_known_default() != regen_parsed.port_or_known_default()
            || parsed.path() != regen_parsed.path()
        {
            return Ok(false);
        }
        let expected_sig = regen_parsed
            .query_pairs()
            .find(|(k, _)| k == "X-Amz-Signature")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default();

        Ok(constant_time_eq(
            signature.as_bytes(),
            expected_sig.as_bytes(),
        ))
    }

    /// Reject IPv4/IPv6 literal endpoint hosts when `virtual_hosted_style` is enabled, since a bucket subdomain cannot be prefixed to an IP address.
    fn reject_ip_literal_virtual_host(&self, endpoint: &Url) -> Result<(), SigningError> {
        if self.virtual_hosted_style
            && matches!(
                endpoint.host(),
                Some(url::Host::Ipv4(_) | url::Host::Ipv6(_))
            )
        {
            return Err(SigningError::InvalidInput(format!(
                "virtual_hosted_style requires a DNS endpoint host; IP-literal endpoint `{endpoint}` cannot carry a bucket subdomain"
            )));
        }
        Ok(())
    }
}

pub fn sign_s3_v4_url(
    config: &S3SignConfig,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
) -> Result<String, StorageError> {
    config
        .sign_v4_url(method, object_path, expires_in_secs, now_unix)
        .map_err(StorageError::from)
}

pub fn verify_s3_v4_url(
    config: &S3SignConfig,
    method: &str,
    signed_url: &str,
    now_unix: i64,
) -> Result<bool, StorageError> {
    config
        .verify_v4_url(method, signed_url, now_unix)
        .map_err(StorageError::from)
}

#[derive(Clone, Debug)]
pub struct GcsSignConfig {
    pub bucket: String,
    pub service_account_email: String,
    pub private_key_pem: Option<String>,
    pub access_token: Option<String>,
    pub endpoint: Option<String>,
}

/// Deterministic GCS V4 canonical request and string-to-sign components (`GOOG4-RSA-SHA256`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcsV4CanonicalRequest {
    pub canonical_uri: String,
    pub canonical_querystring: String,
    pub canonical_request: String,
    pub string_to_sign: String,
    pub base_url: String,
}

pub(crate) fn resolve_gcs_endpoint_origin(
    endpoint: Option<&str>,
) -> Result<(String, String, String), SigningError> {
    let Some(raw) = endpoint.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok((
            "https://storage.googleapis.com".to_string(),
            "storage.googleapis.com".to_string(),
            String::new(),
        ));
    };
    let parsed = Url::parse(raw)
        .map_err(|e| SigningError::InvalidInput(format!("invalid GCS endpoint `{raw}`: {e}")))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(SigningError::InvalidInput(format!(
            "GCS endpoint `{raw}` must use http:// or https://"
        )));
    }
    let host_only = parsed.host_str().filter(|h| !h.is_empty()).ok_or_else(|| {
        SigningError::InvalidInput(format!("missing host in GCS endpoint `{raw}`"))
    })?;
    let host = match parsed.port() {
        Some(port)
            if (parsed.scheme() == "http" && port != 80)
                || (parsed.scheme() == "https" && port != 443) =>
        {
            format!("{host_only}:{port}")
        }
        _ => host_only.to_string(),
    };
    let prefix = parsed.path().trim_matches('/').to_string();
    let base_url = format!("{}://{host}", parsed.scheme());
    Ok((base_url, host, prefix))
}

/// Build the GCS V4 (`GOOG4-RSA-SHA256`) canonical request and string-to-sign without requiring a local private key.
pub fn build_gcs_v4_canonical_request(
    bucket: &str,
    service_account_email: &str,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
    endpoint: Option<&str>,
) -> Result<GcsV4CanonicalRequest, SigningError> {
    validate_expires_in_secs(expires_in_secs)?;
    let trimmed_bucket = bucket.trim();
    if trimmed_bucket.is_empty() {
        return Err(SigningError::InvalidInput("empty GCS bucket".to_string()));
    }
    let trimmed_email = service_account_email.trim();
    if trimmed_email.is_empty() {
        return Err(SigningError::InvalidKey(
            "missing GCS service_account_email".to_string(),
        ));
    }
    let trimmed_object = object_path.trim_start_matches('/');
    if trimmed_object.is_empty() {
        return Err(SigningError::InvalidInput(
            "empty GCS object_path".to_string(),
        ));
    }

    let (base_url, host, endpoint_prefix) = resolve_gcs_endpoint_origin(endpoint)?;

    let timestamp: DateTime<Utc> = Utc
        .timestamp_opt(now_unix, 0)
        .single()
        .ok_or_else(|| SigningError::InvalidExpiry("invalid unix timestamp".to_string()))?;
    let goog_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = timestamp.format("%Y%m%d").to_string();

    let encoded_bucket = rfc3986_encode(trimmed_bucket, true);
    let encoded_object = rfc3986_encode(trimmed_object, false);
    let canonical_uri = if endpoint_prefix.is_empty() {
        format!("/{encoded_bucket}/{encoded_object}")
    } else {
        format!(
            "/{}/{encoded_bucket}/{encoded_object}",
            rfc3986_encode(&endpoint_prefix, false)
        )
    };
    let credential_scope = format!("{datestamp}/auto/storage/goog4_request");
    let credential = format!("{trimmed_email}/{credential_scope}");

    let mut query_params = [
        (
            "X-Goog-Algorithm".to_string(),
            "GOOG4-RSA-SHA256".to_string(),
        ),
        ("X-Goog-Credential".to_string(), credential),
        ("X-Goog-Date".to_string(), goog_date.clone()),
        ("X-Goog-Expires".to_string(), expires_in_secs.to_string()),
        ("X-Goog-SignedHeaders".to_string(), "host".to_string()),
    ];
    query_params.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_querystring = query_params
        .iter()
        .map(|(k, v)| format!("{}={}", rfc3986_encode(k, true), rfc3986_encode(v, true)))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_request = format!(
        "{}\n{}\n{}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
        method.trim().to_ascii_uppercase(),
        canonical_uri,
        canonical_querystring,
        host
    );
    let string_to_sign = format!(
        "GOOG4-RSA-SHA256\n{}\n{}\n{}",
        goog_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );

    Ok(GcsV4CanonicalRequest {
        canonical_uri,
        canonical_querystring,
        canonical_request,
        string_to_sign,
        base_url,
    })
}

/// Assemble a final GCS V4 presigned URL from a `GcsV4CanonicalRequest` and an IAM `signBlob` hex signature.
pub fn assemble_gcs_iam_v4_url(
    canonical: &GcsV4CanonicalRequest,
    signature_hex: &str,
) -> Result<String, SigningError> {
    let trimmed = signature_hex.trim();
    if trimmed.is_empty() {
        return Err(SigningError::InvalidKey(
            "GCS V4 signature hex must not be empty".to_string(),
        ));
    }
    let sig_bytes = hex::decode(trimmed)
        .map_err(|e| SigningError::InvalidKey(format!("invalid GCS V4 signature hex: {e}")))?;
    let normalized_hex = hex::encode(sig_bytes);
    let origin = if canonical.base_url.trim().is_empty() {
        "https://storage.googleapis.com"
    } else {
        canonical.base_url.trim_end_matches('/')
    };
    Ok(format!(
        "{origin}{}?{}&X-Goog-Signature={normalized_hex}",
        canonical.canonical_uri, canonical.canonical_querystring
    ))
}

/// Sign a GCS V4 presigned URL locally using a PKCS#8 or PKCS#1 RSA private key PEM (`GOOG4-RSA-SHA256`).
pub fn sign_gcs_v4_url(
    config: &GcsSignConfig,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
) -> Result<String, SigningError> {
    let pem = config
        .private_key_pem
        .as_deref()
        .ok_or_else(|| SigningError::InvalidKey("missing GCS RSA private key PEM".to_string()))?;
    let rsa_key = parse_rsa_private_key(pem)?;
    let canonical = build_gcs_v4_canonical_request(
        &config.bucket,
        &config.service_account_email,
        method,
        object_path,
        expires_in_secs,
        now_unix,
        config.endpoint.as_deref(),
    )?;
    let signing_key = SigningKey::<Sha256>::new(rsa_key);
    let signature = signing_key.sign(canonical.string_to_sign.as_bytes());
    let hex_sig = hex::encode(signature.to_bytes());
    assemble_gcs_iam_v4_url(&canonical, &hex_sig)
}

#[derive(Serialize)]
struct IamSignBlobRequest {
    payload: String,
}

#[derive(Deserialize)]
struct IamSignBlobResponse {
    #[serde(rename = "signedBlob")]
    signed_blob: String,
}

static IAM_LOOPBACK_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_default()
});

static IAM_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_default()
});

fn is_loopback_iam_endpoint(iam_base_url: &str) -> bool {
    let Ok(parsed) = Url::parse(iam_base_url.trim()) else {
        return false;
    };
    matches!(
        parsed.host_str(),
        Some("127.0.0.1" | "localhost" | "::1" | "[::1]")
    )
}

fn select_iam_http_client(iam_base_url: &str) -> &'static reqwest::Client {
    if is_loopback_iam_endpoint(iam_base_url) {
        &IAM_LOOPBACK_CLIENT
    } else {
        &IAM_HTTP_CLIENT
    }
}

static SIGNING_TOKEN_CACHES: LazyLock<RwLock<HashMap<String, Arc<GcpTokenCache>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

fn get_or_create_signing_token_cache(
    credentials_options: Option<&HashMap<String, String>>,
    iam_base_url: &str,
) -> Option<Arc<GcpTokenCache>> {
    if let Some(opts) = credentials_options {
        let mut pairs: Vec<(&str, &str)> =
            opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
        let cache_key = format!(
            "opts:{}",
            pairs
                .into_iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&")
        );
        if let Some(existing) = SIGNING_TOKEN_CACHES
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&cache_key)
        {
            return Some(Arc::clone(existing));
        }
        let created = GcpTokenCache::from_options(opts, crate::gcp_auth::GCS_OAUTH_SCOPES)?;
        let mut guard = SIGNING_TOKEN_CACHES
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let entry = guard
            .entry(cache_key)
            .or_insert_with(|| Arc::clone(&created));
        return Some(Arc::clone(entry));
    }

    if !iam_base_url.contains("googleapis.com") {
        return None;
    }

    let ambient_key = format!(
        "ambient:{:?}:{:?}:{:?}:{:?}:{:?}",
        std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
        std::env::var("CLOUDSDK_CONFIG").ok(),
        std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok(),
        std::env::var("CLOUDSDK_AUTH_ACCESS_TOKEN").ok(),
        std::env::var("GCE_METADATA_HOST").ok(),
    );
    if let Some(existing) = SIGNING_TOKEN_CACHES
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&ambient_key)
    {
        return Some(Arc::clone(existing));
    }
    let created = GcpTokenCache::from_options(&HashMap::new(), crate::gcp_auth::GCS_OAUTH_SCOPES)?;
    let mut guard = SIGNING_TOKEN_CACHES
        .write()
        .unwrap_or_else(|e| e.into_inner());
    let entry = guard
        .entry(ambient_key)
        .or_insert_with(|| Arc::clone(&created));
    Some(Arc::clone(entry))
}

#[allow(clippy::too_many_arguments)]
pub async fn sign_gcs_v4_with_options_and_handle_checked(
    config: &GcsSignConfig,
    credentials_options: Option<&HashMap<String, String>>,
    token_cache: Option<&SharedGcpTokenCacheHandle>,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
    iam_base_url: &str,
) -> Result<String, SigningError> {
    let mut local_sa_identity: Option<(String, String)> = config
        .private_key_pem
        .as_ref()
        .map(|pem| (config.service_account_email.clone(), pem.clone()));

    if local_sa_identity.is_none() && config.access_token.is_none() {
        let has_target_sa_override = credentials_options.is_some_and(|opts| {
            opts.get("target_service_account")
                .or_else(|| opts.get("google_target_service_account"))
                .or_else(|| opts.get("gcs_target_service_account"))
                .is_some_and(|v| !v.trim().is_empty())
        });
        if !has_target_sa_override {
            let discovered_key = if let Some(cache) = token_cache {
                cache.service_account_key()
            } else {
                match credentials_options {
                    Some(opts) => {
                        let allow_ambient = !opts
                            .get("allow_ambient_adc")
                            .is_some_and(|v| v.eq_ignore_ascii_case("false") || v == "0");
                        extract_adc_service_account_key(opts, allow_ambient)
                    }
                    None => extract_adc_service_account_key(&HashMap::new(), true),
                }
            };
            if let Some(sa_key) = discovered_key
                && (config.service_account_email.is_empty()
                    || sa_key.client_email == config.service_account_email)
            {
                local_sa_identity = Some((sa_key.client_email, sa_key.private_key_pem));
            }
        }
    }

    if let Some((email, pem)) = local_sa_identity {
        let local_cfg = GcsSignConfig {
            bucket: config.bucket.clone(),
            service_account_email: email,
            private_key_pem: Some(pem),
            access_token: None,
            endpoint: config.endpoint.clone(),
        };
        return sign_gcs_v4_url(&local_cfg, method, object_path, expires_in_secs, now_unix);
    }

    let canonical = build_gcs_v4_canonical_request(
        &config.bucket,
        &config.service_account_email,
        method,
        object_path,
        expires_in_secs,
        now_unix,
        config.endpoint.as_deref(),
    )?;

    let url_endpoint = format!(
        "{}/v1/projects/-/serviceAccounts/{}:signBlob",
        iam_base_url.trim_end_matches('/'),
        config.service_account_email.trim()
    );
    let client = select_iam_http_client(iam_base_url);
    let payload_b64 = BASE64_STANDARD.encode(canonical.string_to_sign.as_bytes());

    let resolved_cache = if config.access_token.is_some() {
        None
    } else if let Some(cache) = token_cache {
        Some(cache.clone())
    } else {
        get_or_create_signing_token_cache(credentials_options, iam_base_url)
            .map(SharedGcpTokenCacheHandle::from_arc)
    };

    let resp = if let Some(ref cache) = resolved_cache {
        let snap = cache
            .get_or_refresh()
            .await
            .map_err(SigningError::RemoteSigning)?;
        let mut first_req = client
            .post(&url_endpoint)
            .bearer_auth(&snap.access_token)
            .json(&IamSignBlobRequest {
                payload: payload_b64.clone(),
            });
        if let Some(ref qp) = snap.quota_project_id
            && !qp.trim().is_empty()
        {
            first_req = first_req.header("x-goog-user-project", qp.trim());
        }
        let first_resp = first_req.send().await.map_err(|e| {
            SigningError::RemoteSigning(format!("IAM signBlob request failed: {e}"))
        })?;
        if first_resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            cache.invalidate(snap.generation);
            let refreshed = cache
                .get_or_refresh()
                .await
                .map_err(SigningError::RemoteSigning)?;
            let mut retry_req = client
                .post(&url_endpoint)
                .bearer_auth(&refreshed.access_token)
                .json(&IamSignBlobRequest {
                    payload: payload_b64,
                });
            if let Some(ref qp) = refreshed.quota_project_id
                && !qp.trim().is_empty()
            {
                retry_req = retry_req.header("x-goog-user-project", qp.trim());
            }
            retry_req.send().await.map_err(|e| {
                SigningError::RemoteSigning(format!("IAM signBlob request failed: {e}"))
            })?
        } else {
            first_resp
        }
    } else {
        let mut req = client.post(&url_endpoint).json(&IamSignBlobRequest {
            payload: payload_b64,
        });
        if let Some(ref token) = config.access_token {
            req = req.bearer_auth(token);
        }
        req.send()
            .await
            .map_err(|e| SigningError::RemoteSigning(format!("IAM signBlob request failed: {e}")))?
    };

    if !resp.status().is_success() {
        return Err(SigningError::RemoteSigning(format!(
            "IAM signBlob returned HTTP {}",
            resp.status()
        )));
    }
    let parsed: IamSignBlobResponse = resp
        .json()
        .await
        .map_err(|e| SigningError::RemoteSigning(format!("invalid IAM signBlob JSON: {e}")))?;
    let raw_sig = BASE64_STANDARD
        .decode(parsed.signed_blob.trim().as_bytes())
        .map_err(|e| {
            SigningError::InvalidKey(format!("invalid IAM signBlob base64 signature: {e}"))
        })?;
    let hex_sig = hex::encode(raw_sig);
    assemble_gcs_iam_v4_url(&canonical, &hex_sig)
}

#[allow(clippy::too_many_arguments)]
pub async fn sign_gcs_v4_with_options_and_cache_checked(
    config: &GcsSignConfig,
    credentials_options: Option<&HashMap<String, String>>,
    token_cache: Option<&Arc<GcpTokenCache>>,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
    iam_base_url: &str,
) -> Result<String, SigningError> {
    let handle = token_cache
        .cloned()
        .map(SharedGcpTokenCacheHandle::from_arc);
    sign_gcs_v4_with_options_and_handle_checked(
        config,
        credentials_options,
        handle.as_ref(),
        method,
        object_path,
        expires_in_secs,
        now_unix,
        iam_base_url,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn sign_gcs_v4_with_options_and_cache(
    config: &GcsSignConfig,
    credentials_options: Option<&HashMap<String, String>>,
    token_cache: Option<&Arc<GcpTokenCache>>,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
    iam_base_url: &str,
) -> Result<String, StorageError> {
    sign_gcs_v4_with_options_and_cache_checked(
        config,
        credentials_options,
        token_cache,
        method,
        object_path,
        expires_in_secs,
        now_unix,
        iam_base_url,
    )
    .await
    .map_err(StorageError::from)
}

pub async fn sign_gcs_v4_with_iam_endpoint_and_cache(
    config: &GcsSignConfig,
    token_cache: Option<&SharedGcpTokenCacheHandle>,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
    iam_base_url: &str,
) -> Result<String, StorageError> {
    sign_gcs_v4_with_options_and_handle_checked(
        config,
        None,
        token_cache,
        method,
        object_path,
        expires_in_secs,
        now_unix,
        iam_base_url,
    )
    .await
    .map_err(StorageError::from)
}

pub async fn sign_gcs_v4_with_iam_endpoint(
    config: &GcsSignConfig,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
    iam_base_url: &str,
) -> Result<String, StorageError> {
    sign_gcs_v4_with_iam_endpoint_and_cache(
        config,
        None,
        method,
        object_path,
        expires_in_secs,
        now_unix,
        iam_base_url,
    )
    .await
}

#[derive(Clone, Debug)]
pub struct AzureSasConfig {
    pub account: String,
    pub container: String,
    pub access_key_b64: String,
    pub endpoint: Option<String>,
}

impl AzureSasConfig {
    /// Generate a zero-I/O Azure Shared Key Blob SAS URL (`sv=2020-12-06`) with strict base64 key and expiry bounds validation.
    pub fn sign_sas_url(
        &self,
        method: &str,
        blob_path: &str,
        expires_in_secs: u64,
        now_unix: i64,
    ) -> Result<String, SigningError> {
        validate_expires_in_secs(expires_in_secs)?;
        let account = self.account.trim();
        if account.is_empty() {
            return Err(SigningError::InvalidInput(
                "empty Azure Storage account".to_string(),
            ));
        }
        let container = self.container.trim();
        if container.is_empty() {
            return Err(SigningError::InvalidInput(
                "empty Azure Blob container".to_string(),
            ));
        }
        let trimmed_blob = blob_path.trim_start_matches('/');
        if trimmed_blob.is_empty() {
            return Err(SigningError::InvalidInput(
                "empty Azure blob_path".to_string(),
            ));
        }
        let key_b64 = self.access_key_b64.trim();
        if key_b64.is_empty() {
            return Err(SigningError::InvalidKey(
                "Azure account access_key_b64 must not be empty".to_string(),
            ));
        }
        let raw_key = BASE64_STANDARD.decode(key_b64.as_bytes()).map_err(|e| {
            SigningError::InvalidKey(format!("invalid Azure Shared Key base64: {e}"))
        })?;
        if raw_key.is_empty() {
            return Err(SigningError::InvalidKey(
                "decoded Azure Shared Key must not be empty".to_string(),
            ));
        }

        let start_dt: DateTime<Utc> = Utc
            .timestamp_opt(now_unix, 0)
            .single()
            .ok_or_else(|| SigningError::InvalidExpiry("invalid unix timestamp".to_string()))?;
        let expiry_unix = now_unix
            .checked_add(expires_in_secs as i64)
            .ok_or_else(|| SigningError::InvalidExpiry("expiry timestamp overflow".to_string()))?;
        let expiry_dt: DateTime<Utc> = Utc
            .timestamp_opt(expiry_unix, 0)
            .single()
            .ok_or_else(|| SigningError::InvalidExpiry("invalid expiry timestamp".to_string()))?;
        let st = start_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let se = expiry_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let sp = match method.trim().to_ascii_uppercase().as_str() {
            "PUT" | "POST" => "cw",
            "DELETE" => "d",
            _ => "r",
        };
        let sv = "2020-12-06";
        let sr = "b";
        let canonicalized_resource = format!("/blob/{account}/{container}/{trimmed_blob}");
        let string_to_sign = format!(
            "{sp}\n{st}\n{se}\n{canonicalized_resource}\n\n\nhttps\n{sv}\n{sr}\n\n\n\n\n\n"
        );
        let sig_bytes = hmac_sha256(&raw_key, string_to_sign.as_bytes())?;
        let sig_b64 = BASE64_STANDARD.encode(sig_bytes);

        let base_endpoint = self
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("https://{account}.blob.core.windows.net"));
        let encoded_container = rfc3986_encode(container, true);
        let encoded_blob = rfc3986_encode(trimmed_blob, false);
        let query = format!(
            "sv={}&st={}&se={}&sr={sr}&sp={sp}&spr=https&sig={}",
            rfc3986_encode(sv, true),
            rfc3986_encode(&st, true),
            rfc3986_encode(&se, true),
            rfc3986_encode(&sig_b64, true)
        );
        Ok(format!(
            "{}/{encoded_container}/{encoded_blob}?{query}",
            base_endpoint.trim_end_matches('/')
        ))
    }
}

/// Generate a zero-I/O Azure Shared Key Blob SAS URL (`sv=2020-12-06`).
pub fn sign_azure_sas_url(
    config: &AzureSasConfig,
    method: &str,
    blob_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
) -> Result<String, StorageError> {
    config
        .sign_sas_url(method, blob_path, expires_in_secs, now_unix)
        .map_err(StorageError::from)
}

#[derive(Clone, Debug)]
pub struct AwsSigV4HeaderConfig {
    pub service: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

pub fn sign_aws_sigv4_headers(
    config: &AwsSigV4HeaderConfig,
    method: &str,
    url: &str,
    payload: &[u8],
    content_type: &str,
    now_unix: i64,
) -> Result<HashMap<String, String>, StorageError> {
    let parsed =
        Url::parse(url).map_err(|e| StorageError::InvalidUrl(url.to_string(), e.to_string()))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| StorageError::Signing("missing host in url".to_string()))?
        .to_string();

    let host_header = match parsed.port() {
        Some(port)
            if (parsed.scheme() == "http" && port != 80)
                || (parsed.scheme() == "https" && port != 443) =>
        {
            format!("{host}:{port}")
        }
        _ => host,
    };

    let timestamp: DateTime<Utc> = Utc
        .timestamp_opt(now_unix, 0)
        .single()
        .ok_or_else(|| StorageError::Signing("invalid unix timestamp".to_string()))?;
    let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = timestamp.format("%Y%m%d").to_string();
    let payload_hash = sha256_hex(payload);

    let mut canonical_headers = format!(
        "content-type:{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        content_type.trim(),
        host_header.trim(),
        payload_hash,
        amz_date,
    );
    let signed_headers = if let Some(ref token) = config.session_token {
        canonical_headers.push_str(&format!("x-amz-security-token:{}\n", token.trim()));
        "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token".to_string()
    } else {
        "content-type;host;x-amz-content-sha256;x-amz-date".to_string()
    };

    let canonical_uri = if parsed.path().is_empty() {
        "/".to_string()
    } else {
        parsed.path().to_string()
    };
    let canonical_querystring = parsed.query().unwrap_or("");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        canonical_uri,
        canonical_querystring,
        canonical_headers,
        signed_headers,
        payload_hash
    );

    let credential_scope = format!(
        "{datestamp}/{}/{}/aws4_request",
        config.region, config.service
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", config.secret_access_key).as_bytes(),
        datestamp.as_bytes(),
    )?;
    let k_region = hmac_sha256(&k_date, config.region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, config.service.as_bytes())?;
    let k_signing = hmac_sha256(&k_service, b"aws4_request")?;
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes())?);

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
        config.access_key_id
    );

    let mut headers = HashMap::new();
    headers.insert("Authorization".to_string(), auth_header);
    headers.insert("content-type".to_string(), content_type.to_string());
    headers.insert("host".to_string(), host_header);
    headers.insert("x-amz-date".to_string(), amz_date);
    headers.insert("x-amz-content-sha256".to_string(), payload_hash);
    if let Some(ref token) = config.session_token {
        headers.insert("x-amz-security-token".to_string(), token.clone());
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_aws_sigv4_headers_ses_v2() {
        let config = AwsSigV4HeaderConfig {
            service: "ses".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: Some("session-token-test".to_string()),
        };
        let payload = br#"{"FromEmailAddress":"sender@example.com"}"#;
        let headers = sign_aws_sigv4_headers(
            &config,
            "POST",
            "https://email.us-east-1.amazonaws.com/v2/email/outbound-emails",
            payload,
            "application/json",
            1_700_000_000,
        )
        .expect("signing must succeed");

        assert_eq!(
            headers.get("host").expect("host header"),
            "email.us-east-1.amazonaws.com"
        );
        assert_eq!(
            headers.get("x-amz-date").expect("x-amz-date header"),
            "20231114T221320Z"
        );
        assert_eq!(
            headers
                .get("x-amz-security-token")
                .expect("security token header"),
            "session-token-test"
        );
        let auth = headers.get("Authorization").expect("Authorization header");
        assert!(auth.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20231114/us-east-1/ses/aws4_request"
        ));
        assert!(auth.contains(
            "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
        ));
        assert!(auth.contains("Signature="));
    }
}
