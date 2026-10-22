//! Presigned URL signing and verification for HMAC-SHA256, S3 SigV4, and GCS V4 (with keyless IAM signBlob).

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, TimeZone, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use url::Url;
use vertebra_core::util::{constant_time_eq, sha256_hex};

use crate::store::StorageError;

type HmacSha256 = Hmac<Sha256>;

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Vec<u8>, StorageError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| StorageError::Signing(format!("invalid HMAC key length: {e}")))?;
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

    pub fn sign_url(
        &self,
        base_url: &str,
        method: &str,
        expires_in_secs: u64,
        now_unix: i64,
    ) -> Result<String, StorageError> {
        let mut url = Url::parse(base_url)
            .map_err(|e| StorageError::InvalidUrl(base_url.to_string(), e.to_string()))?;
        let expires_at = now_unix + expires_in_secs as i64;
        let canonical = format!(
            "{}\n{}\n{}",
            method.to_ascii_uppercase(),
            url.path(),
            expires_at
        );
        let sig = hex::encode(hmac_sha256(&self.secret, canonical.as_bytes())?);
        url.query_pairs_mut()
            .append_pair("X-Vtb-Expires", &expires_at.to_string())
            .append_pair("X-Vtb-Method", &method.to_ascii_uppercase())
            .append_pair("X-Vtb-Signature", &sig);
        Ok(url.into())
    }

    pub fn verify_url(
        &self,
        signed_url: &str,
        method: &str,
        now_unix: i64,
    ) -> Result<bool, StorageError> {
        let parsed = Url::parse(signed_url)
            .map_err(|e| StorageError::InvalidUrl(signed_url.to_string(), e.to_string()))?;
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
        if now_unix > exp || !m.eq_ignore_ascii_case(method) {
            return Ok(false);
        }
        let canonical = format!("{}\n{}\n{}", m.to_ascii_uppercase(), parsed.path(), exp);
        let expected = hex::encode(hmac_sha256(&self.secret, canonical.as_bytes())?);
        Ok(constant_time_eq(sig.as_bytes(), expected.as_bytes()))
    }
}

#[derive(Clone, Debug)]
pub struct S3SignConfig {
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: Option<String>,
}

pub fn sign_s3_v4_url(
    config: &S3SignConfig,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
) -> Result<String, StorageError> {
    let timestamp: DateTime<Utc> = Utc
        .timestamp_opt(now_unix, 0)
        .single()
        .ok_or_else(|| StorageError::Signing("invalid unix timestamp".to_string()))?;
    let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = timestamp.format("%Y%m%d").to_string();

    let base_endpoint = config
        .endpoint
        .clone()
        .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", config.region));
    let trimmed_path = object_path.trim_start_matches('/');
    let full_url = format!(
        "{}/{}/{}",
        base_endpoint.trim_end_matches('/'),
        config.bucket,
        trimmed_path
    );
    let parsed = Url::parse(&full_url)
        .map_err(|e| StorageError::InvalidUrl(full_url.clone(), e.to_string()))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| StorageError::Signing("missing S3 host".to_string()))?
        .to_string();

    let credential_scope = format!("{}/{}/s3/aws4_request", datestamp, config.region);
    let credential = format!("{}/{}", config.access_key_id, credential_scope);

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
        .map(|(k, v)| format!("{}={}", urlencoding_encode(k), urlencoding_encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_request = format!(
        "{}\n{}\n{}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
        method.to_ascii_uppercase(),
        parsed.path(),
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
        format!("AWS4{}", config.secret_access_key).as_bytes(),
        datestamp.as_bytes(),
    )?;
    let k_region = hmac_sha256(&k_date, config.region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, b"s3")?;
    let k_signing = hmac_sha256(&k_service, b"aws4_request")?;
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes())?);

    Ok(format!(
        "{}?{}&X-Amz-Signature={}",
        full_url, canonical_querystring, signature
    ))
}

pub fn verify_s3_v4_url(
    config: &S3SignConfig,
    method: &str,
    signed_url: &str,
    now_unix: i64,
) -> Result<bool, StorageError> {
    let parsed = Url::parse(signed_url)
        .map_err(|e| StorageError::InvalidUrl(signed_url.to_string(), e.to_string()))?;
    let params: std::collections::HashMap<String, String> = parsed
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

    let signed_dt = chrono::NaiveDateTime::parse_from_str(amz_date, "%Y%m%dT%H%M%SZ")
        .map_err(|e| StorageError::Signing(format!("invalid X-Amz-Date: {e}")))?;
    let signed_unix = signed_dt.and_utc().timestamp();
    if now_unix > signed_unix + expires_secs as i64 {
        return Ok(false);
    }

    let prefix = format!("/{}/", config.bucket);
    let object_path = parsed.path().strip_prefix(&prefix).unwrap_or(parsed.path());
    let regenerated = sign_s3_v4_url(config, method, object_path, expires_secs, signed_unix)?;
    let regen_parsed = Url::parse(&regenerated)
        .map_err(|e| StorageError::InvalidUrl(regenerated.clone(), e.to_string()))?;
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

#[derive(Clone, Debug)]
pub struct GcsSignConfig {
    pub bucket: String,
    pub service_account_email: String,
    pub private_key_pem: Option<String>,
    pub access_token: Option<String>,
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

pub async fn sign_gcs_v4_with_iam_endpoint(
    config: &GcsSignConfig,
    method: &str,
    object_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
    iam_base_url: &str,
) -> Result<String, StorageError> {
    let timestamp: DateTime<Utc> = Utc
        .timestamp_opt(now_unix, 0)
        .single()
        .ok_or_else(|| StorageError::Signing("invalid unix timestamp".to_string()))?;
    let goog_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = timestamp.format("%Y%m%d").to_string();

    let trimmed = object_path.trim_start_matches('/');
    let canonical_uri = format!("/{}/{}", config.bucket, trimmed);
    let credential_scope = format!("{datestamp}/auto/storage/goog4_request");
    let credential = format!("{}/{}", config.service_account_email, credential_scope);

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
        .map(|(k, v)| format!("{}={}", urlencoding_encode(k), urlencoding_encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_request = format!(
        "{}\n{}\n{}\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD",
        method.to_ascii_uppercase(),
        canonical_uri,
        canonical_querystring
    );
    let string_to_sign = format!(
        "GOOG4-RSA-SHA256\n{}\n{}\n{}",
        goog_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let url_endpoint = format!(
        "{}/v1/projects/-/serviceAccounts/{}:signBlob",
        iam_base_url.trim_end_matches('/'),
        config.service_account_email
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap_or_default();
    let mut req = client.post(&url_endpoint).json(&IamSignBlobRequest {
        payload: BASE64_STANDARD.encode(string_to_sign.as_bytes()),
    });
    if let Some(ref token) = config.access_token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| StorageError::Signing(format!("IAM signBlob request failed: {e}")))?;
    if !resp.status().is_success() {
        return Err(StorageError::Signing(format!(
            "IAM signBlob returned HTTP {}",
            resp.status()
        )));
    }
    let parsed: IamSignBlobResponse = resp
        .json()
        .await
        .map_err(|e| StorageError::Signing(format!("invalid IAM signBlob JSON: {e}")))?;
    let raw_sig = BASE64_STANDARD
        .decode(parsed.signed_blob.as_bytes())
        .unwrap_or_else(|_| parsed.signed_blob.into_bytes());
    let hex_sig = hex::encode(raw_sig);

    Ok(format!(
        "https://storage.googleapis.com{}?{}&X-Goog-Signature={}",
        canonical_uri, canonical_querystring, hex_sig
    ))
}

#[derive(Clone, Debug)]
pub struct AzureSasConfig {
    pub account: String,
    pub container: String,
    pub access_key_b64: String,
    pub endpoint: Option<String>,
}

/// Generate a zero-I/O Azure Shared Key Blob SAS URL (`sv=2020-12-06`).
pub fn sign_azure_sas_url(
    config: &AzureSasConfig,
    method: &str,
    blob_path: &str,
    expires_in_secs: u64,
    now_unix: i64,
) -> Result<String, StorageError> {
    let start_dt: DateTime<Utc> = Utc
        .timestamp_opt(now_unix, 0)
        .single()
        .ok_or_else(|| StorageError::Signing("invalid unix timestamp".to_string()))?;
    let expiry_dt: DateTime<Utc> = Utc
        .timestamp_opt(now_unix + expires_in_secs as i64, 0)
        .single()
        .ok_or_else(|| StorageError::Signing("invalid expiry timestamp".to_string()))?;
    let st = start_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let se = expiry_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let sp = match method.to_ascii_uppercase().as_str() {
        "PUT" | "POST" => "cw",
        "DELETE" => "d",
        _ => "r",
    };
    let sv = "2020-12-06";
    let sr = "b";
    let trimmed_blob = blob_path.trim_start_matches('/');
    let canonicalized_resource = format!(
        "/blob/{}/{}/{}",
        config.account, config.container, trimmed_blob
    );
    let string_to_sign =
        format!("{sp}\n{st}\n{se}\n{canonicalized_resource}\n\n\nhttps\n{sv}\n{sr}\n\n\n\n\n\n");
    let raw_key = BASE64_STANDARD
        .decode(config.access_key_b64.as_bytes())
        .unwrap_or_else(|_| config.access_key_b64.as_bytes().to_vec());
    let sig_bytes = hmac_sha256(&raw_key, string_to_sign.as_bytes())?;
    let sig_b64 = BASE64_STANDARD.encode(sig_bytes);

    let base_endpoint = config
        .endpoint
        .clone()
        .unwrap_or_else(|| format!("https://{}.blob.core.windows.net", config.account));
    let query = format!(
        "sv={}&st={}&se={}&sr={}&sp={}&spr=https&sig={}",
        urlencoding_encode(sv),
        urlencoding_encode(&st),
        urlencoding_encode(&se),
        sr,
        sp,
        urlencoding_encode(&sig_b64)
    );
    Ok(format!(
        "{}/{}/{}?{}",
        base_endpoint.trim_end_matches('/'),
        config.container,
        trimmed_blob,
        query
    ))
}

fn urlencoding_encode(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}
