//! Request payload size enforcement (`413 Payload Too Large`) and bounded `gzip`/`br`/`zstd`/`deflate` decompression.

use std::io::Read;

use bytes::Bytes;
use http::StatusCode;

/// Configurable byte limits for incoming HTTP request payloads.
#[derive(Debug, Clone, Copy)]
pub struct PayloadLimits {
    /// Maximum compressed or uncompressed wire payload size in bytes (default: 10 MiB).
    pub max_body_size: usize,
    /// Maximum decompressed payload size in bytes to prevent zip/zstd bombs (default: 32 MiB).
    pub max_decompressed_size: usize,
}

impl Default for PayloadLimits {
    fn default() -> Self {
        Self {
            max_body_size: 10 * 1024 * 1024,
            max_decompressed_size: 32 * 1024 * 1024,
        }
    }
}

/// Errors produced during request payload validation and decompression.
#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    #[error("request payload length {actual} exceeds limit {limit}")]
    TooLarge { actual: usize, limit: usize },
    #[error("decompressed request payload exceeds limit {limit}")]
    DecompressedTooLarge { limit: usize },
    #[error("unsupported content-encoding: {0}")]
    UnsupportedEncoding(String),
    #[error("corrupt compressed request payload: {0}")]
    DecompressionFailed(String),
}

impl PayloadError {
    /// Map the payload error to its canonical HTTP status code.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::TooLarge { .. } | Self::DecompressedTooLarge { .. } => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            Self::UnsupportedEncoding(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::DecompressionFailed(_) => StatusCode::BAD_REQUEST,
        }
    }
}

fn read_bounded<R: Read>(reader: R, max_decompressed_size: usize) -> Result<Bytes, PayloadError> {
    let mut limited = reader.take((max_decompressed_size as u64) + 1);
    let mut out = Vec::with_capacity(max_decompressed_size.min(64 * 1024));
    limited
        .read_to_end(&mut out)
        .map_err(|e| PayloadError::DecompressionFailed(e.to_string()))?;
    if out.len() > max_decompressed_size {
        return Err(PayloadError::DecompressedTooLarge {
            limit: max_decompressed_size,
        });
    }
    Ok(Bytes::from(out))
}

/// Validate and optionally decompress an incoming HTTP request payload according to `Content-Encoding`.
pub fn decode_request_payload(
    body: &[u8],
    content_encoding: Option<&str>,
    limits: &PayloadLimits,
) -> Result<Bytes, PayloadError> {
    if body.len() > limits.max_body_size {
        return Err(PayloadError::TooLarge {
            actual: body.len(),
            limit: limits.max_body_size,
        });
    }

    let Some(raw_enc) = content_encoding else {
        return Ok(Bytes::copy_from_slice(body));
    };
    let enc = raw_enc.trim().to_ascii_lowercase();
    if enc.is_empty() || enc == "identity" {
        return Ok(Bytes::copy_from_slice(body));
    }

    match enc.as_str() {
        "gzip" | "x-gzip" => {
            let decoder = flate2::read::GzDecoder::new(body);
            read_bounded(decoder, limits.max_decompressed_size)
        }
        "deflate" => {
            let decoder = flate2::read::ZlibDecoder::new(body);
            read_bounded(decoder, limits.max_decompressed_size)
        }
        "br" => {
            let decoder = brotli::Decompressor::new(body, 4096);
            read_bounded(decoder, limits.max_decompressed_size)
        }
        "zstd" => {
            let decoder = zstd::stream::read::Decoder::new(body)
                .map_err(|e| PayloadError::DecompressionFailed(e.to_string()))?;
            read_bounded(decoder, limits.max_decompressed_size)
        }
        other => Err(PayloadError::UnsupportedEncoding(other.to_string())),
    }
}
