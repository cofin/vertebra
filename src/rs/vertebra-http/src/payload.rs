//! Request payload size enforcement (`413 Payload Too Large`) and bounded `gzip`/`br`/`zstd`/`deflate` decompression.

use std::io::Read;

use bytes::{Bytes, BytesMut};
use http::StatusCode;
use http_body_util::BodyExt;
use hyper::body::Body;
use serde::{Deserialize, Serialize};

/// Configurable byte limits for incoming HTTP request payloads.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
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
    ///Error reading the underlying HTTP body stream.
    #[error("failed to read request body stream: {0}")]
    StreamRead(String),
}

impl PayloadError {
    /// Map the payload error to its canonical HTTP status code.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::TooLarge { .. } | Self::DecompressedTooLarge { .. } => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            Self::UnsupportedEncoding(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::DecompressionFailed(_) | Self::StreamRead(_) => StatusCode::BAD_REQUEST,
        }
    }
}

/// Streamingly collect an HTTP request body while enforcing `limits.max_body_size` on every frame
/// and optionally decompressing `Content-Encoding` (`gzip`, `deflate`, `br`, `zstd`).
pub async fn collect_bounded_body<B>(
    mut body: B,
    content_length: Option<usize>,
    content_encoding: Option<&str>,
    limits: &PayloadLimits,
) -> Result<Bytes, PayloadError>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    if let Some(len) = content_length
        && len > limits.max_body_size
    {
        return Err(PayloadError::TooLarge {
            actual: len,
            limit: limits.max_body_size,
        });
    }

    let lower_hint = body.size_hint().lower() as usize;
    if lower_hint > limits.max_body_size {
        return Err(PayloadError::TooLarge {
            actual: lower_hint,
            limit: limits.max_body_size,
        });
    }

    if body.is_end_stream() {
        return Ok(Bytes::new());
    }

    let mut first_chunk: Option<Bytes> = None;
    let mut multi_buf: Option<BytesMut> = None;
    let mut total_bytes = 0usize;

    while let Some(frame_res) = body.frame().await {
        let frame = frame_res.map_err(|e| PayloadError::StreamRead(e.to_string()))?;
        if let Ok(data) = frame.into_data() {
            if data.is_empty() {
                continue;
            }
            total_bytes = total_bytes.saturating_add(data.len());
            if total_bytes > limits.max_body_size {
                return Err(PayloadError::TooLarge {
                    actual: total_bytes,
                    limit: limits.max_body_size,
                });
            }
            if let Some(ref mut buf) = multi_buf {
                buf.extend_from_slice(&data);
            } else if let Some(prev) = first_chunk.take() {
                let cap = content_length
                    .unwrap_or(total_bytes * 2)
                    .min(limits.max_body_size)
                    .max(total_bytes);
                let mut buf = BytesMut::with_capacity(cap);
                buf.extend_from_slice(&prev);
                buf.extend_from_slice(&data);
                multi_buf = Some(buf);
            } else {
                first_chunk = Some(data);
            }
        }
    }

    let raw_bytes = if let Some(buf) = multi_buf {
        buf.freeze()
    } else {
        first_chunk.unwrap_or_default()
    };

    if let Some(enc) = is_active_content_encoding(content_encoding) {
        return decode_request_payload(&raw_bytes, Some(enc), limits);
    }

    Ok(raw_bytes)
}

/// Return the trimmed `Content-Encoding` directive when decompression is required.
pub(crate) fn is_active_content_encoding(raw: Option<&str>) -> Option<&str> {
    let trimmed = raw?.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("identity") {
        None
    } else {
        Some(trimmed)
    }
}

/// Validate a pre-buffered request payload (HTTP/3 QUIC) and decompress when `Content-Encoding` is active.
///
/// Returns `(decoded_bytes, was_decompressed)` so uncompressed payloads remain zero-copy `Bytes` slices.
pub fn decode_buffered_body(
    body: Bytes,
    content_length: Option<usize>,
    content_encoding: Option<&str>,
    limits: &PayloadLimits,
) -> Result<(Bytes, bool), PayloadError> {
    if let Some(len) = content_length
        && len > limits.max_body_size
    {
        return Err(PayloadError::TooLarge {
            actual: len,
            limit: limits.max_body_size,
        });
    }
    if body.len() > limits.max_body_size {
        return Err(PayloadError::TooLarge {
            actual: body.len(),
            limit: limits.max_body_size,
        });
    }
    if body.is_empty() {
        return Ok((Bytes::new(), false));
    }
    if let Some(enc) = is_active_content_encoding(content_encoding) {
        return decode_request_payload(&body, Some(enc), limits).map(|decoded| (decoded, true));
    }
    Ok((body, false))
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
