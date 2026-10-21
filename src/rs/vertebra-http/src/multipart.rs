//! Zero-GIL streaming `multipart/form-data` parser (`multipart.rs`) powered by `multer 3.1.0`.

use std::convert::Infallible;

use bytes::{Bytes, BytesMut};
use futures_util::stream;
use http::StatusCode;
use multer::Multipart;

/// Size and field count limits for `multipart/form-data` uploads.
#[derive(Debug, Clone, Copy)]
pub struct MultipartLimits {
    /// Maximum number of multipart fields allowed in a single request (default: 128).
    pub max_fields: usize,
    /// Maximum size in bytes of any single uploaded file field (default: 16 MiB).
    pub max_file_size: usize,
    /// Maximum cumulative bytes across all parts (default: 32 MiB).
    pub max_total_size: usize,
}

impl Default for MultipartLimits {
    fn default() -> Self {
        Self {
            max_fields: 128,
            max_file_size: 16 * 1024 * 1024,
            max_total_size: 32 * 1024 * 1024,
        }
    }
}

/// Decoded value of a single multipart field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartPartValue {
    /// UTF-8 text form field.
    Text(String),
    /// Binary file upload payload.
    File(Bytes),
}

/// A single parsed part from a `multipart/form-data` body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPart {
    pub name: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub value: MultipartPartValue,
}

/// Complete parsed `multipart/form-data` payload.
#[derive(Debug, Clone, Default)]
pub struct ParsedMultipart {
    pub parts: Vec<MultipartPart>,
}

/// Errors raised during `multipart/form-data` boundary parsing.
#[derive(Debug, thiserror::Error)]
pub enum MultipartError {
    #[error("missing or invalid multipart boundary in Content-Type")]
    MissingBoundary,
    #[error("multipart field count exceeds limit {limit}")]
    TooManyFields { limit: usize },
    #[error("multipart file size exceeds limit {limit}")]
    FileTooLarge { limit: usize },
    #[error("multipart total size exceeds limit {limit}")]
    TotalSizeTooLarge { limit: usize },
    #[error("malformed multipart stream: {0}")]
    Malformed(String),
}

impl MultipartError {
    /// Map the multipart error to its HTTP status code.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::TooManyFields { .. }
            | Self::FileTooLarge { .. }
            | Self::TotalSizeTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::MissingBoundary | Self::Malformed(_) => StatusCode::BAD_REQUEST,
        }
    }
}

/// Parse a `multipart/form-data` payload in Rust using `multer 3.1.0` while enforcing field and byte limits.
pub async fn parse_multipart_body(
    body: Bytes,
    content_type_header: &str,
    limits: &MultipartLimits,
) -> Result<ParsedMultipart, MultipartError> {
    if body.len() > limits.max_total_size {
        return Err(MultipartError::TotalSizeTooLarge {
            limit: limits.max_total_size,
        });
    }
    let boundary =
        multer::parse_boundary(content_type_header).map_err(|_| MultipartError::MissingBoundary)?;
    let byte_stream = stream::once(async move { Ok::<Bytes, Infallible>(body) });
    let mut multipart = Multipart::new(byte_stream, boundary);

    let mut parts = Vec::new();
    let mut total_bytes = 0usize;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| MultipartError::Malformed(e.to_string()))?
    {
        if parts.len() >= limits.max_fields {
            return Err(MultipartError::TooManyFields {
                limit: limits.max_fields,
            });
        }
        let name = field.name().unwrap_or("").to_string();
        let filename = field.file_name().map(str::to_string);
        let content_type = field.content_type().map(|m| m.to_string());

        let mut buf = BytesMut::new();
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|e| MultipartError::Malformed(e.to_string()))?
        {
            total_bytes += chunk.len();
            if total_bytes > limits.max_total_size {
                return Err(MultipartError::TotalSizeTooLarge {
                    limit: limits.max_total_size,
                });
            }
            if buf.len() + chunk.len() > limits.max_file_size {
                return Err(MultipartError::FileTooLarge {
                    limit: limits.max_file_size,
                });
            }
            buf.extend_from_slice(&chunk);
        }

        let frozen = buf.freeze();
        let value = if filename.is_some() {
            MultipartPartValue::File(frozen)
        } else {
            match std::str::from_utf8(&frozen) {
                Ok(text) => MultipartPartValue::Text(text.to_string()),
                Err(_) => MultipartPartValue::File(frozen),
            }
        };

        parts.push(MultipartPart {
            name,
            filename,
            content_type,
            value,
        });
    }

    Ok(ParsedMultipart { parts })
}
