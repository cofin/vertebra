//! Zero-GIL HTTP response compression negotiation and encoding (`compression.rs`).

use std::io::Write;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, header};
use serde::{Deserialize, Serialize};

/// Supported HTTP response `Content-Encoding` codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseCodec {
    Zstd,
    Brotli,
    Gzip,
}

impl ResponseCodec {
    /// Return the wire `Content-Encoding` header token.
    pub fn as_header_str(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Brotli => "br",
            Self::Gzip => "gzip",
        }
    }

    fn priority(self) -> u8 {
        match self {
            Self::Zstd => 3,
            Self::Brotli => 2,
            Self::Gzip => 1,
        }
    }
}

/// Configuration for automatic HTTP response compression in Rust.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Minimum uncompressed body length in bytes before compressing (default: 512).
    pub minimum_size: usize,
    /// Zstd compression level (1..=22, default: 3).
    pub zstd_level: i32,
    /// Brotli quality level (0..=11, default: 4).
    pub brotli_quality: u32,
    /// Gzip compression level (0..=9, default: 6).
    pub gzip_level: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            minimum_size: 512,
            zstd_level: 3,
            brotli_quality: 4,
            gzip_level: 6,
        }
    }
}

/// Negotiate the preferred `ResponseCodec` from an `Accept-Encoding` header value.
pub fn negotiate_response_codec(accept_encoding: &str) -> Option<ResponseCodec> {
    let mut best: Option<(ResponseCodec, f32)> = None;
    for raw_part in accept_encoding.split(',') {
        let mut segments = raw_part.trim().split(';');
        let token = segments.next().unwrap_or("").trim().to_ascii_lowercase();
        let codec = match token.as_str() {
            "zstd" => ResponseCodec::Zstd,
            "br" => ResponseCodec::Brotli,
            "gzip" | "x-gzip" => ResponseCodec::Gzip,
            _ => continue,
        };
        let mut q = 1.0f32;
        for param in segments {
            if let Some(val) = param.trim().strip_prefix("q=")
                && let Ok(parsed) = val.parse::<f32>()
            {
                q = parsed;
            }
        }
        if q <= 0.0 {
            continue;
        }
        if best.is_none_or(|(best_codec, best_q)| {
            q > best_q
                || ((q - best_q).abs() < f32::EPSILON && codec.priority() > best_codec.priority())
        }) {
            best = Some((codec, q));
        }
    }
    best.map(|(c, _)| c)
}

fn is_compressible_content_type(content_type: &str) -> bool {
    let lower = content_type.to_ascii_lowercase();
    lower.starts_with("text/")
        || lower.contains("json")
        || lower.contains("javascript")
        || lower.contains("xml")
        || lower.contains("svg")
        || lower.contains("wasm")
}

/// Compress a response body in Rust if it meets the size threshold and compressible media type.
pub fn compress_response_body(
    body: &[u8],
    accept_encoding: Option<&str>,
    content_type: &str,
    config: &CompressionConfig,
) -> Option<(ResponseCodec, Bytes)> {
    if body.len() < config.minimum_size || !is_compressible_content_type(content_type) {
        return None;
    }
    let codec = negotiate_response_codec(accept_encoding?)?;
    match codec {
        ResponseCodec::Zstd => {
            let encoded = zstd::stream::encode_all(body, config.zstd_level).ok()?;
            Some((codec, Bytes::from(encoded)))
        }
        ResponseCodec::Brotli => {
            let mut out = Vec::new();
            {
                let mut writer =
                    brotli::CompressorWriter::new(&mut out, 4096, config.brotli_quality, 22);
                writer.write_all(body).ok()?;
            }
            Some((codec, Bytes::from(out)))
        }
        ResponseCodec::Gzip => {
            let mut encoder = flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::new(config.gzip_level),
            );
            encoder.write_all(body).ok()?;
            let encoded = encoder.finish().ok()?;
            Some((codec, Bytes::from(encoded)))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionBackend {
    Zstd,
    Gzip,
    #[serde(alias = "br")]
    Brotli,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionOffloadConfig {
    pub backend: CompressionBackend,
    pub minimum_size: usize,
    pub gzip_compress_level: u32,
    pub brotli_quality: u32,
    pub zstd_level: i32,
    pub exclude_patterns: Vec<String>,
    pub excluded_paths: Vec<String>,
}

impl Default for CompressionOffloadConfig {
    fn default() -> Self {
        Self {
            backend: CompressionBackend::Zstd,
            minimum_size: 500,
            gzip_compress_level: 6,
            brotli_quality: 4,
            zstd_level: 3,
            exclude_patterns: Vec::new(),
            excluded_paths: Vec::new(),
        }
    }
}

impl CompressionOffloadConfig {
    pub fn is_path_excluded(&self, path: &str) -> bool {
        if self.excluded_paths.iter().any(|p| p == path) {
            return true;
        }
        self.exclude_patterns
            .iter()
            .any(|pattern| path == pattern || path.starts_with(pattern.as_str()))
    }

    pub fn compress_if_eligible(
        &self,
        path: &str,
        accept_encoding: Option<&str>,
        headers: &mut HeaderMap,
        body: Bytes,
    ) -> Bytes {
        if body.len() < self.minimum_size || self.is_path_excluded(path) {
            return body;
        }
        if headers.contains_key(header::CONTENT_ENCODING) {
            return body;
        }
        let Some(accept) = accept_encoding else {
            return body;
        };

        match self.backend {
            CompressionBackend::Zstd if accept.contains("zstd") => {
                if let Ok(compressed) = zstd::stream::encode_all(body.as_ref(), self.zstd_level) {
                    headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("zstd"));
                    headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
                    headers.remove(header::CONTENT_LENGTH);
                    return Bytes::from(compressed);
                }
                body
            }
            CompressionBackend::Gzip if accept.contains("gzip") => {
                let mut encoder = flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::new(self.gzip_compress_level),
                );
                if encoder.write_all(body.as_ref()).is_ok()
                    && let Ok(compressed) = encoder.finish()
                {
                    headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
                    headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
                    headers.remove(header::CONTENT_LENGTH);
                    return Bytes::from(compressed);
                }
                body
            }
            CompressionBackend::Brotli if accept.contains("br") => {
                let mut writer =
                    brotli::CompressorWriter::new(Vec::new(), 4096, self.brotli_quality, 22);
                if writer.write_all(body.as_ref()).is_ok() {
                    let compressed = writer.into_inner();
                    headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("br"));
                    headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
                    headers.remove(header::CONTENT_LENGTH);
                    return Bytes::from(compressed);
                }
                body
            }
            _ => body,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offload_compresses_zstd_gzip_and_brotli() {
        let raw = Bytes::from(vec![b'a'; 1024]);

        for (backend, accept, expected_encoding) in [
            (CompressionBackend::Zstd, "zstd, gzip", "zstd"),
            (CompressionBackend::Gzip, "gzip, deflate", "gzip"),
            (CompressionBackend::Brotli, "br, gzip", "br"),
        ] {
            let cfg = CompressionOffloadConfig {
                backend,
                minimum_size: 256,
                ..CompressionOffloadConfig::default()
            };
            let mut headers = HeaderMap::new();
            let compressed =
                cfg.compress_if_eligible("/items", Some(accept), &mut headers, raw.clone());
            assert!(compressed.len() < raw.len());
            assert_eq!(
                headers
                    .get(header::CONTENT_ENCODING)
                    .and_then(|v| v.to_str().ok()),
                Some(expected_encoding)
            );
        }
    }
}
