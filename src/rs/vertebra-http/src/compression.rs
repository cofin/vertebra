//! Zero-GIL HTTP response compression negotiation and encoding (`compression.rs`).

use std::io::Write;

use bytes::Bytes;

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
