//! Cleartext HTTP/2 (`h2c`) prior-knowledge detection, RFC 9113 `Upgrade: h2c`
//! settings negotiation, and `PrependStream` I/O adapter.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use http::{HeaderMap, Response, StatusCode};
use http_body_util::Full;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Standard 24-byte HTTP/2 connection preface (`"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"`).
pub const H2C_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Detected wire protocol on a cleartext TCP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleartextProtocol {
    /// Standard HTTP/1.1 connection (may still request `Upgrade: h2c`).
    Http1,
    /// Cleartext HTTP/2 prior-knowledge connection starting with `H2C_PREFACE`.
    H2cPriorKnowledge,
}

/// Decoded HTTP/2 SETTINGS parameters extracted from an `HTTP2-Settings` header.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct H2cUpgradeParams {
    pub header_table_size: Option<u32>,
    pub enable_push: Option<bool>,
    pub max_concurrent_streams: Option<u32>,
    pub initial_window_size: Option<u32>,
    pub max_frame_size: Option<u32>,
    pub max_header_list_size: Option<u32>,
}

/// Errors encountered during `h2c` upgrade header parsing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum H2cError {
    #[error("missing HTTP2-Settings header on h2c upgrade request")]
    MissingSettingsHeader,
    #[error("invalid base64url encoding in HTTP2-Settings header")]
    InvalidBase64Url,
    #[error("HTTP2-Settings payload length must be a multiple of 6 bytes")]
    InvalidSettingsFrameLength,
    #[error("invalid HTTP/2 SETTINGS parameter value")]
    InvalidSettingValue,
}

/// Peek at the leading bytes of a `TcpStream` to determine whether the client
/// sent the 24-byte HTTP/2 connection preface (`H2C_PREFACE`).
pub async fn detect_cleartext_protocol(stream: &TcpStream) -> io::Result<CleartextProtocol> {
    let mut buf = [0u8; 24];
    let n = stream.peek(&mut buf).await?;
    if n >= 4
        && buf[..n.min(24)] == H2C_PREFACE[..n.min(24)]
        && (n == 24 || buf.starts_with(b"PRI * "))
    {
        return Ok(CleartextProtocol::H2cPriorKnowledge);
    }
    Ok(CleartextProtocol::Http1)
}

/// Parse RFC 7540 Section 3.2 / RFC 9113 `Upgrade: h2c` and `HTTP2-Settings` headers.
///
/// Returns `Ok(None)` if the request does not ask for an `h2c` upgrade.
pub fn parse_h2c_upgrade_headers(
    headers: &HeaderMap,
) -> Result<Option<H2cUpgradeParams>, H2cError> {
    let is_h2c_upgrade = headers
        .get_all(http::header::UPGRADE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("h2c"));

    if !is_h2c_upgrade {
        return Ok(None);
    }

    let has_connection_upgrade = headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));

    if !has_connection_upgrade {
        return Ok(None);
    }

    let raw_settings = headers
        .get("http2-settings")
        .and_then(|v| v.to_str().ok())
        .ok_or(H2cError::MissingSettingsHeader)?;

    let decoded = decode_base64url(raw_settings.trim())?;
    if !decoded.len().is_multiple_of(6) {
        return Err(H2cError::InvalidSettingsFrameLength);
    }

    let mut params = H2cUpgradeParams::default();
    for chunk in decoded.as_chunks::<6>().0 {
        let id = u16::from_be_bytes([chunk[0], chunk[1]]);
        let val = u32::from_be_bytes([chunk[2], chunk[3], chunk[4], chunk[5]]);
        match id {
            0x1 => params.header_table_size = Some(val),
            0x2 => match val {
                0 => params.enable_push = Some(false),
                1 => params.enable_push = Some(true),
                _ => return Err(H2cError::InvalidSettingValue),
            },
            0x3 => params.max_concurrent_streams = Some(val),
            0x4 => {
                if val > 0x7FFF_FFFF {
                    return Err(H2cError::InvalidSettingValue);
                }
                params.initial_window_size = Some(val);
            }
            0x5 => {
                if !(16_384..=16_777_215).contains(&val) {
                    return Err(H2cError::InvalidSettingValue);
                }
                params.max_frame_size = Some(val);
            }
            0x6 => params.max_header_list_size = Some(val),
            _ => {}
        }
    }

    Ok(Some(params))
}

/// Build the `101 Switching Protocols` response required to complete an HTTP/1.1
/// `Upgrade: h2c` handshake.
pub fn build_h2c_switching_protocols_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::UPGRADE, "h2c")
        .body(Full::new(Bytes::new()))
        .expect("static 101 response must build")
}

/// Decode an RFC 4648 Section 5 Base64URL string (with or without `=` padding).
pub fn decode_base64url(input: &str) -> Result<Vec<u8>, H2cError> {
    let trimmed = input.trim_end_matches('=');
    let mut out = Vec::with_capacity((trimmed.len() * 3) / 4);
    let mut acc: u32 = 0;
    let mut bits: u8 = 0;

    for b in trimmed.bytes() {
        let val: u32 = match b {
            b'A'..=b'Z' => (b - b'A') as u32,
            b'a'..=b'z' => (b - b'a' + 26) as u32,
            b'0'..=b'9' => (b - b'0' + 52) as u32,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            _ => return Err(H2cError::InvalidBase64Url),
        };
        acc = (acc << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xFF) as u8);
        }
    }

    Ok(out)
}

/// Stream wrapper that yields a pre-buffered prefix (`Bytes`) before delegating
/// subsequent reads and all writes to the underlying stream `S`.
pub struct PrependStream<S> {
    prefix: Bytes,
    inner: S,
}

impl<S> PrependStream<S> {
    /// Wrap `inner` with an initial byte `prefix` to be consumed first on `AsyncRead`.
    pub fn new(prefix: Bytes, inner: S) -> Self {
        Self { prefix, inner }
    }

    /// Access a shared reference to the underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PrependStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.prefix.is_empty() {
            let to_copy = self.prefix.len().min(buf.remaining());
            buf.put_slice(&self.prefix[..to_copy]);
            self.prefix.advance(to_copy);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrependStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
