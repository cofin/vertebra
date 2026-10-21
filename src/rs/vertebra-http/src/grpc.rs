//! Native Hyper gRPC 5-byte Length-Prefixed Message (LPM) wire framing,
//! `grpc-timeout` parsing, percent-encoded `grpc-message` trailers, and
//! `FLAG_TRAILERS` HTTP/2 trailer emission.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use hyper::body::{Body, Frame};
use tokio::sync::mpsc;

/// Length of the gRPC Length-Prefixed Message header (`1` compression byte + `4` BE length bytes).
pub const GRPC_HEADER_LEN: usize = 5;

/// Default maximum inbound/outbound gRPC message size (`4 MiB`).
pub const DEFAULT_MAX_GRPC_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Response flag bit indicating that the frame carries HTTP/2 trailing headers.
pub const FLAG_TRAILERS: u16 = 1 << 5;

/// Canonical gRPC status codes (`0..=16`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcStatus {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl GrpcStatus {
    /// Convert a raw `u8` status code (`0..=16`) into a `GrpcStatus`.
    pub const fn from_u8(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Ok),
            1 => Some(Self::Cancelled),
            2 => Some(Self::Unknown),
            3 => Some(Self::InvalidArgument),
            4 => Some(Self::DeadlineExceeded),
            5 => Some(Self::NotFound),
            6 => Some(Self::AlreadyExists),
            7 => Some(Self::PermissionDenied),
            8 => Some(Self::ResourceExhausted),
            9 => Some(Self::FailedPrecondition),
            10 => Some(Self::Aborted),
            11 => Some(Self::OutOfRange),
            12 => Some(Self::Unimplemented),
            13 => Some(Self::Internal),
            14 => Some(Self::Unavailable),
            15 => Some(Self::DataLoss),
            16 => Some(Self::Unauthenticated),
            _ => None,
        }
    }

    /// Return the numeric `u8` representation (`0..=16`).
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Return the static ASCII digit representation for `grpc-status` headers.
    pub const fn as_code_str(self) -> &'static str {
        match self {
            Self::Ok => "0",
            Self::Cancelled => "1",
            Self::Unknown => "2",
            Self::InvalidArgument => "3",
            Self::DeadlineExceeded => "4",
            Self::NotFound => "5",
            Self::AlreadyExists => "6",
            Self::PermissionDenied => "7",
            Self::ResourceExhausted => "8",
            Self::FailedPrecondition => "9",
            Self::Aborted => "10",
            Self::OutOfRange => "11",
            Self::Unimplemented => "12",
            Self::Internal => "13",
            Self::Unavailable => "14",
            Self::DataLoss => "15",
            Self::Unauthenticated => "16",
        }
    }

    /// Return the canonical uppercase gRPC status name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Cancelled => "CANCELLED",
            Self::Unknown => "UNKNOWN",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::Aborted => "ABORTED",
            Self::OutOfRange => "OUT_OF_RANGE",
            Self::Unimplemented => "UNIMPLEMENTED",
            Self::Internal => "INTERNAL",
            Self::Unavailable => "UNAVAILABLE",
            Self::DataLoss => "DATA_LOSS",
            Self::Unauthenticated => "UNAUTHENTICATED",
        }
    }
}

/// Wire payload codec negotiated via `content-type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GrpcWireFormat {
    /// `application/grpc+msgpack`
    Msgpack,
    /// `application/grpc+proto` or `application/grpc`
    #[default]
    Proto,
}

impl GrpcWireFormat {
    /// Inspect a `content-type` string to determine the gRPC wire format.
    pub fn from_content_type(ct: &str) -> Self {
        if ct.contains("+msgpack") {
            Self::Msgpack
        } else {
            Self::Proto
        }
    }

    /// Inspect a request `HeaderMap` to determine the gRPC wire format.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(Self::from_content_type)
            .unwrap_or_default()
    }

    /// Return the `content-type` header value for this wire format.
    pub const fn content_type_str(self) -> &'static str {
        match self {
            Self::Msgpack => "application/grpc+msgpack",
            Self::Proto => "application/grpc+proto",
        }
    }
}

/// A single decoded 5-byte length-prefixed gRPC frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcFrame {
    pub compressed: bool,
    pub payload: Bytes,
}

/// Errors produced during gRPC framing or header parsing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GrpcError {
    #[error("invalid gRPC compression flag byte: {0}")]
    InvalidCompressionFlag(u8),
    #[error("gRPC message length {size} exceeds maximum {max}")]
    MessageTooLarge { size: usize, max: usize },
    #[error("invalid grpc-timeout header: {0}")]
    InvalidTimeout(String),
}

impl GrpcError {
    /// Map a framing or protocol error to its canonical `GrpcStatus`.
    pub const fn status(&self) -> GrpcStatus {
        match self {
            Self::InvalidCompressionFlag(_) => GrpcStatus::Internal,
            Self::MessageTooLarge { .. } => GrpcStatus::ResourceExhausted,
            Self::InvalidTimeout(_) => GrpcStatus::InvalidArgument,
        }
    }
}

/// Return `true` if `req` is an HTTP `POST` request with `content-type` starting with `application/grpc`.
pub fn is_grpc_request<B>(req: &Request<B>) -> bool {
    req.method() == Method::POST
        && req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("application/grpc"))
}

/// Encode a single gRPC payload with its 5-byte Length-Prefixed Message header into `dst`.
pub fn encode_grpc_frame(payload: &[u8], compressed: bool, dst: &mut BytesMut) {
    dst.reserve(GRPC_HEADER_LEN + payload.len());
    dst.put_u8(u8::from(compressed));
    dst.put_u32(payload.len() as u32);
    dst.extend_from_slice(payload);
}

/// Decode all complete 5-byte LPM frames available in `src`, leaving any trailing
/// partial frame intact in `src` for subsequent reads.
pub fn decode_grpc_frames(
    src: &mut BytesMut,
    max_message_size: usize,
) -> Result<Vec<GrpcFrame>, GrpcError> {
    let mut frames = Vec::new();
    while src.len() >= GRPC_HEADER_LEN {
        let flag = src[0];
        if flag > 1 {
            return Err(GrpcError::InvalidCompressionFlag(flag));
        }
        let msg_len = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;
        if msg_len > max_message_size {
            return Err(GrpcError::MessageTooLarge {
                size: msg_len,
                max: max_message_size,
            });
        }
        if src.len() < GRPC_HEADER_LEN + msg_len {
            break;
        }
        src.advance(GRPC_HEADER_LEN);
        let payload = src.split_to(msg_len).freeze();
        frames.push(GrpcFrame {
            compressed: flag == 1,
            payload,
        });
    }
    Ok(frames)
}

/// Percent-encode a `grpc-message` trailer string per the gRPC over HTTP/2 specification
/// (RFC 3986 percent-encoding for non-printable ASCII, `%`, and UTF-8 bytes).
pub fn percent_encode_grpc_message(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    for &b in msg.as_bytes() {
        if (0x20..=0x7E).contains(&b) && b != b'%' && b != b' ' {
            out.push(b as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{b:02X}");
        }
    }
    out
}

/// Decode a percent-encoded `grpc-message` trailer string back to UTF-8.
pub fn percent_decode_grpc_message(encoded: &str) -> String {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(hex_str) = std::str::from_utf8(&bytes[i + 1..i + 3])
            && let Ok(val) = u8::from_str_radix(hex_str, 16)
        {
            out.push(val);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Build an HTTP/2 trailing `HeaderMap` containing `grpc-status` and optional
/// percent-encoded `grpc-message`.
pub fn build_grpc_trailers(status: GrpcStatus, message: Option<&str>) -> HeaderMap {
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "grpc-status",
        HeaderValue::from_static(status.as_code_str()),
    );
    if let Some(msg) = message
        && !msg.is_empty()
    {
        let encoded = percent_encode_grpc_message(msg);
        if let Ok(val) = HeaderValue::from_str(&encoded) {
            trailers.insert("grpc-message", val);
        }
    }
    trailers
}

/// Parse a `grpc-timeout` header value (`<1-8 digits><H|M|S|m|u|n>`) into a `Duration`.
pub fn parse_grpc_timeout(header_val: &str) -> Result<Duration, GrpcError> {
    let trimmed = header_val.trim();
    if trimmed.len() < 2 || trimmed.len() > 9 {
        return Err(GrpcError::InvalidTimeout(trimmed.to_string()));
    }
    let (digits, unit) = trimmed.split_at(trimmed.len() - 1);
    let value: u64 = digits
        .parse()
        .map_err(|_| GrpcError::InvalidTimeout(trimmed.to_string()))?;

    match unit {
        "H" => Ok(Duration::from_secs(value.saturating_mul(3600))),
        "M" => Ok(Duration::from_secs(value.saturating_mul(60))),
        "S" => Ok(Duration::from_secs(value)),
        "m" => Ok(Duration::from_millis(value)),
        "u" => Ok(Duration::from_micros(value)),
        "n" => Ok(Duration::from_nanos(value)),
        _ => Err(GrpcError::InvalidTimeout(trimmed.to_string())),
    }
}

/// Stream body yielding gRPC LPM data frames followed by an HTTP/2 `Frame::trailers` frame.
pub struct GrpcStreamBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
}

impl Body for GrpcStreamBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.rx.poll_recv(cx)
    }
}

/// Build an HTTP/2 gRPC response that streams `frames` as 5-byte LPM data chunks
/// and emits `Frame::trailers` when `flags & FLAG_TRAILERS != 0`.
pub fn build_grpc_response_with_trailers(
    wire_format: GrpcWireFormat,
    frames: Vec<GrpcFrame>,
    status: GrpcStatus,
    message: Option<&str>,
    flags: u16,
) -> Response<GrpcStreamBody> {
    let (tx, rx) = mpsc::channel(frames.len() + 2);
    let trailers = if (flags & FLAG_TRAILERS) != 0 {
        Some(build_grpc_trailers(status, message))
    } else {
        None
    };

    tokio::spawn(async move {
        for frame in frames {
            let mut buf = BytesMut::with_capacity(GRPC_HEADER_LEN + frame.payload.len());
            encode_grpc_frame(&frame.payload, frame.compressed, &mut buf);
            if tx.send(Ok(Frame::data(buf.freeze()))).await.is_err() {
                return;
            }
        }
        if let Some(tr) = trailers {
            let _ = tx.send(Ok(Frame::trailers(tr))).await;
        }
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, wire_format.content_type_str())
        .body(GrpcStreamBody { rx })
        .expect("gRPC response must build")
}

/// Build an immediate trailers-only gRPC response (where `grpc-status` and
/// `grpc-message` are placed directly in the initial HTTP/2 response headers).
pub fn build_grpc_trailers_only_response(
    wire_format: GrpcWireFormat,
    status: GrpcStatus,
    message: Option<&str>,
) -> Response<http_body_util::Full<Bytes>> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, wire_format.content_type_str())
        .header("grpc-status", status.as_code_str());

    if let Some(msg) = message
        && !msg.is_empty()
    {
        builder = builder.header("grpc-message", percent_encode_grpc_message(msg));
    }

    builder
        .body(http_body_util::Full::new(Bytes::new()))
        .expect("trailers-only gRPC response must build")
}
