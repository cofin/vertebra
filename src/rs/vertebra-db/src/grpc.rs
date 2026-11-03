//! Pure-Rust HTTP/2 (`h2` + `h2c`) gRPC channel pool, `128 MiB` LPM framing, `google.rpc.Status`
//! trailer decoder, RFC 4122 UUIDv4 generator, and zero-AST `ProtoReader` / `ProtoWriter` codecs.

use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STD, STANDARD_NO_PAD as BASE64_NO_PAD};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use http::header::{AUTHORIZATION, CONTENT_TYPE, TE, USER_AGENT};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Uri};
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming};
use hyper::client::conn::http2::{self, SendRequest};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio_rustls::TlsConnector;
use url::Url;

/// Default maximum inbound/outbound gRPC message size (`128 MiB`).
///
/// BigQuery Storage Read `ReadRowsResponse` Arrow batches and Cloud Spanner
/// `PartialResultSet` chunks routinely exceed `4 MiB`.
pub const DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// Default HTTP/2 initial stream flow-control window size (`8 MiB`).
pub const DEFAULT_INITIAL_STREAM_WINDOW_SIZE: u32 = 8 * 1024 * 1024;

/// Default HTTP/2 initial connection flow-control window size (`16 MiB`).
pub const DEFAULT_INITIAL_CONNECTION_WINDOW_SIZE: u32 = 16 * 1024 * 1024;

/// Protobuf v3 wire types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum WireType {
    Varint = 0,
    Fixed64 = 1,
    LengthDelimited = 2,
    Fixed32 = 5,
}

impl WireType {
    pub fn from_u32(val: u32) -> Result<Self, ProtoError> {
        match val {
            0 => Ok(Self::Varint),
            1 => Ok(Self::Fixed64),
            2 => Ok(Self::LengthDelimited),
            5 => Ok(Self::Fixed32),
            other => Err(ProtoError::InvalidWireType(other)),
        }
    }
}

/// Errors produced by [`ProtoReader`] during wire decoding.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtoError {
    #[error("unexpected end of protobuf buffer")]
    UnexpectedEof,
    #[error("protobuf varint overflowed 64 bits")]
    VarintOverflow,
    #[error("invalid protobuf wire type: {0}")]
    InvalidWireType(u32),
    #[error("invalid protobuf field number: {0}")]
    InvalidFieldNumber(u32),
    #[error("invalid UTF-8 string in protobuf field")]
    InvalidUtf8,
    #[error("protobuf decode error: {0}")]
    Custom(String),
}

/// Zero-AST Protobuf v3 wire reader over a borrowed byte slice.
#[derive(Debug, Clone)]
pub struct ProtoReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ProtoReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn next_tag(&mut self) -> Result<Option<(u32, WireType)>, ProtoError> {
        if self.is_empty() {
            return Ok(None);
        }
        let raw = self.read_varint()?;
        let field_number = (raw >> 3) as u32;
        if field_number == 0 {
            return Err(ProtoError::InvalidFieldNumber(0));
        }
        let wire_type = WireType::from_u32((raw & 0x07) as u32)?;
        Ok(Some((field_number, wire_type)))
    }

    pub fn read_varint(&mut self) -> Result<u64, ProtoError> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        while shift < 64 {
            if self.pos >= self.buf.len() {
                return Err(ProtoError::UnexpectedEof);
            }
            let byte = self.buf[self.pos];
            self.pos += 1;
            result |= u64::from(byte & 0x7f) << shift;
            if (byte & 0x80) == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(ProtoError::VarintOverflow)
    }

    pub fn read_u32(&mut self) -> Result<u32, ProtoError> {
        Ok(self.read_varint()? as u32)
    }

    pub fn read_i32(&mut self) -> Result<i32, ProtoError> {
        Ok(self.read_varint()? as i32)
    }

    pub fn read_i64(&mut self) -> Result<i64, ProtoError> {
        Ok(self.read_varint()? as i64)
    }

    pub fn read_bool(&mut self) -> Result<bool, ProtoError> {
        Ok(self.read_varint()? != 0)
    }

    pub fn read_sint32(&mut self) -> Result<i32, ProtoError> {
        let raw = self.read_u32()?;
        Ok(((raw >> 1) as i32) ^ -((raw & 1) as i32))
    }

    pub fn read_sint64(&mut self) -> Result<i64, ProtoError> {
        let raw = self.read_varint()?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }

    pub fn read_fixed32(&mut self) -> Result<u32, ProtoError> {
        if self.remaining() < 4 {
            return Err(ProtoError::UnexpectedEof);
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(u32::from_le_bytes(arr))
    }

    pub fn read_fixed64(&mut self) -> Result<u64, ProtoError> {
        if self.remaining() < 8 {
            return Err(ProtoError::UnexpectedEof);
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_le_bytes(arr))
    }

    pub fn read_f32(&mut self) -> Result<f32, ProtoError> {
        Ok(f32::from_bits(self.read_fixed32()?))
    }

    pub fn read_f64(&mut self) -> Result<f64, ProtoError> {
        Ok(f64::from_bits(self.read_fixed64()?))
    }

    pub fn read_bytes(&mut self) -> Result<&'a [u8], ProtoError> {
        let len = self.read_varint()? as usize;
        if self.remaining() < len {
            return Err(ProtoError::UnexpectedEof);
        }
        let slice = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    pub fn read_str(&mut self) -> Result<&'a str, ProtoError> {
        let bytes = self.read_bytes()?;
        std::str::from_utf8(bytes).map_err(|_| ProtoError::InvalidUtf8)
    }

    pub fn read_nested(&mut self) -> Result<ProtoReader<'a>, ProtoError> {
        let bytes = self.read_bytes()?;
        Ok(ProtoReader::new(bytes))
    }

    pub fn read_packed_varints(&mut self) -> Result<Vec<u64>, ProtoError> {
        let mut sub = self.read_nested()?;
        let mut out = Vec::new();
        while !sub.is_empty() {
            out.push(sub.read_varint()?);
        }
        Ok(out)
    }

    pub fn read_packed_i64(&mut self) -> Result<Vec<i64>, ProtoError> {
        let mut sub = self.read_nested()?;
        let mut out = Vec::new();
        while !sub.is_empty() {
            out.push(sub.read_i64()?);
        }
        Ok(out)
    }

    pub fn read_packed_fixed64(&mut self) -> Result<Vec<u64>, ProtoError> {
        let bytes = self.read_bytes()?;
        if !bytes.len().is_multiple_of(8) {
            return Err(ProtoError::UnexpectedEof);
        }
        let mut out = Vec::with_capacity(bytes.len() / 8);
        for &chunk in bytes.as_chunks::<8>().0 {
            out.push(u64::from_le_bytes(chunk));
        }
        Ok(out)
    }

    pub fn read_packed_f64(&mut self) -> Result<Vec<f64>, ProtoError> {
        let raw = self.read_packed_fixed64()?;
        Ok(raw.into_iter().map(f64::from_bits).collect())
    }

    pub fn skip_field(&mut self, wire_type: WireType) -> Result<(), ProtoError> {
        match wire_type {
            WireType::Varint => {
                let _ = self.read_varint()?;
                Ok(())
            }
            WireType::Fixed64 => {
                if self.remaining() < 8 {
                    return Err(ProtoError::UnexpectedEof);
                }
                self.pos += 8;
                Ok(())
            }
            WireType::LengthDelimited => {
                let _ = self.read_bytes()?;
                Ok(())
            }
            WireType::Fixed32 => {
                if self.remaining() < 4 {
                    return Err(ProtoError::UnexpectedEof);
                }
                self.pos += 4;
                Ok(())
            }
        }
    }
}

/// Zero-AST Protobuf v3 wire writer backed by `BytesMut`.
#[derive(Debug, Default, Clone)]
pub struct ProtoWriter {
    buf: BytesMut,
}

impl ProtoWriter {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(256),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn write_raw_varint(&mut self, mut val: u64) {
        while val >= 0x80 {
            self.buf.put_u8(((val as u8) & 0x7f) | 0x80);
            val >>= 7;
        }
        self.buf.put_u8(val as u8);
    }

    pub fn write_tag(&mut self, field_number: u32, wire_type: WireType) {
        let tag = (u64::from(field_number) << 3) | (wire_type as u64);
        self.write_raw_varint(tag);
    }

    pub fn write_varint(&mut self, field_number: u32, val: u64) {
        self.write_tag(field_number, WireType::Varint);
        self.write_raw_varint(val);
    }

    pub fn write_u32(&mut self, field_number: u32, val: u32) {
        self.write_varint(field_number, u64::from(val));
    }

    pub fn write_i32(&mut self, field_number: u32, val: i32) {
        self.write_varint(field_number, val as i64 as u64);
    }

    pub fn write_i64(&mut self, field_number: u32, val: i64) {
        self.write_varint(field_number, val as u64);
    }

    pub fn write_bool(&mut self, field_number: u32, val: bool) {
        self.write_varint(field_number, u64::from(val));
    }

    pub fn write_sint32(&mut self, field_number: u32, val: i32) {
        let encoded = ((val << 1) ^ (val >> 31)) as u32;
        self.write_varint(field_number, u64::from(encoded));
    }

    pub fn write_sint64(&mut self, field_number: u32, val: i64) {
        let encoded = ((val << 1) ^ (val >> 63)) as u64;
        self.write_varint(field_number, encoded);
    }

    pub fn write_fixed32(&mut self, field_number: u32, val: u32) {
        self.write_tag(field_number, WireType::Fixed32);
        self.buf.put_u32_le(val);
    }

    pub fn write_fixed64(&mut self, field_number: u32, val: u64) {
        self.write_tag(field_number, WireType::Fixed64);
        self.buf.put_u64_le(val);
    }

    pub fn write_f32(&mut self, field_number: u32, val: f32) {
        self.write_fixed32(field_number, val.to_bits());
    }

    pub fn write_f64(&mut self, field_number: u32, val: f64) {
        self.write_fixed64(field_number, val.to_bits());
    }

    pub fn write_bytes(&mut self, field_number: u32, data: &[u8]) {
        self.write_tag(field_number, WireType::LengthDelimited);
        self.write_raw_varint(data.len() as u64);
        self.buf.extend_from_slice(data);
    }

    pub fn write_str(&mut self, field_number: u32, s: &str) {
        self.write_bytes(field_number, s.as_bytes());
    }

    pub fn write_nested<F>(&mut self, field_number: u32, build_fn: F)
    where
        F: FnOnce(&mut ProtoWriter),
    {
        let mut nested = ProtoWriter::new();
        build_fn(&mut nested);
        let bytes = nested.finish();
        self.write_bytes(field_number, &bytes);
    }

    pub fn write_packed_varints(&mut self, field_number: u32, values: &[u64]) {
        self.write_nested(field_number, |w| {
            for &val in values {
                w.write_raw_varint(val);
            }
        });
    }

    pub fn write_packed_i64(&mut self, field_number: u32, values: &[i64]) {
        self.write_nested(field_number, |w| {
            for &val in values {
                w.write_raw_varint(val as u64);
            }
        });
    }

    pub fn write_packed_fixed64(&mut self, field_number: u32, values: &[u64]) {
        self.write_tag(field_number, WireType::LengthDelimited);
        self.write_raw_varint((values.len() * 8) as u64);
        for &val in values {
            self.buf.put_u64_le(val);
        }
    }

    pub fn write_packed_f64(&mut self, field_number: u32, values: &[f64]) {
        self.write_tag(field_number, WireType::LengthDelimited);
        self.write_raw_varint((values.len() * 8) as u64);
        for &val in values {
            self.buf.put_u64_le(val.to_bits());
        }
    }

    pub fn finish(self) -> Bytes {
        self.buf.freeze()
    }
}

/// Canonical gRPC status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum GrpcCode {
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

impl GrpcCode {
    pub fn from_i32(code: i32) -> Self {
        match code {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => Self::Unknown,
        }
    }

    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Structured `google.rpc.*` error detail unpacked from `grpc-status-details-bin`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrpcErrorDetail {
    RetryInfo {
        retry_delay_ms: u64,
    },
    ResourceInfo {
        resource_type: String,
        resource_name: String,
        owner: String,
        description: String,
    },
    ErrorInfo {
        reason: String,
        domain: String,
        metadata: HashMap<String, String>,
    },
    BadRequest {
        field_violations: Vec<(String, String)>,
    },
    Unknown {
        type_url: String,
        raw_value: Bytes,
    },
}

/// Rich gRPC status decoded from HTTP/2 headers/trailers (`grpc-status`, `grpc-message`, `grpc-status-details-bin`).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("gRPC status {:?} ({}): {}", .code, .raw_code, .message)]
pub struct GrpcStatus {
    pub code: GrpcCode,
    pub raw_code: i32,
    pub message: String,
    pub details: Vec<GrpcErrorDetail>,
}

impl GrpcStatus {
    pub fn new(code: GrpcCode, message: impl Into<String>) -> Self {
        Self {
            code,
            raw_code: code.as_i32(),
            message: message.into(),
            details: Vec::new(),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.code == GrpcCode::Ok
    }

    pub fn retry_delay_ms(&self) -> Option<u64> {
        for detail in &self.details {
            if let GrpcErrorDetail::RetryInfo { retry_delay_ms } = detail {
                return Some(*retry_delay_ms);
            }
        }
        None
    }

    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let raw_code = headers.get("grpc-status")?.to_str().ok()?;
        let parsed_code: i32 = raw_code.trim().parse().ok()?;
        let code = GrpcCode::from_i32(parsed_code);
        let mut message = headers
            .get("grpc-message")
            .and_then(|v| v.to_str().ok())
            .map(percent_decode_grpc_message)
            .unwrap_or_default();
        let mut details = Vec::new();

        if let Some(details_b64) = headers.get("grpc-status-details-bin")
            && let Ok(details_str) = details_b64.to_str()
        {
            let trimmed = details_str.trim();
            let decoded_bytes = BASE64_STD
                .decode(trimmed)
                .or_else(|_| BASE64_NO_PAD.decode(trimmed.trim_end_matches('=')));
            if let Ok(raw_status) = decoded_bytes
                && let Ok((_, status_msg, unpacked)) = decode_google_rpc_status(&raw_status)
            {
                if message.is_empty() && !status_msg.is_empty() {
                    message = status_msg;
                }
                details = unpacked;
            }
        }

        Some(Self {
            code,
            raw_code: parsed_code,
            message,
            details,
        })
    }

    /// Encode a `google.rpc.Status` protobuf payload into standard base64 for `grpc-status-details-bin`.
    pub fn encode_status_details_bin(
        code: GrpcCode,
        message: &str,
        details: &[GrpcErrorDetail],
    ) -> String {
        let mut w = ProtoWriter::new();
        w.write_i32(1, code.as_i32());
        if !message.is_empty() {
            w.write_str(2, message);
        }
        for detail in details {
            match detail {
                GrpcErrorDetail::RetryInfo { retry_delay_ms } => {
                    let secs = (*retry_delay_ms / 1000) as i64;
                    let nanos = ((*retry_delay_ms % 1000) * 1_000_000) as i32;
                    let mut val_w = ProtoWriter::new();
                    val_w.write_nested(1, |dur| {
                        if secs != 0 {
                            dur.write_i64(1, secs);
                        }
                        if nanos != 0 {
                            dur.write_i32(2, nanos);
                        }
                    });
                    let val_bytes = val_w.finish();
                    w.write_nested(3, |any_w| {
                        any_w.write_str(1, "type.googleapis.com/google.rpc.RetryInfo");
                        any_w.write_bytes(2, &val_bytes);
                    });
                }
                GrpcErrorDetail::ResourceInfo {
                    resource_type,
                    resource_name,
                    owner,
                    description,
                } => {
                    let mut val_w = ProtoWriter::new();
                    val_w.write_str(1, resource_type);
                    val_w.write_str(2, resource_name);
                    val_w.write_str(3, owner);
                    val_w.write_str(4, description);
                    let val_bytes = val_w.finish();
                    w.write_nested(3, |any_w| {
                        any_w.write_str(1, "type.googleapis.com/google.rpc.ResourceInfo");
                        any_w.write_bytes(2, &val_bytes);
                    });
                }
                GrpcErrorDetail::ErrorInfo {
                    reason,
                    domain,
                    metadata,
                } => {
                    let mut val_w = ProtoWriter::new();
                    val_w.write_str(1, reason);
                    val_w.write_str(2, domain);
                    let mut sorted_keys: Vec<_> = metadata.keys().collect();
                    sorted_keys.sort();
                    for k in sorted_keys {
                        if let Some(v) = metadata.get(k) {
                            val_w.write_nested(3, |entry| {
                                entry.write_str(1, k);
                                entry.write_str(2, v);
                            });
                        }
                    }
                    let val_bytes = val_w.finish();
                    w.write_nested(3, |any_w| {
                        any_w.write_str(1, "type.googleapis.com/google.rpc.ErrorInfo");
                        any_w.write_bytes(2, &val_bytes);
                    });
                }
                GrpcErrorDetail::BadRequest { field_violations } => {
                    let mut val_w = ProtoWriter::new();
                    for (field, desc) in field_violations {
                        val_w.write_nested(1, |fv| {
                            fv.write_str(1, field);
                            fv.write_str(2, desc);
                        });
                    }
                    let val_bytes = val_w.finish();
                    w.write_nested(3, |any_w| {
                        any_w.write_str(1, "type.googleapis.com/google.rpc.BadRequest");
                        any_w.write_bytes(2, &val_bytes);
                    });
                }
                GrpcErrorDetail::Unknown {
                    type_url,
                    raw_value,
                } => {
                    w.write_nested(3, |any_w| {
                        any_w.write_str(1, type_url);
                        any_w.write_bytes(2, raw_value);
                    });
                }
            }
        }
        BASE64_STD.encode(w.finish())
    }
}

fn percent_decode_grpc_message(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] == b'%'
            && idx + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (
                from_hex_digit(bytes[idx + 1]),
                from_hex_digit(bytes[idx + 2]),
            )
        {
            out.push((hi << 4) | lo);
            idx += 3;
            continue;
        }
        out.push(bytes[idx]);
        idx += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn decode_google_rpc_status(buf: &[u8]) -> Result<(i32, String, Vec<GrpcErrorDetail>), ProtoError> {
    let mut reader = ProtoReader::new(buf);
    let mut code = 0i32;
    let mut message = String::new();
    let mut details = Vec::new();
    while let Some((field, wire_type)) = reader.next_tag()? {
        match (field, wire_type) {
            (1, WireType::Varint) => {
                code = reader.read_i32()?;
            }
            (2, WireType::LengthDelimited) => {
                message = reader.read_str()?.to_string();
            }
            (3, WireType::LengthDelimited) => {
                let mut any_reader = reader.read_nested()?;
                let mut type_url = String::new();
                let mut value_bytes: &[u8] = &[];
                while let Some((af, aw)) = any_reader.next_tag()? {
                    match (af, aw) {
                        (1, WireType::LengthDelimited) => {
                            type_url = any_reader.read_str()?.to_string();
                        }
                        (2, WireType::LengthDelimited) => {
                            value_bytes = any_reader.read_bytes()?;
                        }
                        _ => any_reader.skip_field(aw)?,
                    }
                }
                details.push(decode_google_rpc_any(&type_url, value_bytes)?);
            }
            _ => reader.skip_field(wire_type)?,
        }
    }
    Ok((code, message, details))
}

fn decode_google_rpc_any(type_url: &str, value: &[u8]) -> Result<GrpcErrorDetail, ProtoError> {
    if type_url.ends_with("/google.rpc.RetryInfo") || type_url == "google.rpc.RetryInfo" {
        let mut r = ProtoReader::new(value);
        let mut delay_ms = 0u64;
        while let Some((field, wt)) = r.next_tag()? {
            if field == 1 && wt == WireType::LengthDelimited {
                let mut dur = r.read_nested()?;
                let mut seconds = 0i64;
                let mut nanos = 0i32;
                while let Some((df, dwt)) = dur.next_tag()? {
                    match (df, dwt) {
                        (1, WireType::Varint) => seconds = dur.read_i64()?,
                        (2, WireType::Varint) => nanos = dur.read_i32()?,
                        _ => dur.skip_field(dwt)?,
                    }
                }
                let secs_ms = u64::try_from(seconds.max(0))
                    .unwrap_or(0)
                    .saturating_mul(1000);
                let nanos_ms = u64::try_from(nanos.max(0)).unwrap_or(0) / 1_000_000;
                delay_ms = secs_ms.saturating_add(nanos_ms);
            } else {
                r.skip_field(wt)?;
            }
        }
        return Ok(GrpcErrorDetail::RetryInfo {
            retry_delay_ms: delay_ms,
        });
    }

    if type_url.ends_with("/google.rpc.ResourceInfo") || type_url == "google.rpc.ResourceInfo" {
        let mut r = ProtoReader::new(value);
        let mut resource_type = String::new();
        let mut resource_name = String::new();
        let mut owner = String::new();
        let mut description = String::new();
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::LengthDelimited) => resource_type = r.read_str()?.to_string(),
                (2, WireType::LengthDelimited) => resource_name = r.read_str()?.to_string(),
                (3, WireType::LengthDelimited) => owner = r.read_str()?.to_string(),
                (4, WireType::LengthDelimited) => description = r.read_str()?.to_string(),
                _ => r.skip_field(wt)?,
            }
        }
        return Ok(GrpcErrorDetail::ResourceInfo {
            resource_type,
            resource_name,
            owner,
            description,
        });
    }

    if type_url.ends_with("/google.rpc.ErrorInfo") || type_url == "google.rpc.ErrorInfo" {
        let mut r = ProtoReader::new(value);
        let mut reason = String::new();
        let mut domain = String::new();
        let mut metadata = HashMap::new();
        while let Some((field, wt)) = r.next_tag()? {
            match (field, wt) {
                (1, WireType::LengthDelimited) => reason = r.read_str()?.to_string(),
                (2, WireType::LengthDelimited) => domain = r.read_str()?.to_string(),
                (3, WireType::LengthDelimited) => {
                    let mut entry = r.read_nested()?;
                    let mut k = String::new();
                    let mut v = String::new();
                    while let Some((ef, ewt)) = entry.next_tag()? {
                        match (ef, ewt) {
                            (1, WireType::LengthDelimited) => k = entry.read_str()?.to_string(),
                            (2, WireType::LengthDelimited) => v = entry.read_str()?.to_string(),
                            _ => entry.skip_field(ewt)?,
                        }
                    }
                    metadata.insert(k, v);
                }
                _ => r.skip_field(wt)?,
            }
        }
        return Ok(GrpcErrorDetail::ErrorInfo {
            reason,
            domain,
            metadata,
        });
    }

    if type_url.ends_with("/google.rpc.BadRequest") || type_url == "google.rpc.BadRequest" {
        let mut r = ProtoReader::new(value);
        let mut field_violations = Vec::new();
        while let Some((field, wt)) = r.next_tag()? {
            if field == 1 && wt == WireType::LengthDelimited {
                let mut fv = r.read_nested()?;
                let mut f_name = String::new();
                let mut f_desc = String::new();
                while let Some((vf, vwt)) = fv.next_tag()? {
                    match (vf, vwt) {
                        (1, WireType::LengthDelimited) => f_name = fv.read_str()?.to_string(),
                        (2, WireType::LengthDelimited) => f_desc = fv.read_str()?.to_string(),
                        _ => fv.skip_field(vwt)?,
                    }
                }
                field_violations.push((f_name, f_desc));
            } else {
                r.skip_field(wt)?;
            }
        }
        return Ok(GrpcErrorDetail::BadRequest { field_violations });
    }

    Ok(GrpcErrorDetail::Unknown {
        type_url: type_url.to_string(),
        raw_value: Bytes::copy_from_slice(value),
    })
}

/// Pack a Protobuf message into a 5-byte gRPC Length-Prefixed Message (LPM) frame.
pub fn encode_lpm_frame(
    payload: &[u8],
    compress_gzip: bool,
    max_encoding_size: usize,
) -> Result<Bytes, GrpcStatus> {
    if compress_gzip {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder
            .write_all(payload)
            .map_err(|e| GrpcStatus::new(GrpcCode::Internal, format!("gzip encode error: {e}")))?;
        let compressed = encoder
            .finish()
            .map_err(|e| GrpcStatus::new(GrpcCode::Internal, format!("gzip finish error: {e}")))?;
        if compressed.len() > max_encoding_size || compressed.len() > u32::MAX as usize {
            return Err(GrpcStatus::new(
                GrpcCode::ResourceExhausted,
                format!(
                    "gRPC outbound compressed message size {} exceeds limit {}",
                    compressed.len(),
                    max_encoding_size
                ),
            ));
        }
        let mut out = BytesMut::with_capacity(5 + compressed.len());
        out.put_u8(1);
        out.put_u32(compressed.len() as u32);
        out.extend_from_slice(&compressed);
        Ok(out.freeze())
    } else {
        if payload.len() > max_encoding_size || payload.len() > u32::MAX as usize {
            return Err(GrpcStatus::new(
                GrpcCode::ResourceExhausted,
                format!(
                    "gRPC outbound message size {} exceeds limit {}",
                    payload.len(),
                    max_encoding_size
                ),
            ));
        }
        let mut out = BytesMut::with_capacity(5 + payload.len());
        out.put_u8(0);
        out.put_u32(payload.len() as u32);
        out.extend_from_slice(payload);
        Ok(out.freeze())
    }
}

/// Streaming gRPC Length-Prefixed Message (LPM) frame decoder supporting `128 MiB` frames and `gzip`.
#[derive(Debug)]
pub struct LpmFrameDecoder {
    buf: BytesMut,
    max_decoding_message_size: usize,
    grpc_encoding: Option<String>,
}

impl LpmFrameDecoder {
    pub fn new(max_decoding_message_size: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(8192),
            max_decoding_message_size,
            grpc_encoding: None,
        }
    }

    pub fn with_encoding(max_decoding_message_size: usize, grpc_encoding: Option<&str>) -> Self {
        Self {
            buf: BytesMut::with_capacity(8192),
            max_decoding_message_size,
            grpc_encoding: grpc_encoding.map(|s| s.trim().to_ascii_lowercase()),
        }
    }

    pub fn set_encoding(&mut self, grpc_encoding: Option<&str>) {
        self.grpc_encoding = grpc_encoding.map(|s| s.trim().to_ascii_lowercase());
    }

    pub fn push_chunk(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn next_frame(&mut self) -> Result<Option<Bytes>, GrpcStatus> {
        if self.buf.len() < 5 {
            return Ok(None);
        }
        let compressed_flag = self.buf[0];
        if compressed_flag > 1 {
            return Err(GrpcStatus::new(
                GrpcCode::Internal,
                format!("invalid gRPC LPM compression flag: {compressed_flag}"),
            ));
        }
        let frame_len =
            u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
        if frame_len > self.max_decoding_message_size {
            return Err(GrpcStatus::new(
                GrpcCode::ResourceExhausted,
                format!(
                    "gRPC inbound frame length {frame_len} exceeds max_decoding_message_size {}",
                    self.max_decoding_message_size
                ),
            ));
        }
        if self.buf.len() < 5 + frame_len {
            return Ok(None);
        }
        self.buf.advance(5);
        let raw_frame = self.buf.split_to(frame_len).freeze();
        if compressed_flag == 0 {
            return Ok(Some(raw_frame));
        }

        let encoding = self.grpc_encoding.as_deref().unwrap_or("gzip");
        if encoding != "gzip" && encoding != "identity" {
            return Err(GrpcStatus::new(
                GrpcCode::Unimplemented,
                format!("unsupported grpc-encoding: {encoding}"),
            ));
        }

        let limit = self.max_decoding_message_size;
        let decoder = GzDecoder::new(Cursor::new(raw_frame));
        let mut bounded = decoder.take((limit as u64).saturating_add(1));
        let mut decompressed = Vec::with_capacity(frame_len.saturating_mul(2).min(limit));
        bounded.read_to_end(&mut decompressed).map_err(|e| {
            GrpcStatus::new(
                GrpcCode::Internal,
                format!("gRPC gzip decompression failed: {e}"),
            )
        })?;
        if decompressed.len() > limit {
            return Err(GrpcStatus::new(
                GrpcCode::ResourceExhausted,
                format!("gRPC decompressed gzip payload exceeds max_decoding_message_size {limit}"),
            ));
        }
        Ok(Some(Bytes::from(decompressed)))
    }
}

/// Format a [`Duration`] into the gRPC `grpc-timeout` wire header format (`1500m`, `30S`, etc.).
pub fn format_grpc_timeout(timeout: Duration) -> String {
    let total_nanos = timeout.as_nanos();
    if total_nanos == 0 {
        return "0m".to_string();
    }
    if total_nanos.is_multiple_of(1_000_000) {
        let millis = total_nanos / 1_000_000;
        if millis <= 99_999_999 {
            return format!("{millis}m");
        }
    }
    if total_nanos.is_multiple_of(1_000) {
        let micros = total_nanos / 1_000;
        if micros <= 99_999_999 {
            return format!("{micros}u");
        }
    }
    if total_nanos <= 99_999_999 {
        return format!("{total_nanos}n");
    }
    let secs = timeout.as_secs();
    if secs <= 99_999_999 {
        return format!("{secs}S");
    }
    let mins = secs / 60;
    if mins <= 99_999_999 {
        return format!("{mins}M");
    }
    format!("{}H", mins / 60)
}

/// Generate an RFC 9562 version 4 UUID string via `vertebra_core::id::uuid4_str`.
pub fn generate_request_uuid_v4() -> String {
    vertebra_core::id::uuid4_str()
}

/// Configuration for [`GrpcChannelPool`].
#[derive(Debug, Clone)]
pub struct GrpcPoolConfig {
    pub endpoint: String,
    pub pool_size: usize,
    pub initial_stream_window_size: u32,
    pub initial_connection_window_size: u32,
    pub keep_alive_interval: Duration,
    pub keep_alive_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_decoding_message_size: usize,
    pub max_encoding_message_size: usize,
    pub accept_gzip: bool,
    pub send_gzip: bool,
    pub ca_pem: Option<Vec<u8>>,
    pub tls_domain_override: Option<String>,
    pub user_agent: String,
}

impl GrpcPoolConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            pool_size: 4,
            initial_stream_window_size: DEFAULT_INITIAL_STREAM_WINDOW_SIZE,
            initial_connection_window_size: DEFAULT_INITIAL_CONNECTION_WINDOW_SIZE,
            keep_alive_interval: Duration::from_secs(30),
            keep_alive_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            max_decoding_message_size: DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE,
            max_encoding_message_size: DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE,
            accept_gzip: true,
            send_gzip: false,
            ca_pem: None,
            tls_domain_override: None,
            user_agent: "vertebra-grpc/0.3".to_string(),
        }
    }
}

/// Per-RPC metadata headers attached to outbound gRPC requests.
#[derive(Debug, Clone, Default)]
pub struct GrpcRequestMetadata {
    pub bearer_token: Option<String>,
    pub routing_params: Option<String>,
    pub quota_project: Option<String>,
    pub spanner_request_id: Option<String>,
    pub timeout: Option<Duration>,
    pub compress_gzip: bool,
    pub extra_headers: Vec<(String, String)>,
}

/// Request body supporting both single-frame unary/server-streaming calls and mpsc bidi-streaming calls.
pub enum GrpcRequestBody {
    Once(Option<Bytes>),
    Stream(mpsc::Receiver<Bytes>),
}

impl Body for GrpcRequestBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut *self {
            GrpcRequestBody::Once(slot) => Poll::Ready(slot.take().map(|b| Ok(Frame::data(b)))),
            GrpcRequestBody::Stream(rx) => match rx.poll_recv(cx) {
                Poll::Ready(Some(chunk)) => Poll::Ready(Some(Ok(Frame::data(chunk)))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            GrpcRequestBody::Once(slot) => slot.is_none(),
            GrpcRequestBody::Stream(_) => false,
        }
    }
}

/// Inbound server-streaming or bidi-streaming gRPC message stream.
pub struct GrpcFrameStream {
    response_headers: HeaderMap,
    trailing_metadata: Option<HeaderMap>,
    body: Option<Incoming>,
    decoder: LpmFrameDecoder,
    finished: bool,
}

impl GrpcFrameStream {
    pub fn response_headers(&self) -> &HeaderMap {
        &self.response_headers
    }

    pub fn trailing_metadata(&self) -> Option<&HeaderMap> {
        self.trailing_metadata.as_ref()
    }

    pub async fn next_message(&mut self) -> Result<Option<Bytes>, GrpcStatus> {
        loop {
            if let Some(frame) = self.decoder.next_frame()? {
                return Ok(Some(frame));
            }
            if self.finished {
                if !self.decoder.is_empty() {
                    return Err(GrpcStatus::new(
                        GrpcCode::Internal,
                        "truncated gRPC LPM frame at end of stream",
                    ));
                }
                return Ok(None);
            }
            let Some(ref mut body) = self.body else {
                self.finished = true;
                continue;
            };
            match body.frame().await {
                Some(Ok(frame)) => {
                    if frame.is_data() {
                        if let Ok(data) = frame.into_data() {
                            self.decoder.push_chunk(&data);
                        }
                    } else if frame.is_trailers()
                        && let Ok(trailers) = frame.into_trailers()
                    {
                        if let Some(status) = GrpcStatus::from_headers(&trailers)
                            && !status.is_ok()
                        {
                            self.trailing_metadata = Some(trailers);
                            self.finished = true;
                            return Err(status);
                        }
                        self.trailing_metadata = Some(trailers);
                    }
                }
                Some(Err(err)) => {
                    self.finished = true;
                    return Err(GrpcStatus::new(
                        GrpcCode::Unavailable,
                        format!("HTTP/2 stream body error: {err}"),
                    ));
                }
                None => {
                    self.finished = true;
                }
            }
        }
    }
}

/// Multiplexed HTTP/2 (`h2` TLS and `h2c` cleartext) channel pool for Google Cloud gRPC services.
pub struct GrpcChannelPool {
    config: GrpcPoolConfig,
    scheme: String,
    host: String,
    port: u16,
    authority: String,
    tls_connector: Option<TlsConnector>,
    slots: Vec<Mutex<Option<SendRequest<GrpcRequestBody>>>>,
    rr_cursor: AtomicUsize,
}

impl GrpcChannelPool {
    pub fn new(config: GrpcPoolConfig) -> Result<Self, GrpcStatus> {
        let normalized = if config.endpoint.contains("://") {
            config.endpoint.clone()
        } else {
            format!("https://{}", config.endpoint)
        };
        let parsed = Url::parse(&normalized).map_err(|e| {
            GrpcStatus::new(
                GrpcCode::InvalidArgument,
                format!("invalid gRPC endpoint URL '{}': {e}", config.endpoint),
            )
        })?;
        let scheme = parsed.scheme().to_ascii_lowercase();
        if scheme != "http" && scheme != "https" {
            return Err(GrpcStatus::new(
                GrpcCode::InvalidArgument,
                format!("unsupported gRPC endpoint scheme: {scheme}"),
            ));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| {
                GrpcStatus::new(
                    GrpcCode::InvalidArgument,
                    format!("gRPC endpoint missing host: {}", config.endpoint),
                )
            })?
            .to_string();
        let default_port = if scheme == "https" { 443 } else { 80 };
        let port = parsed.port().unwrap_or(default_port);
        let authority = if (scheme == "https" && port == 443) || (scheme == "http" && port == 80) {
            host.clone()
        } else {
            format!("{host}:{port}")
        };

        let tls_connector = if scheme == "https" {
            let mut root_store = RootCertStore::empty();
            root_store.add_parsable_certificates(
                webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter().cloned(),
            );
            if let Some(ref pem_bytes) = config.ca_pem {
                let mut reader = std::io::BufReader::new(pem_bytes.as_slice());
                for cert in rustls_pemfile::certs(&mut reader) {
                    let cert = cert.map_err(|e| {
                        GrpcStatus::new(
                            GrpcCode::InvalidArgument,
                            format!("invalid custom CA PEM certificate: {e}"),
                        )
                    })?;
                    root_store.add(cert).map_err(|e| {
                        GrpcStatus::new(
                            GrpcCode::InvalidArgument,
                            format!("failed adding custom CA certificate: {e}"),
                        )
                    })?;
                }
            }
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let mut tls_cfg = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    GrpcStatus::new(
                        GrpcCode::Internal,
                        format!("TLS protocol config error: {e}"),
                    )
                })?
                .with_root_certificates(root_store)
                .with_no_client_auth();
            tls_cfg.alpn_protocols = vec![b"h2".to_vec()];
            Some(TlsConnector::from(Arc::new(tls_cfg)))
        } else {
            None
        };

        let pool_size = config.pool_size.max(1);
        let mut slots = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            slots.push(Mutex::new(None));
        }

        Ok(Self {
            config,
            scheme,
            host,
            port,
            authority,
            tls_connector,
            slots,
            rr_cursor: AtomicUsize::new(0),
        })
    }

    pub fn config(&self) -> &GrpcPoolConfig {
        &self.config
    }

    async fn acquire_sender(&self) -> Result<(usize, SendRequest<GrpcRequestBody>), GrpcStatus> {
        let idx = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let mut guard = self.slots[idx].lock().await;
        if let Some(ref mut existing) = *guard {
            if existing.is_ready() {
                return Ok((idx, existing.clone()));
            }
            if std::future::poll_fn(|cx| existing.poll_ready(cx))
                .await
                .is_ok()
            {
                return Ok((idx, existing.clone()));
            }
        }
        let sender = self.dial_new_connection().await?;
        *guard = Some(sender.clone());
        Ok((idx, sender))
    }

    async fn reconnect_slot(&self, idx: usize) -> Result<SendRequest<GrpcRequestBody>, GrpcStatus> {
        let mut guard = self.slots[idx].lock().await;
        let sender = self.dial_new_connection().await?;
        *guard = Some(sender.clone());
        Ok(sender)
    }

    async fn dial_new_connection(&self) -> Result<SendRequest<GrpcRequestBody>, GrpcStatus> {
        let addr = format!("{}:{}", self.host, self.port);
        let tcp = tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| {
                GrpcStatus::new(
                    GrpcCode::DeadlineExceeded,
                    format!("timed out connecting to gRPC endpoint {addr}"),
                )
            })?
            .map_err(|e| {
                GrpcStatus::new(
                    GrpcCode::Unavailable,
                    format!("failed connecting to gRPC endpoint {addr}: {e}"),
                )
            })?;
        let _ = tcp.set_nodelay(true);

        let mut h2_builder = http2::Builder::new(TokioExecutor::new());
        h2_builder
            .timer(TokioTimer::new())
            .initial_stream_window_size(self.config.initial_stream_window_size)
            .initial_connection_window_size(self.config.initial_connection_window_size)
            .adaptive_window(true)
            .keep_alive_interval(Some(self.config.keep_alive_interval))
            .keep_alive_timeout(self.config.keep_alive_timeout)
            .keep_alive_while_idle(true);

        if let Some(ref connector) = self.tls_connector {
            let sni_host = self
                .config
                .tls_domain_override
                .as_deref()
                .unwrap_or(&self.host)
                .to_string();
            let server_name = ServerName::try_from(sni_host.clone()).map_err(|e| {
                GrpcStatus::new(
                    GrpcCode::InvalidArgument,
                    format!("invalid TLS ServerName '{sni_host}': {e}"),
                )
            })?;
            let tls_stream = tokio::time::timeout(
                self.config.connect_timeout,
                connector.connect(server_name, tcp),
            )
            .await
            .map_err(|_| {
                GrpcStatus::new(
                    GrpcCode::DeadlineExceeded,
                    format!("TLS handshake timed out for {addr}"),
                )
            })?
            .map_err(|e| {
                GrpcStatus::new(
                    GrpcCode::Unavailable,
                    format!("TLS handshake failed for {addr}: {e}"),
                )
            })?;
            let (sender, conn) = h2_builder
                .handshake(TokioIo::new(tls_stream))
                .await
                .map_err(|e| {
                    GrpcStatus::new(
                        GrpcCode::Unavailable,
                        format!("HTTP/2 TLS handshake failed for {addr}: {e}"),
                    )
                })?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(sender)
        } else {
            let (sender, conn) = h2_builder.handshake(TokioIo::new(tcp)).await.map_err(|e| {
                GrpcStatus::new(
                    GrpcCode::Unavailable,
                    format!("HTTP/2 h2c handshake failed for {addr}: {e}"),
                )
            })?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(sender)
        }
    }

    fn build_http_request(
        &self,
        path: &str,
        body: GrpcRequestBody,
        meta: &GrpcRequestMetadata,
    ) -> Result<Request<GrpcRequestBody>, GrpcStatus> {
        let normalized_path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        let uri_str = format!("{}://{}{}", self.scheme, self.authority, normalized_path);
        let uri: Uri = uri_str.parse().map_err(|e| {
            GrpcStatus::new(
                GrpcCode::InvalidArgument,
                format!("invalid gRPC request URI '{uri_str}': {e}"),
            )
        })?;

        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(CONTENT_TYPE, "application/grpc+proto")
            .header(TE, "trailers")
            .header(USER_AGENT, &self.config.user_agent);

        if self.config.accept_gzip {
            builder = builder.header("grpc-accept-encoding", "gzip");
        }
        if meta.compress_gzip || self.config.send_gzip {
            builder = builder.header("grpc-encoding", "gzip");
        }
        if let Some(ref token) = meta.bearer_token
            && !token.is_empty()
        {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        if let Some(ref routing) = meta.routing_params
            && !routing.is_empty()
        {
            builder = builder.header("x-goog-request-params", routing.as_str());
        }
        if let Some(ref qp) = meta.quota_project
            && !qp.is_empty()
        {
            builder = builder.header("x-goog-user-project", qp.as_str());
        }
        if let Some(ref req_id) = meta.spanner_request_id
            && !req_id.is_empty()
        {
            builder = builder.header("x-goog-spanner-request-id", req_id.as_str());
        }
        if let Some(timeout) = meta.timeout {
            builder = builder.header("grpc-timeout", format_grpc_timeout(timeout));
        }

        let mut req = builder.body(body).map_err(|e| {
            GrpcStatus::new(
                GrpcCode::Internal,
                format!("failed building HTTP/2 gRPC request: {e}"),
            )
        })?;

        for (k, v) in &meta.extra_headers {
            if let (Ok(name), Ok(val)) = (
                HeaderName::from_bytes(k.as_bytes()),
                HeaderValue::from_str(v),
            ) {
                req.headers_mut().insert(name, val);
            }
        }
        Ok(req)
    }

    async fn dispatch_stream(
        &self,
        path: &str,
        body: GrpcRequestBody,
        meta: &GrpcRequestMetadata,
    ) -> Result<GrpcFrameStream, GrpcStatus> {
        let (idx, mut sender) = self.acquire_sender().await?;
        let req = self.build_http_request(path, body, meta)?;
        let resp = match sender.send_request(req).await {
            Ok(resp) => resp,
            Err(err) => {
                let _ = self.reconnect_slot(idx).await;
                return Err(GrpcStatus::new(
                    GrpcCode::Unavailable,
                    format!("HTTP/2 channel closed during streaming request dispatch: {err}"),
                ));
            }
        };
        self.build_frame_stream(path, resp)
    }

    /// Execute a unary gRPC call, returning `(response_protobuf_bytes, response_headers)`.
    pub async fn unary(
        &self,
        path: &str,
        req_payload: Bytes,
        meta: &GrpcRequestMetadata,
    ) -> Result<(Bytes, HeaderMap), GrpcStatus> {
        let frame = encode_lpm_frame(
            &req_payload,
            meta.compress_gzip || self.config.send_gzip,
            self.config.max_encoding_message_size,
        )?;
        let mut stream = self
            .server_streaming_from_lpm_frame(path, frame, meta)
            .await?;
        let first = stream.next_message().await?.ok_or_else(|| {
            if let Some(trailers) = stream.trailing_metadata()
                && let Some(status) = GrpcStatus::from_headers(trailers)
                && !status.is_ok()
            {
                return status;
            }
            if let Some(status) = GrpcStatus::from_headers(stream.response_headers())
                && !status.is_ok()
            {
                return status;
            }
            GrpcStatus::new(
                GrpcCode::Internal,
                format!("unary gRPC call to {path} completed without a response message"),
            )
        })?;
        while let Some(_extra) = stream.next_message().await? {}
        let mut combined_headers = stream.response_headers().clone();
        if let Some(trailers) = stream.trailing_metadata() {
            for (k, v) in trailers {
                combined_headers.insert(k.clone(), v.clone());
            }
        }
        Ok((first, combined_headers))
    }

    /// Execute a server-streaming gRPC call (`req_payload` is raw unframed Protobuf bytes).
    pub async fn server_streaming(
        &self,
        path: &str,
        req_payload: Bytes,
        meta: &GrpcRequestMetadata,
    ) -> Result<GrpcFrameStream, GrpcStatus> {
        let frame = encode_lpm_frame(
            &req_payload,
            meta.compress_gzip || self.config.send_gzip,
            self.config.max_encoding_message_size,
        )?;
        self.server_streaming_from_lpm_frame(path, frame, meta)
            .await
    }

    async fn server_streaming_from_lpm_frame(
        &self,
        path: &str,
        lpm_frame: Bytes,
        meta: &GrpcRequestMetadata,
    ) -> Result<GrpcFrameStream, GrpcStatus> {
        let (idx, mut sender) = self.acquire_sender().await?;
        let req =
            self.build_http_request(path, GrpcRequestBody::Once(Some(lpm_frame.clone())), meta)?;
        let resp = match sender.send_request(req).await {
            Ok(r) => r,
            Err(_) => {
                let mut fresh = self.reconnect_slot(idx).await?;
                let retry_req =
                    self.build_http_request(path, GrpcRequestBody::Once(Some(lpm_frame)), meta)?;
                fresh.send_request(retry_req).await.map_err(|e| {
                    GrpcStatus::new(
                        GrpcCode::Unavailable,
                        format!("HTTP/2 gRPC request failed after reconnect: {e}"),
                    )
                })?
            }
        };
        self.build_frame_stream(path, resp)
    }

    /// Execute a bidirectional-streaming gRPC call where `req_rx` yields raw unframed Protobuf messages.
    pub async fn bidi_streaming(
        &self,
        path: &str,
        mut req_rx: mpsc::Receiver<Bytes>,
        meta: &GrpcRequestMetadata,
    ) -> Result<GrpcFrameStream, GrpcStatus> {
        let (framed_tx, framed_rx) = mpsc::channel::<Bytes>(32);
        let compress_gzip = meta.compress_gzip || self.config.send_gzip;
        let max_enc = self.config.max_encoding_message_size;
        tokio::spawn(async move {
            while let Some(raw_msg) = req_rx.recv().await {
                let frame = match encode_lpm_frame(&raw_msg, compress_gzip, max_enc) {
                    Ok(f) => f,
                    Err(_) => break,
                };
                if framed_tx.send(frame).await.is_err() {
                    break;
                }
            }
        });
        self.dispatch_stream(path, GrpcRequestBody::Stream(framed_rx), meta)
            .await
    }

    fn build_frame_stream(
        &self,
        path: &str,
        resp: http::Response<Incoming>,
    ) -> Result<GrpcFrameStream, GrpcStatus> {
        let http_status = resp.status();
        let headers = resp.headers().clone();
        if let Some(grpc_status) = GrpcStatus::from_headers(&headers)
            && !grpc_status.is_ok()
        {
            return Err(grpc_status);
        }
        if http_status != http::StatusCode::OK {
            let code = match http_status.as_u16() {
                400 => GrpcCode::Internal,
                401 => GrpcCode::Unauthenticated,
                403 => GrpcCode::PermissionDenied,
                404 => GrpcCode::Unimplemented,
                429 | 502 | 503 | 504 => GrpcCode::Unavailable,
                _ => GrpcCode::Unknown,
            };
            return Err(GrpcStatus::new(
                code,
                format!("unexpected HTTP status {http_status} on gRPC call to {path}"),
            ));
        }
        let grpc_encoding = headers.get("grpc-encoding").and_then(|v| v.to_str().ok());
        let decoder =
            LpmFrameDecoder::with_encoding(self.config.max_decoding_message_size, grpc_encoding);
        let (_, incoming) = resp.into_parts();
        Ok(GrpcFrameStream {
            response_headers: headers,
            trailing_metadata: None,
            body: Some(incoming),
            decoder,
            finished: false,
        })
    }
}
