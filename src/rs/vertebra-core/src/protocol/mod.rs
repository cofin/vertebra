//! FlatBuffers protocol definitions for IPC control plane.
//!
//! This module provides zero-parse-cost message headers using FlatBuffers.
//! The control plane (headers, metadata) uses FlatBuffers for random-access reads,
//! while dynamic payloads remain MessagePack-encoded for flexibility.
//!
//! # Wire Format
//!
//! Messages are structured as:
//! ```text
//! ┌─────────────────────┬─────────────────────┐
//! │  FlatBuffer Header  │  MessagePack Payload │
//! │     (16 bytes)      │    (variable len)    │
//! └─────────────────────┴─────────────────────┘
//! ```
//!
//! The header contains:
//! - `id`: 64-bit correlation ID for multiplexing
//! - `flags`: 32-bit flags (priority, compression, etc.)
//! - `payload_len`: 32-bit length of the MessagePack payload

use std::mem::size_of;

/// Fixed-size request header (16 bytes, cache-line friendly).
///
/// This header is designed for zero-copy reads directly from shared memory.
/// All fields are little-endian for consistent cross-platform behavior.
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestHeader {
    /// Correlation ID for request/response matching in multiplexed scenarios.
    pub id: u64,
    /// Request flags (see `flags` module for constants).
    pub flags: u32,
    /// Length of the MessagePack payload following this header.
    pub payload_len: u32,
}

impl RequestHeader {
    /// Header size in bytes.
    pub const SIZE: usize = size_of::<Self>();

    /// Create a new request header.
    #[inline]
    pub const fn new(id: u64, flags: u32, payload_len: u32) -> Self {
        Self {
            id,
            flags,
            payload_len,
        }
    }

    /// Read a header from a byte slice (zero-copy on little-endian platforms).
    ///
    /// # Safety
    /// The slice must be at least `SIZE` bytes and properly aligned.
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Option<&Self> {
        if bytes.len() < Self::SIZE {
            return None;
        }
        // Check alignment
        let ptr = bytes.as_ptr();
        if ptr.align_offset(align_of::<Self>()) != 0 {
            return None;
        }
        // SAFETY: We've verified length and alignment
        Some(unsafe { &*(ptr as *const Self) })
    }

    /// Write header to a byte slice.
    #[inline]
    pub fn write_to(&self, bytes: &mut [u8]) -> bool {
        if bytes.len() < Self::SIZE {
            return false;
        }
        let src = unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, Self::SIZE)
        };
        bytes[..Self::SIZE].copy_from_slice(src);
        true
    }

    /// Convert to bytes (copies on big-endian platforms).
    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        self.write_to(&mut bytes);
        bytes
    }
}

/// Fixed-size response header (16 bytes).
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResponseHeader {
    /// Correlation ID (matches the request).
    pub id: u64,
    /// Response flags.
    pub flags: u32,
    /// Length of the MessagePack payload following this header.
    pub payload_len: u32,
}

impl ResponseHeader {
    /// Header size in bytes.
    pub const SIZE: usize = size_of::<Self>();

    /// Create a new response header.
    #[inline]
    pub const fn new(id: u64, flags: u32, payload_len: u32) -> Self {
        Self {
            id,
            flags,
            payload_len,
        }
    }

    /// Read a header from a byte slice (zero-copy on little-endian platforms).
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Option<&Self> {
        if bytes.len() < Self::SIZE {
            return None;
        }
        let ptr = bytes.as_ptr();
        if ptr.align_offset(align_of::<Self>()) != 0 {
            return None;
        }
        Some(unsafe { &*(ptr as *const Self) })
    }

    /// Write header to a byte slice.
    #[inline]
    pub fn write_to(&self, bytes: &mut [u8]) -> bool {
        if bytes.len() < Self::SIZE {
            return false;
        }
        let src = unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, Self::SIZE)
        };
        bytes[..Self::SIZE].copy_from_slice(src);
        true
    }

    /// Convert to bytes.
    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        self.write_to(&mut bytes);
        bytes
    }
}

/// Request flags for IPC messages.
pub mod flags {
    /// No special flags.
    pub const NONE: u32 = 0;
    /// Request requires immediate processing (skip queue).
    pub const PRIORITY: u32 = 1 << 0;
    /// Payload is compressed.
    pub const COMPRESSED: u32 = 1 << 1;
    /// Request expects no response.
    pub const FIRE_AND_FORGET: u32 = 1 << 2;
    /// Request is part of a batch.
    pub const BATCH: u32 = 1 << 3;
    /// Response indicates success.
    pub const OK: u32 = 1 << 4;
    /// Response contains an error.
    pub const ERROR: u32 = 1 << 5;
}

/// Response flags for IPC messages.
pub mod response_flags {
    pub use super::flags::{COMPRESSED, ERROR, NONE, OK};
}

/// Control message types for broker coordination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlMessageKind {
    /// Client connecting to broker.
    Connect,
    /// Client disconnecting.
    Disconnect,
    /// Heartbeat/keepalive.
    Heartbeat,
    /// Shutdown request.
    Shutdown,
}

impl ControlMessageKind {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "connect" => Some(Self::Connect),
            "disconnect" => Some(Self::Disconnect),
            "heartbeat" => Some(Self::Heartbeat),
            "shutdown" => Some(Self::Shutdown),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Disconnect => "disconnect",
            Self::Heartbeat => "heartbeat",
            Self::Shutdown => "shutdown",
        }
    }
}

use std::mem::align_of;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_header_size() {
        assert_eq!(RequestHeader::SIZE, 16);
        assert_eq!(ResponseHeader::SIZE, 16);
    }

    #[test]
    fn test_request_header_roundtrip() {
        let header = RequestHeader::new(42, flags::PRIORITY | flags::COMPRESSED, 1024);
        assert_eq!(header.id, 42);
        assert_eq!(header.flags, flags::PRIORITY | flags::COMPRESSED);
        assert_eq!(header.payload_len, 1024);

        let bytes = header.to_bytes();
        let restored = RequestHeader::from_bytes(&bytes).unwrap();
        assert_eq!(*restored, header);
    }

    #[test]
    fn test_response_header_roundtrip() {
        let header = ResponseHeader::new(123, flags::OK, 512);
        let bytes = header.to_bytes();
        let restored = ResponseHeader::from_bytes(&bytes).unwrap();
        assert_eq!(*restored, header);
    }

    #[test]
    fn test_header_alignment() {
        // Verify headers are properly aligned
        let header = RequestHeader::default();
        let ptr = &header as *const _ as usize;
        assert_eq!(ptr % 8, 0, "Header should be 8-byte aligned");
    }

    #[test]
    fn test_control_message_kind() {
        assert_eq!(ControlMessageKind::Connect.as_str(), "connect");
        assert_eq!(
            ControlMessageKind::from_str("disconnect"),
            Some(ControlMessageKind::Disconnect)
        );
        assert_eq!(ControlMessageKind::from_str("invalid"), None);
    }

    #[test]
    fn test_flags() {
        let f = flags::PRIORITY | flags::BATCH;
        assert!(f & flags::PRIORITY != 0);
        assert!(f & flags::BATCH != 0);
        assert!(f & flags::COMPRESSED == 0);
    }
}
