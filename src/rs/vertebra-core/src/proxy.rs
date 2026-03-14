//! C-ABI proxy structs for HTTP requests and responses.
//!
//! These types cross the FFI boundary between Rust and Python. They are
//! `#[repr(C)]` so Python (via PyO3 or ctypes) can read fields at fixed,
//! well-defined offsets without calling back into Rust for each access.
//!
//! # Stability Contract
//!
//! Field order, size, and alignment are FROZEN once Ch8 (FFI Bridge) ships.
//! Any breaking change requires a version bump in `ShmHeader::abi_version`.
//!
//! # Memory Layout: `RequestInfo`
//!
//! ```text
//! Offset  Size  Field
//! ──────  ────  ────────────────────────────────────────────────────────
//!      0     1  method: HttpMethod  (u8 repr)
//!      1     7  _pad: [u8; 7]       (alignment padding to 8-byte boundary)
//!      8     8  handler_id: u64
//!     16     8  path_offset: u64    (byte offset into path slab)
//!     24     8  path_len: u64
//!     32     8  headers_offset: u64 (byte offset into headers slab)
//!     40     8  body_offset: u64    (byte offset into body slab)
//!     48     8  body_len: u64
//!     56     8  correlation_id: u64
//! ──────
//!     64 bytes total
//! ```
//!
//! # Memory Layout: `ResponseInfo`
//!
//! ```text
//! Offset  Size  Field
//! ──────  ────  ────────────────────────────────────────────────────────
//!      0     2  status: u16         (HTTP status code, e.g. 200)
//!      2     6  _pad: [u8; 6]       (alignment padding)
//!      8     8  correlation_id: u64 (echoed from RequestInfo)
//!     16     8  headers_offset: u64 (byte offset into response headers slab)
//!     24     8  headers_len: u64
//!     32     8  body_offset: u64    (byte offset into response body slab)
//!     40     8  body_len: u64
//!     48    16  _reserved: [u8; 16] (future use — zero on write)
//! ──────
//!     64 bytes total
//! ```

/// HTTP method enum, represented as a single byte for C-ABI compactness.
///
/// # Repr
/// `u8` — fits in one byte, no padding wasted.
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum HttpMethod {
    /// GET
    GET = 0,
    /// POST
    POST = 1,
    /// PUT
    PUT = 2,
    /// DELETE
    DELETE = 3,
    /// PATCH
    PATCH = 4,
    /// HEAD
    HEAD = 5,
    /// OPTIONS
    OPTIONS = 6,
    /// CONNECT
    CONNECT = 7,
    /// TRACE
    TRACE = 8,
    /// Unknown or extension method
    OTHER = 255,
}

impl HttpMethod {
    /// Parse an HTTP method byte string into an `HttpMethod`.
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"GET" => Self::GET,
            b"POST" => Self::POST,
            b"PUT" => Self::PUT,
            b"DELETE" => Self::DELETE,
            b"PATCH" => Self::PATCH,
            b"HEAD" => Self::HEAD,
            b"OPTIONS" => Self::OPTIONS,
            b"CONNECT" => Self::CONNECT,
            b"TRACE" => Self::TRACE,
            _ => Self::OTHER,
        }
    }
}

/// Compact HTTP request descriptor passed over the ring buffer.
///
/// All variable-length data (path, headers, body) is stored in external slabs.
/// `RequestInfo` carries only fixed-size metadata: offsets, lengths, method,
/// handler ID, and correlation ID.
///
/// See module-level documentation for the exact memory layout.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct RequestInfo {
    /// HTTP method (1 byte).
    pub method: HttpMethod,
    /// Alignment padding — MUST be zero on write.
    pub _pad: [u8; 7],
    /// Handler ID assigned by the native router (maps to a Python callable).
    pub handler_id: u64,
    /// Byte offset of the request path within the path slab.
    pub path_offset: u64,
    /// Byte length of the request path.
    pub path_len: u64,
    /// Byte offset of serialised request headers within the headers slab.
    pub headers_offset: u64,
    /// Byte offset of the request body within the body slab.
    pub body_offset: u64,
    /// Byte length of the request body.
    pub body_len: u64,
    /// Monotonic correlation identifier set by the HTTP collector.
    pub correlation_id: u64,
}

/// Compact HTTP response descriptor returned by Python over the ring buffer.
///
/// Python fills this struct and passes it back to the Rust response writer,
/// which reads the header/body slabs and writes them to the socket.
///
/// See module-level documentation for the exact memory layout.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ResponseInfo {
    /// HTTP status code (e.g. 200, 404).
    pub status: u16,
    /// Alignment padding — MUST be zero on write.
    pub _pad: [u8; 6],
    /// Echoed `RequestInfo::correlation_id` — used to match response to request.
    pub correlation_id: u64,
    /// Byte offset of response headers within the response headers slab.
    pub headers_offset: u64,
    /// Byte length of response headers.
    pub headers_len: u64,
    /// Byte offset of response body within the response body slab.
    pub body_offset: u64,
    /// Byte length of response body.
    pub body_len: u64,
    /// Reserved for future use — always zero.
    pub _reserved: [u8; 16],
}

impl ResponseInfo {
    /// Construct a minimal `ResponseInfo` for a status-only response (e.g. 204).
    pub fn status_only(status: u16, correlation_id: u64) -> Self {
        Self {
            status,
            _pad: [0u8; 6],
            correlation_id,
            headers_offset: 0,
            headers_len: 0,
            body_offset: 0,
            body_len: 0,
            _reserved: [0u8; 16],
        }
    }
}

// SAFETY: Both structs contain only POD fields (u8, u16, u64 arrays).
// They are designed for cross-thread / cross-process sharing via SHM.
unsafe impl Send for RequestInfo {}
unsafe impl Sync for RequestInfo {}
unsafe impl Send for ResponseInfo {}
unsafe impl Sync for ResponseInfo {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_info_size_is_64() {
        assert_eq!(std::mem::size_of::<RequestInfo>(), 64);
    }

    #[test]
    fn response_info_size_is_64() {
        assert_eq!(std::mem::size_of::<ResponseInfo>(), 64);
    }

    #[test]
    fn request_info_field_offsets() {
        // Verify field offsets match the documented layout using offset_of!
        // (stabilized in Rust 1.77).
        assert_eq!(std::mem::offset_of!(RequestInfo, method), 0);
        assert_eq!(std::mem::offset_of!(RequestInfo, _pad), 1);
        assert_eq!(std::mem::offset_of!(RequestInfo, handler_id), 8);
        assert_eq!(std::mem::offset_of!(RequestInfo, path_offset), 16);
        assert_eq!(std::mem::offset_of!(RequestInfo, path_len), 24);
        assert_eq!(std::mem::offset_of!(RequestInfo, headers_offset), 32);
        assert_eq!(std::mem::offset_of!(RequestInfo, body_offset), 40);
        assert_eq!(std::mem::offset_of!(RequestInfo, body_len), 48);
        assert_eq!(std::mem::offset_of!(RequestInfo, correlation_id), 56);
    }

    #[test]
    fn response_info_field_offsets() {
        assert_eq!(std::mem::offset_of!(ResponseInfo, status), 0);
        assert_eq!(std::mem::offset_of!(ResponseInfo, _pad), 2);
        assert_eq!(std::mem::offset_of!(ResponseInfo, correlation_id), 8);
        assert_eq!(std::mem::offset_of!(ResponseInfo, headers_offset), 16);
        assert_eq!(std::mem::offset_of!(ResponseInfo, headers_len), 24);
        assert_eq!(std::mem::offset_of!(ResponseInfo, body_offset), 32);
        assert_eq!(std::mem::offset_of!(ResponseInfo, body_len), 40);
        assert_eq!(std::mem::offset_of!(ResponseInfo, _reserved), 48);
    }

    #[test]
    fn http_method_from_bytes_known() {
        assert_eq!(HttpMethod::from_bytes(b"GET"), HttpMethod::GET);
        assert_eq!(HttpMethod::from_bytes(b"POST"), HttpMethod::POST);
        assert_eq!(HttpMethod::from_bytes(b"DELETE"), HttpMethod::DELETE);
    }

    #[test]
    fn http_method_from_bytes_unknown() {
        assert_eq!(HttpMethod::from_bytes(b"PROPFIND"), HttpMethod::OTHER);
    }

    #[test]
    fn response_info_status_only() {
        let r = ResponseInfo::status_only(204, 7);
        assert_eq!(r.status, 204);
        assert_eq!(r.correlation_id, 7);
        assert_eq!(r.body_len, 0);
        assert_eq!(r._reserved, [0u8; 16]);
    }

    #[test]
    fn request_info_is_repr_c() {
        // Verify no hidden padding by checking total size == sum of field sizes.
        // 1 (method) + 7 (pad) + 8*7 (u64 fields) = 64
        assert_eq!(std::mem::size_of::<RequestInfo>(), 1 + 7 + 8 * 7);
    }

    #[test]
    fn response_info_is_repr_c() {
        // 2 (status) + 6 (pad) + 8 (corr) + 8+8+8+8 (4 u64s) + 16 (reserved) = 64
        assert_eq!(std::mem::size_of::<ResponseInfo>(), 2 + 6 + 8 + 32 + 16);
    }
}
