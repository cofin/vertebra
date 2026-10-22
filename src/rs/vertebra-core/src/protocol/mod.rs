//! Protocol definitions for IPC control plane.

pub mod codec;

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
    /// Multi-frame streaming response active.
    pub const STREAMING: u32 = 1 << 6;
    /// Terminal frame of a stream or unary response.
    pub const FINAL: u32 = 1 << 7;
    /// Initial response headers carried in this frame.
    pub const HEADERS_SENT: u32 = 1 << 8;
    /// Frame carries HTTP/2 or gRPC trailing headers.
    pub const TRAILERS: u32 = 1 << 9;
}

/// Response flags for IPC messages.
pub mod response_flags {
    pub use super::flags::{COMPRESSED, ERROR, FINAL, HEADERS_SENT, NONE, OK, STREAMING, TRAILERS};
    pub const STREAM_START: u32 = STREAMING | HEADERS_SENT;
    pub const STREAM_CHUNK: u32 = STREAMING;
    pub const STREAM_END: u32 = STREAMING | FINAL;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flags() {
        let f = flags::PRIORITY | flags::BATCH;
        assert!(f & flags::PRIORITY != 0);
        assert!(f & flags::BATCH != 0);
        assert!(f & flags::COMPRESSED == 0);
    }
}
