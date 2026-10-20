use serde::{Deserialize, Serialize};

pub const FLAG_STREAMING: u16 = 1 << 0;
pub const FLAG_FINAL: u16 = 1 << 1;
pub const FLAG_ERROR: u16 = 1 << 2;
pub const FLAG_COMPRESSED: u16 = 1 << 3;
pub const FLAG_HEADERS_SENT: u16 = 1 << 4;
pub const FLAG_TRAILERS: u16 = 1 << 5;

/// Fixed 16-byte wire header for `ResponseSlot`:
/// `[request_id: u64 LE][status: u16 LE][flags: u16 LE][header_count: u16 LE][reserved: u16 LE]`
pub const RESPONSE_SLOT_HEADER_LEN: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("destination buffer too small: needed {needed} bytes, got {available}")]
    BufferTooSmall { needed: usize, available: usize },
    #[error("malformed IPC frame: {0}")]
    MalformedFrame(&'static str),
}

/// Framed SHM response slot supporting unary and multi-chunk streaming payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseSlot {
    pub request_id: u64,
    pub status: u16,
    pub flags: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl ResponseSlot {
    /// Compute the exact serialized byte length for `headers` and `body_slice`.
    #[inline]
    pub fn encoded_size(headers: &[(String, String)], body_slice: &[u8]) -> usize {
        let headers_bytes: usize = headers.iter().map(|(k, v)| 4 + k.len() + v.len()).sum();
        RESPONSE_SLOT_HEADER_LEN + 4 + headers_bytes + body_slice.len()
    }

    /// Encode this `ResponseSlot` into `dst`, returning the number of bytes written.
    pub fn encode_into(&self, dst: &mut [u8]) -> Result<usize, IpcError> {
        Self::encode_raw_parts_into(
            self.request_id,
            self.status,
            self.flags,
            &self.headers,
            &self.body,
            dst,
        )
    }

    /// Encode response metadata, borrowed header tuples, and a borrowed `body_slice` directly
    /// into `dst` without allocating an intermediate `ResponseSlot` or `Vec<u8>`.
    pub fn encode_raw_parts_into(
        request_id: u64,
        status: u16,
        flags: u16,
        headers: &[(String, String)],
        body_slice: &[u8],
        dst: &mut [u8],
    ) -> Result<usize, IpcError> {
        let needed = Self::encoded_size(headers, body_slice);
        if dst.len() < needed {
            return Err(IpcError::BufferTooSmall {
                needed,
                available: dst.len(),
            });
        }

        dst[0..8].copy_from_slice(&request_id.to_le_bytes());
        dst[8..10].copy_from_slice(&status.to_le_bytes());
        dst[10..12].copy_from_slice(&flags.to_le_bytes());
        dst[12..14].copy_from_slice(&(headers.len() as u16).to_le_bytes());
        dst[14..16].copy_from_slice(&0u16.to_le_bytes());

        let mut offset = RESPONSE_SLOT_HEADER_LEN;
        for (k, v) in headers {
            let kb = k.as_bytes();
            let vb = v.as_bytes();
            dst[offset..offset + 2].copy_from_slice(&(kb.len() as u16).to_le_bytes());
            offset += 2;
            dst[offset..offset + kb.len()].copy_from_slice(kb);
            offset += kb.len();
            dst[offset..offset + 2].copy_from_slice(&(vb.len() as u16).to_le_bytes());
            offset += 2;
            dst[offset..offset + vb.len()].copy_from_slice(vb);
            offset += vb.len();
        }

        dst[offset..offset + 4].copy_from_slice(&(body_slice.len() as u32).to_le_bytes());
        offset += 4;
        dst[offset..offset + body_slice.len()].copy_from_slice(body_slice);
        offset += body_slice.len();

        Ok(offset)
    }

    /// Peek `request_id` from the 16-byte slot prefix in `O(1)` without deserializing headers or body.
    #[inline]
    pub fn peek_request_id(src: &[u8]) -> Option<u64> {
        if src.len() < 8 {
            return None;
        }
        Some(u64::from_le_bytes(src[0..8].try_into().ok()?))
    }

    /// Peek `flags` from the 16-byte slot prefix in `O(1)` without deserializing headers or body.
    #[inline]
    pub fn peek_flags(src: &[u8]) -> Option<u16> {
        if src.len() < 12 {
            return None;
        }
        Some(u16::from_le_bytes(src[10..12].try_into().ok()?))
    }

    /// Peek `status` from the 16-byte slot prefix in `O(1)`.
    #[inline]
    pub fn peek_status(src: &[u8]) -> Option<u16> {
        if src.len() < 10 {
            return None;
        }
        Some(u16::from_le_bytes(src[8..10].try_into().ok()?))
    }

    /// Decode a `ResponseSlot` from a byte slice.
    pub fn decode_from(src: &[u8]) -> Result<Self, IpcError> {
        if src.len() < RESPONSE_SLOT_HEADER_LEN + 4 {
            return Err(IpcError::MalformedFrame("frame shorter than header"));
        }
        let request_id = u64::from_le_bytes(src[0..8].try_into().unwrap());
        let status = u16::from_le_bytes(src[8..10].try_into().unwrap());
        let flags = u16::from_le_bytes(src[10..12].try_into().unwrap());
        let header_count = u16::from_le_bytes(src[12..14].try_into().unwrap()) as usize;

        let mut offset = RESPONSE_SLOT_HEADER_LEN;
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            if offset + 2 > src.len() {
                return Err(IpcError::MalformedFrame("truncated header key length"));
            }
            let klen = u16::from_le_bytes(src[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if offset + klen + 2 > src.len() {
                return Err(IpcError::MalformedFrame("truncated header key"));
            }
            let key = String::from_utf8_lossy(&src[offset..offset + klen]).into_owned();
            offset += klen;
            let vlen = u16::from_le_bytes(src[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if offset + vlen > src.len() {
                return Err(IpcError::MalformedFrame("truncated header value"));
            }
            let val = String::from_utf8_lossy(&src[offset..offset + vlen]).into_owned();
            offset += vlen;
            headers.push((key, val));
        }

        if offset + 4 > src.len() {
            return Err(IpcError::MalformedFrame("truncated body length"));
        }
        let body_len = u32::from_le_bytes(src[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + body_len > src.len() {
            return Err(IpcError::MalformedFrame("truncated body payload"));
        }
        let body = src[offset..offset + body_len].to_vec();

        Ok(Self {
            request_id,
            status,
            flags,
            headers,
            body,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum Opcode {
    /// Standard ASGI HTTP request
    AsgiHttp = 1,
    /// Standard ASGI WebSocket connection
    AsgiWebsocket = 2,
    /// Direct optimized HTTP request (bypasses ASGI)
    DirectHttp = 3,
    /// Direct optimized WebSocket open
    WsOpen = 4,
    /// Direct optimized WebSocket message
    WsMessage = 5,
    /// Direct optimized WebSocket close
    WsClose = 6,
    /// Server-Sent Events data
    SseData = 7,
    /// Hash a password (Bun Native)
    HashPassword = 8,
    /// Verify a password (Bun Native)
    VerifyPassword = 9,
    /// Generate random bytes (Bun Native)
    GenerateRandom = 10,
    /// Sign a CSRF token (Bun Native)
    CsrfSign = 11,
}

impl From<u32> for Opcode {
    fn from(value: u32) -> Self {
        match value {
            1 => Opcode::AsgiHttp,
            2 => Opcode::AsgiWebsocket,
            3 => Opcode::DirectHttp,
            4 => Opcode::WsOpen,
            5 => Opcode::WsMessage,
            6 => Opcode::WsClose,
            7 => Opcode::SseData,
            8 => Opcode::HashPassword,
            9 => Opcode::VerifyPassword,
            10 => Opcode::GenerateRandom,
            11 => Opcode::CsrfSign,
            _ => Opcode::AsgiHttp, // Default/fallback
        }
    }
}
