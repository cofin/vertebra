//! Versioned DBRPC binary frame header and operation codec.

/// Active DBRPC wire protocol version.
pub const DBRPC_VERSION: u16 = 1;

/// Fixed byte width of an encoded [`DbRpcHeader`] in little-endian layout.
pub const DBRPC_HEADER_SIZE: usize = 23;

/// Operation code carried by a DBRPC frame header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DbRpcOp {
    Execute = 1,
    ExecuteMany = 2,
    Begin = 3,
    Commit = 4,
    Rollback = 5,
    Savepoint = 6,
    FetchChunk = 7,
    Cancel = 8,
    Close = 9,
}

impl DbRpcOp {
    /// Decode a wire discriminant byte into [`DbRpcOp`].
    pub const fn from_u8(raw: u8) -> Result<Self, DbRpcError> {
        match raw {
            1 => Ok(Self::Execute),
            2 => Ok(Self::ExecuteMany),
            3 => Ok(Self::Begin),
            4 => Ok(Self::Commit),
            5 => Ok(Self::Rollback),
            6 => Ok(Self::Savepoint),
            7 => Ok(Self::FetchChunk),
            8 => Ok(Self::Cancel),
            9 => Ok(Self::Close),
            other => Err(DbRpcError::InvalidOp(other)),
        }
    }
}

/// Errors returned when encoding or decoding DBRPC frame headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DbRpcError {
    #[error("truncated dbrpc header: expected at least {expected} bytes, got {actual}")]
    Truncated { expected: usize, actual: usize },
    #[error("invalid dbrpc op discriminant: {0}")]
    InvalidOp(u8),
    #[error("unsupported dbrpc version: {0}")]
    UnsupportedVersion(u16),
}

/// Fixed 23-byte little-endian header prefixing every DBRPC payload frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbRpcHeader {
    pub version: u16,
    pub op: DbRpcOp,
    pub request_id: u64,
    pub lease_slot: u32,
    pub lease_generation: u64,
}

impl DbRpcHeader {
    /// Construct a new [`DbRpcHeader`] pinned to [`DBRPC_VERSION`].
    pub const fn new(op: DbRpcOp, request_id: u64, lease_slot: u32, lease_generation: u64) -> Self {
        Self {
            version: DBRPC_VERSION,
            op,
            request_id,
            lease_slot,
            lease_generation,
        }
    }

    /// Serialize this header into a fixed 23-byte little-endian array.
    pub fn encode(&self) -> [u8; DBRPC_HEADER_SIZE] {
        let mut buf = [0u8; DBRPC_HEADER_SIZE];
        buf[0..2].copy_from_slice(&self.version.to_le_bytes());
        buf[2] = self.op as u8;
        buf[3..11].copy_from_slice(&self.request_id.to_le_bytes());
        buf[11..15].copy_from_slice(&self.lease_slot.to_le_bytes());
        buf[15..23].copy_from_slice(&self.lease_generation.to_le_bytes());
        buf
    }

    /// Encode this header into `dst`, failing if fewer than [`DBRPC_HEADER_SIZE`] bytes exist.
    pub fn encode_into(&self, dst: &mut [u8]) -> Result<usize, DbRpcError> {
        if dst.len() < DBRPC_HEADER_SIZE {
            return Err(DbRpcError::Truncated {
                expected: DBRPC_HEADER_SIZE,
                actual: dst.len(),
            });
        }
        dst[..DBRPC_HEADER_SIZE].copy_from_slice(&self.encode());
        Ok(DBRPC_HEADER_SIZE)
    }

    /// Decode and validate a [`DbRpcHeader`] from a byte slice prefix.
    pub fn decode(src: &[u8]) -> Result<Self, DbRpcError> {
        if src.len() < DBRPC_HEADER_SIZE {
            return Err(DbRpcError::Truncated {
                expected: DBRPC_HEADER_SIZE,
                actual: src.len(),
            });
        }
        let version = u16::from_le_bytes([src[0], src[1]]);
        if version != DBRPC_VERSION {
            return Err(DbRpcError::UnsupportedVersion(version));
        }
        let op = DbRpcOp::from_u8(src[2])?;
        let request_id = u64::from_le_bytes(src[3..11].try_into().expect("8-byte slice"));
        let lease_slot = u32::from_le_bytes(src[11..15].try_into().expect("4-byte slice"));
        let lease_generation = u64::from_le_bytes(src[15..23].try_into().expect("8-byte slice"));
        Ok(Self {
            version,
            op,
            request_id,
            lease_slot,
            lease_generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_every_op() {
        let ops = [
            DbRpcOp::Execute,
            DbRpcOp::ExecuteMany,
            DbRpcOp::Begin,
            DbRpcOp::Commit,
            DbRpcOp::Rollback,
            DbRpcOp::Savepoint,
            DbRpcOp::FetchChunk,
            DbRpcOp::Cancel,
            DbRpcOp::Close,
        ];
        for (idx, op) in ops.into_iter().enumerate() {
            let hdr = DbRpcHeader::new(op, 1000 + idx as u64, idx as u32, 99 + idx as u64);
            let encoded = hdr.encode();
            let decoded = DbRpcHeader::decode(&encoded).expect("decode valid header");
            assert_eq!(decoded, hdr);
        }
    }

    #[test]
    fn decode_rejects_truncated_version_and_op() {
        assert_eq!(
            DbRpcHeader::decode(&[1, 0, 1]),
            Err(DbRpcError::Truncated {
                expected: DBRPC_HEADER_SIZE,
                actual: 3,
            })
        );

        let mut bad_ver = DbRpcHeader::new(DbRpcOp::Execute, 1, 0, 1).encode();
        bad_ver[0..2].copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            DbRpcHeader::decode(&bad_ver),
            Err(DbRpcError::UnsupportedVersion(2))
        );

        let mut bad_op = DbRpcHeader::new(DbRpcOp::Execute, 1, 0, 1).encode();
        bad_op[2] = 0xFE;
        assert_eq!(
            DbRpcHeader::decode(&bad_op),
            Err(DbRpcError::InvalidOp(0xFE))
        );
    }
}
