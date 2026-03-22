//! Compression codec for IPC batch payloads.
//!
//! Encodes the codec selection into bits 6-7 of the protocol flags field:
//! - `00` = None (no compression)
//! - `01` = LZ4 (fast, moderate ratio)
//! - `10` = Zstd (slower, better ratio)
//!
//! The COMPRESSED flag (bit 1) signals that a payload is compressed.
//! These bits specify *which* codec was used.

use super::flags;

/// Bits 6-7 mask for codec selection in the flags field.
const CODEC_MASK: u32 = 0b11 << 6;
const CODEC_SHIFT: u32 = 6;

/// Compression codec for batch slab data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    /// No compression.
    None,
    /// LZ4 — fast compression/decompression, moderate ratio.
    Lz4,
    /// Zstd — slower but better compression ratio.
    Zstd,
}

impl CompressionCodec {
    /// Extract the codec from a flags word.
    ///
    /// Returns `CompressionCodec::None` if the COMPRESSED flag is not set,
    /// regardless of what bits 6-7 contain.
    #[inline]
    pub fn from_flags(flags: u32) -> Self {
        if flags & flags::COMPRESSED == 0 {
            return Self::None;
        }
        match (flags & CODEC_MASK) >> CODEC_SHIFT {
            1 => Self::Lz4,
            2 => Self::Zstd,
            // COMPRESSED set but codec bits are 00 or 11 → treat as LZ4 (safe default)
            _ => Self::Lz4,
        }
    }

    /// Encode this codec into a flags word.
    ///
    /// Sets the COMPRESSED flag and codec bits. For `None`, returns 0
    /// (no COMPRESSED flag, no codec bits).
    #[inline]
    pub fn to_flags(self) -> u32 {
        match self {
            Self::None => 0,
            Self::Lz4 => flags::COMPRESSED | (1 << CODEC_SHIFT),
            Self::Zstd => flags::COMPRESSED | (2 << CODEC_SHIFT),
        }
    }

    /// Merge codec bits into an existing flags word.
    ///
    /// Clears existing COMPRESSED + codec bits, then ORs in the new ones.
    #[inline]
    pub fn apply_to_flags(self, existing: u32) -> u32 {
        let cleared = existing & !(flags::COMPRESSED | CODEC_MASK);
        cleared | self.to_flags()
    }
}

/// Compress a byte slice using the specified codec.
///
/// Returns the original data unmodified for `CompressionCodec::None`.
#[cfg(feature = "compression")]
pub fn compress(data: &[u8], codec: CompressionCodec) -> Vec<u8> {
    match codec {
        CompressionCodec::None => data.to_vec(),
        CompressionCodec::Lz4 => lz4_flex::compress_prepend_size(data),
        CompressionCodec::Zstd => {
            zstd::bulk::compress(data, 3).expect("zstd compression should not fail on valid input")
        }
    }
}

/// Decompress a byte slice using the specified codec.
///
/// Returns the original data unmodified for `CompressionCodec::None`.
/// Returns an error if the compressed data is malformed.
#[cfg(feature = "compression")]
pub fn decompress(data: &[u8], codec: CompressionCodec) -> Result<Vec<u8>, CodecError> {
    match codec {
        CompressionCodec::None => Ok(data.to_vec()),
        CompressionCodec::Lz4 => {
            lz4_flex::decompress_size_prepended(data).map_err(|e| CodecError::Lz4(e.to_string()))
        }
        CompressionCodec::Zstd => {
            zstd::bulk::decompress(data, MAX_DECOMPRESS_SIZE)
                .map_err(|e| CodecError::Zstd(e.to_string()))
        }
    }
}

/// Maximum decompression output size (64 MB) — prevents decompression bombs.
#[cfg(feature = "compression")]
const MAX_DECOMPRESS_SIZE: usize = 64 * 1024 * 1024;

/// Errors from codec operations.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("lz4 decompression failed: {0}")]
    Lz4(String),
    #[error("zstd decompression failed: {0}")]
    Zstd(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Flag encoding/decoding ---

    #[test]
    fn codec_none_to_flags_is_zero() {
        assert_eq!(CompressionCodec::None.to_flags(), 0);
    }

    #[test]
    fn codec_lz4_flags_roundtrip() {
        let f = CompressionCodec::Lz4.to_flags();
        assert!(f & flags::COMPRESSED != 0, "COMPRESSED bit must be set");
        assert_eq!(CompressionCodec::from_flags(f), CompressionCodec::Lz4);
    }

    #[test]
    fn codec_zstd_flags_roundtrip() {
        let f = CompressionCodec::Zstd.to_flags();
        assert!(f & flags::COMPRESSED != 0);
        assert_eq!(CompressionCodec::from_flags(f), CompressionCodec::Zstd);
    }

    #[test]
    fn codec_none_from_flags_without_compressed_bit() {
        // Even if codec bits are set, no COMPRESSED flag → None
        let f = 1 << 6; // codec bits = 01, but COMPRESSED not set
        assert_eq!(CompressionCodec::from_flags(f), CompressionCodec::None);
    }

    #[test]
    fn codec_defaults_to_lz4_for_unknown_bits() {
        // COMPRESSED set, codec bits = 00 → default to Lz4
        let f = flags::COMPRESSED;
        assert_eq!(CompressionCodec::from_flags(f), CompressionCodec::Lz4);
        // COMPRESSED set, codec bits = 11 → default to Lz4
        let f = flags::COMPRESSED | (3 << 6);
        assert_eq!(CompressionCodec::from_flags(f), CompressionCodec::Lz4);
    }

    #[test]
    fn apply_to_flags_preserves_other_bits() {
        let existing = flags::PRIORITY | flags::BATCH;
        let result = CompressionCodec::Zstd.apply_to_flags(existing);
        assert!(result & flags::PRIORITY != 0, "PRIORITY should be preserved");
        assert!(result & flags::BATCH != 0, "BATCH should be preserved");
        assert!(result & flags::COMPRESSED != 0, "COMPRESSED should be set");
        assert_eq!(CompressionCodec::from_flags(result), CompressionCodec::Zstd);
    }

    #[test]
    fn apply_none_clears_compression_bits() {
        let existing = flags::PRIORITY | CompressionCodec::Lz4.to_flags();
        let result = CompressionCodec::None.apply_to_flags(existing);
        assert!(result & flags::COMPRESSED == 0, "COMPRESSED should be cleared");
        assert!(result & flags::PRIORITY != 0, "PRIORITY should be preserved");
    }

    // --- Compress/decompress round-trips ---

    #[cfg(feature = "compression")]
    mod compression_tests {
        use super::*;

        #[test]
        fn compress_none_is_identity() {
            let data = b"hello world";
            let out = compress(data, CompressionCodec::None);
            assert_eq!(out, data);
        }

        #[test]
        fn lz4_roundtrip() {
            let data = b"the quick brown fox jumps over the lazy dog";
            let compressed = compress(data, CompressionCodec::Lz4);
            let decompressed = decompress(&compressed, CompressionCodec::Lz4).unwrap();
            assert_eq!(decompressed, data);
        }

        #[test]
        fn zstd_roundtrip() {
            let data = b"the quick brown fox jumps over the lazy dog";
            let compressed = compress(data, CompressionCodec::Zstd);
            let decompressed = decompress(&compressed, CompressionCodec::Zstd).unwrap();
            assert_eq!(decompressed, data);
        }

        #[test]
        fn roundtrip_empty_payload() {
            for codec in [CompressionCodec::None, CompressionCodec::Lz4, CompressionCodec::Zstd] {
                let compressed = compress(b"", codec);
                let decompressed = decompress(&compressed, codec).unwrap();
                assert_eq!(decompressed, b"", "empty roundtrip failed for {codec:?}");
            }
        }

        #[test]
        fn roundtrip_large_payload() {
            let data: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
            for codec in [CompressionCodec::Lz4, CompressionCodec::Zstd] {
                let compressed = compress(&data, codec);
                assert!(
                    compressed.len() < data.len(),
                    "compression should reduce size for repetitive data ({codec:?})"
                );
                let decompressed = decompress(&compressed, codec).unwrap();
                assert_eq!(decompressed, data, "roundtrip failed for {codec:?}");
            }
        }

        #[test]
        fn decompress_garbage_returns_error() {
            let garbage = b"not valid compressed data";
            assert!(decompress(garbage, CompressionCodec::Lz4).is_err());
            assert!(decompress(garbage, CompressionCodec::Zstd).is_err());
        }

        #[test]
        fn decompress_none_is_identity() {
            let data = b"pass through";
            let out = decompress(data, CompressionCodec::None).unwrap();
            assert_eq!(out, data);
        }
    }
}
