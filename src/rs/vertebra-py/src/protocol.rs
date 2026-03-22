//! Python-exposed protocol types.

use pyo3::prelude::*;

use vertebra_core::protocol::codec::CompressionCodec;

/// Compression codec selection for batch transport.
///
/// Available codecs:
/// - `CompressionCodec.NONE` — no compression (default)
/// - `CompressionCodec.LZ4` — fast compression/decompression
/// - `CompressionCodec.ZSTD` — better ratio, slower
#[pyclass(name = "CompressionCodec", frozen, from_py_object)]
#[derive(Clone, Copy, Debug)]
pub struct PyCompressionCodec {
    inner: CompressionCodec,
}

#[pymethods]
impl PyCompressionCodec {
    /// No compression.
    #[classattr]
    const NONE: Self = Self {
        inner: CompressionCodec::None,
    };

    /// LZ4 fast compression.
    #[classattr]
    const LZ4: Self = Self {
        inner: CompressionCodec::Lz4,
    };

    /// Zstd compression (better ratio).
    #[classattr]
    const ZSTD: Self = Self {
        inner: CompressionCodec::Zstd,
    };

    fn __repr__(&self) -> &'static str {
        match self.inner {
            CompressionCodec::None => "CompressionCodec.NONE",
            CompressionCodec::Lz4 => "CompressionCodec.LZ4",
            CompressionCodec::Zstd => "CompressionCodec.ZSTD",
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        match self.inner {
            CompressionCodec::None => 0,
            CompressionCodec::Lz4 => 1,
            CompressionCodec::Zstd => 2,
        }
    }
}

impl PyCompressionCodec {
    /// Convert to the Rust codec enum.
    pub fn to_codec(self) -> CompressionCodec {
        self.inner
    }
}
