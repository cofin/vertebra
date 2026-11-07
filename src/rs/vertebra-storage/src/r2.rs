//! Cloudflare R2 and OCI uniform multipart upload chunking utilities.

use bytes::{Bytes, BytesMut};

/// Uniform 8 MiB multipart chunk size required by Cloudflare R2 and OCI multipart uploads.
pub const R2_UNIFORM_PART_SIZE: usize = 8 * 1024 * 1024;

/// Return `true` when the storage backend requires uniform non-trailing multipart part sizes.
pub fn requires_uniform_part_size(scheme: &str) -> bool {
    matches!(scheme, "r2" | "oci")
}

/// Split a payload into uniform `part_size` chunks (all non-trailing chunks have identical byte length).
pub fn chunk_uniform_parts(data: Bytes, part_size: usize) -> Vec<Bytes> {
    let chunk_len = part_size.max(1);
    if data.is_empty() {
        return vec![Bytes::new()];
    }
    let mut parts = Vec::with_capacity(data.len().div_ceil(chunk_len));
    let mut offset = 0;
    while offset < data.len() {
        let end = (offset + chunk_len).min(data.len());
        parts.push(data.slice(offset..end));
        offset = end;
    }
    parts
}

/// Rolling fixed-capacity re-chunker that emits exact `part_size` `Bytes` frames incrementally
/// without concatenating an entire multi-part stream in memory.
#[derive(Debug)]
pub struct UniformPartAccumulator {
    part_size: usize,
    buf: BytesMut,
}

impl UniformPartAccumulator {
    pub fn new(part_size: usize) -> Self {
        Self {
            part_size: part_size.max(1),
            buf: BytesMut::new(),
        }
    }

    pub fn push(&mut self, mut chunk: Bytes) -> Vec<Bytes> {
        if chunk.is_empty() {
            return Vec::new();
        }
        let mut ready = Vec::new();
        if !self.buf.is_empty() {
            if self.buf.len() + chunk.len() < self.part_size {
                self.buf.extend_from_slice(&chunk);
                return ready;
            }
            let needed = self.part_size - self.buf.len();
            self.buf.extend_from_slice(&chunk.split_to(needed));
            ready.push(self.buf.split().freeze());
        }
        while chunk.len() >= self.part_size {
            ready.push(chunk.split_to(self.part_size));
        }
        if !chunk.is_empty() {
            self.buf.extend_from_slice(&chunk);
        }
        ready
    }

    pub fn finish(&mut self) -> Option<Bytes> {
        if self.buf.is_empty() {
            None
        } else {
            Some(self.buf.split().freeze())
        }
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }
}
