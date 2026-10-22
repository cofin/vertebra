//! Cloudflare R2 multipart upload chunking utilities.

use bytes::Bytes;

/// Uniform 8 MiB multipart chunk size required by Cloudflare R2 and OCI multipart uploads.
pub const R2_UNIFORM_PART_SIZE: usize = 8 * 1024 * 1024;

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
