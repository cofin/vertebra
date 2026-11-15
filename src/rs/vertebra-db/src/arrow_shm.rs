//! Zero-copy POSIX shared-memory (`VTBARROW`) framing and multi-batch stream writer.
//!
//! Frame layout in POSIX shared memory (`/dev/shm`):
//! - `[0..8]`: Magic bytes `b"VTBARROW"`
//! - `[8..16]`: `u64` little-endian payload byte length
//! - `[16..16 + byte_len]`: Arrow IPC stream bytes (`Schema` + `RecordBatch*` + `EOS`)

use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use vertebra_core::shm::{ShmRegion, os_page_size};

/// 8-byte magic header prefixing every `VTBARROW` shared-memory frame.
pub const SHM_ARROW_MAGIC: &[u8; 8] = b"VTBARROW";

/// 8-byte Arrow IPC stream End-of-Stream (EOS) marker (`0xFFFFFFFF` continuation + `0` length).
pub const ARROW_IPC_EOS_MARKER: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

/// Error message returned by shared-memory `VTBARROW` framing operations.
pub type ArrowShmError = String;

/// 16-byte header written at offset `0` of a shared-memory Arrow IPC `ShmRegion`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShmArrowFrameHeader {
    pub magic: [u8; 8],
    pub byte_len: u64,
}

impl ShmArrowFrameHeader {
    /// Total byte width of the `VTBARROW` frame header (`16` bytes).
    pub const SIZE: usize = 16;

    /// Encode header into 16 bytes.
    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..8].copy_from_slice(&self.magic);
        out[8..16].copy_from_slice(&self.byte_len.to_le_bytes());
        out
    }

    /// Decode header from the first 16 bytes of a slice.
    #[inline]
    pub fn from_slice(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < Self::SIZE {
            return Err("buffer too short for 16-byte ShmArrowFrameHeader".to_string());
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if &magic != SHM_ARROW_MAGIC {
            return Err("invalid ShmArrowFrameHeader magic (expected VTBARROW)".to_string());
        }
        let byte_len = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        Ok(Self { magic, byte_len })
    }
}

/// Process-local registry keeping active `VTBARROW` shared-memory regions alive.
pub static ACTIVE_ARROW_SHM_REGIONS: LazyLock<DashMap<String, Arc<ShmRegion>>> =
    LazyLock::new(DashMap::new);

/// Normalize a POSIX shared-memory segment name so it begins with `/`.
#[inline]
pub fn normalize_shm_name(shm_name: &str) -> String {
    let trimmed = shm_name.trim();
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

#[inline]
fn page_align(size: usize) -> Result<usize, String> {
    let page = os_page_size().map_err(|e| format!("failed to query OS page size: {e}"))?;
    let min_size = size.max(page);
    min_size
        .div_ceil(page)
        .checked_mul(page)
        .ok_or_else(|| "SHM Arrow page alignment overflow".to_string())
}

#[inline]
fn allocate_shm_region(norm_name: &str, alloc_size: usize) -> Result<ShmRegion, String> {
    let Some(rest) = norm_name.strip_prefix('/') else {
        return Err(format!("invalid Arrow ShmRegion name `{norm_name}`"));
    };
    if rest.is_empty() || rest.contains('/') || rest.contains('\0') || norm_name.len() > 255 {
        return Err(format!("invalid Arrow ShmRegion name `{norm_name}`"));
    }
    #[cfg(unix)]
    {
        ShmRegion::create_named(norm_name, alloc_size)
            .map_err(|e| format!("failed to create Arrow ShmRegion `{norm_name}`: {e}"))
    }
    #[cfg(not(unix))]
    {
        ShmRegion::create(alloc_size)
            .map_err(|e| format!("failed to create Arrow ShmRegion `{norm_name}`: {e}"))
    }
}

/// Write an Arrow IPC byte stream into a shared-memory `ShmRegion` without inserting it into
/// [`ACTIVE_ARROW_SHM_REGIONS`], returning `(normalized_shm_name, payload_len, Arc<ShmRegion>)`.
///
/// Used when ownership of the `Arc<ShmRegion>` is handed directly across extension boundaries
/// via [`vertebra_core::shm::ShmCapsule`].
pub fn write_shm_arrow_region_unregistered(
    shm_name: &str,
    ipc_bytes: &[u8],
) -> Result<(String, usize, Arc<ShmRegion>), String> {
    let name = normalize_shm_name(shm_name);
    let total_size = ShmArrowFrameHeader::SIZE
        .checked_add(ipc_bytes.len())
        .ok_or_else(|| "SHM Arrow frame size overflow".to_string())?;
    let alloc_size = page_align(total_size)?;
    let mut region = allocate_shm_region(&name, alloc_size)?;

    let header = ShmArrowFrameHeader {
        magic: *SHM_ARROW_MAGIC,
        byte_len: ipc_bytes.len() as u64,
    };
    let hdr_bytes = header.to_bytes();

    // SAFETY: We exclusively own `region` during initialization before wrapping in `Arc`.
    unsafe {
        let slice = region.as_mut_slice();
        slice[0..ShmArrowFrameHeader::SIZE].copy_from_slice(&hdr_bytes);
        slice[ShmArrowFrameHeader::SIZE..ShmArrowFrameHeader::SIZE + ipc_bytes.len()]
            .copy_from_slice(ipc_bytes);
    }

    Ok((name, ipc_bytes.len(), Arc::new(region)))
}

/// Write an Arrow IPC byte stream into a shared-memory `ShmRegion`, returning
/// `(normalized_shm_name, payload_len, Arc<ShmRegion>)`.
pub fn write_shm_arrow_region(
    shm_name: &str,
    ipc_bytes: &[u8],
) -> Result<(String, usize, Arc<ShmRegion>), String> {
    let (name, len, arc_region) = write_shm_arrow_region_unregistered(shm_name, ipc_bytes)?;
    ACTIVE_ARROW_SHM_REGIONS.insert(name.clone(), Arc::clone(&arc_region));
    Ok((name, len, arc_region))
}

/// Write an Arrow IPC byte stream into a shared-memory `ShmRegion`, returning `(normalized_shm_name, payload_len)`.
pub fn write_shm_arrow_frame(shm_name: &str, ipc_bytes: &[u8]) -> Result<(String, usize), String> {
    let (name, len, _) = write_shm_arrow_region(shm_name, ipc_bytes)?;
    Ok((name, len))
}

#[inline]
fn checked_header_frame_bounds(
    header: &ShmArrowFrameHeader,
) -> Result<(usize, usize), ArrowShmError> {
    let len = usize::try_from(header.byte_len)
        .map_err(|_| "corrupted ShmArrowFrameHeader byte_len overflows usize".to_string())?;
    let total_size = ShmArrowFrameHeader::SIZE
        .checked_add(len)
        .filter(|&total| total <= isize::MAX as usize)
        .ok_or_else(|| "corrupted ShmArrowFrameHeader frame size overflow".to_string())?;
    Ok((len, total_size))
}

/// Resolve or open a named `VTBARROW` SHM region and return `(Arc<ShmRegion>, payload_len)`.
pub fn open_shm_arrow_region(shm_name: &str) -> Result<(Arc<ShmRegion>, usize), ArrowShmError> {
    let name = normalize_shm_name(shm_name);
    if let Some(region_ref) = ACTIVE_ARROW_SHM_REGIONS.get(&name) {
        let region = Arc::clone(region_ref.value());
        // SAFETY: Region was initialized before publication and is immutable thereafter.
        let slice = unsafe { region.as_slice() };
        let header = ShmArrowFrameHeader::from_slice(slice)?;
        let (len, total_size) = checked_header_frame_bounds(&header)?;
        if slice.len() < total_size {
            return Err("corrupted ShmArrowFrameHeader byte_len exceeds region size".to_string());
        }
        return Ok((region, len));
    }

    #[cfg(unix)]
    {
        let probe_size =
            os_page_size().map_err(|e| format!("failed to query OS page size: {e}"))?;
        let probe = ShmRegion::open_named(&name, probe_size)
            .map_err(|e| format!("failed to open Arrow ShmRegion `{name}`: {e}"))?;
        // SAFETY: `probe` maps one OS page of readable bytes of a published `VTBARROW`
        // segment and `ShmArrowFrameHeader::from_slice` copies only the 16-byte header by value.
        let header = unsafe { ShmArrowFrameHeader::from_slice(probe.as_slice()) }?;
        let (len, total_size) = checked_header_frame_bounds(&header)?;
        if let Some(actual_size) = probe.backing_len()
            && total_size > actual_size
        {
            return Err(
                "corrupted ShmArrowFrameHeader byte_len exceeds backing segment size".to_string(),
            );
        }
        if total_size <= probe.len() {
            return Ok((Arc::new(probe), len));
        }
        drop(probe);
        let full = ShmRegion::open_named(&name, total_size)
            .map_err(|e| format!("failed to map full Arrow ShmRegion `{name}`: {e}"))?;
        Ok((Arc::new(full), len))
    }
    #[cfg(not(unix))]
    {
        Err(format!("Arrow ShmRegion `{name}` not found"))
    }
}

/// Borrow an Arrow IPC byte stream from a shared-memory `ShmRegion` without intermediate `Vec<u8>` allocation.
pub fn read_shm_arrow_frame_with<R, F>(shm_name: &str, f: F) -> Result<R, ArrowShmError>
where
    F: FnOnce(&[u8]) -> R,
{
    let (region, len) = open_shm_arrow_region(shm_name)?;
    let end = ShmArrowFrameHeader::SIZE
        .checked_add(len)
        .ok_or_else(|| "corrupted ShmArrowFrameHeader frame size overflow".to_string())?;
    // SAFETY: `region` holds an immutable `VTBARROW` mapping anchored by `Arc<ShmRegion>` for
    // the duration of this call.
    let slice = unsafe { region.as_slice() };
    let payload = slice
        .get(ShmArrowFrameHeader::SIZE..end)
        .ok_or_else(|| "corrupted ShmArrowFrameHeader byte_len exceeds region size".to_string())?;
    Ok(f(payload))
}

/// Read an Arrow IPC byte stream from a shared-memory `ShmRegion`.
pub fn read_shm_arrow_frame(shm_name: &str) -> Result<Vec<u8>, String> {
    read_shm_arrow_frame_with(shm_name, |slice| slice.to_vec())
}

/// Release an active shared-memory Arrow IPC `ShmRegion` from the process-local registry.
pub fn release_shm_arrow_frame(shm_name: &str) -> bool {
    let name = normalize_shm_name(shm_name);
    if let Some((_, region)) = ACTIVE_ARROW_SHM_REGIONS.remove(&name) {
        #[cfg(unix)]
        let _ = region.unlink_now();
        #[cfg(not(unix))]
        let _ = region;
        true
    } else {
        false
    }
}

/// Single-owner incremental stream writer that concatenates Arrow IPC frames into a
/// page-aligned `VTBARROW` shared-memory segment with 8-byte frame padding and EOS termination.
pub struct ShmArrowStreamWriter {
    shm_name: String,
    region: ShmRegion,
    capacity: usize,
    write_offset: usize,
    row_count: usize,
    batch_count: usize,
}

impl ShmArrowStreamWriter {
    /// Initialize a new `ShmArrowStreamWriter` at `shm_name` and write the initial `schema_ipc_frame`
    /// immediately after the 16-byte `ShmArrowFrameHeader` reservation.
    pub fn create(
        shm_name: &str,
        initial_capacity: usize,
        schema_ipc_frame: &[u8],
    ) -> Result<Self, String> {
        let norm_name = normalize_shm_name(shm_name);
        let stripped_schema = Self::strip_trailing_eos(schema_ipc_frame);
        let padded_schema_len = (stripped_schema.len() + 7) & !7;
        let min_required = ShmArrowFrameHeader::SIZE
            .checked_add(padded_schema_len)
            .and_then(|v| v.checked_add(ARROW_IPC_EOS_MARKER.len()))
            .ok_or_else(|| "SHM stream initial capacity overflow".to_string())?;
        let capacity = page_align(initial_capacity.max(min_required))?;
        let region = allocate_shm_region(&norm_name, capacity)?;

        let mut writer = Self {
            shm_name: norm_name,
            region,
            capacity,
            write_offset: ShmArrowFrameHeader::SIZE,
            row_count: 0,
            batch_count: 0,
        };
        if !stripped_schema.is_empty() {
            writer.write_padded_bytes(stripped_schema)?;
        }
        Ok(writer)
    }

    /// Total number of rows appended across all frames.
    #[inline]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Total number of record-batch frames appended.
    #[inline]
    pub fn batch_count(&self) -> usize {
        self.batch_count
    }

    /// Normalized POSIX SHM segment name (`/<name>`).
    #[inline]
    pub fn shm_name(&self) -> &str {
        &self.shm_name
    }

    #[inline]
    fn strip_trailing_eos(bytes: &[u8]) -> &[u8] {
        if bytes.len() >= ARROW_IPC_EOS_MARKER.len()
            && bytes[bytes.len() - ARROW_IPC_EOS_MARKER.len()..] == ARROW_IPC_EOS_MARKER
        {
            &bytes[..bytes.len() - ARROW_IPC_EOS_MARKER.len()]
        } else {
            bytes
        }
    }

    fn ensure_capacity(&mut self, additional_bytes: usize) -> Result<(), String> {
        let required = self
            .write_offset
            .checked_add(additional_bytes)
            .and_then(|v| v.checked_add(ARROW_IPC_EOS_MARKER.len()))
            .ok_or_else(|| "SHM Arrow stream capacity overflow".to_string())?;
        if required <= self.capacity {
            return Ok(());
        }
        let doubled = required
            .checked_mul(2)
            .ok_or_else(|| "SHM Arrow stream resize overflow".to_string())?;
        let next_capacity = page_align(doubled.max(self.capacity.saturating_mul(2)))?;

        #[cfg(unix)]
        let _ = self.region.unlink_now();

        let mut next_region = allocate_shm_region(&self.shm_name, next_capacity)?;
        // SAFETY: Both `self.region` and `next_region` are exclusively owned by `self` and have
        // at least `self.write_offset` valid initialized bytes.
        unsafe {
            let src = &self.region.as_slice()[..self.write_offset];
            let dst = &mut next_region.as_mut_slice()[..self.write_offset];
            dst.copy_from_slice(src);
        }
        self.region = next_region;
        self.capacity = next_capacity;
        Ok(())
    }

    fn write_padded_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.is_empty() {
            return Ok(());
        }
        let padded_len = bytes
            .len()
            .checked_add(7)
            .ok_or_else(|| "SHM Arrow frame size overflow".to_string())?
            & !7;
        let pad_bytes = padded_len - bytes.len();
        self.ensure_capacity(padded_len)?;
        // SAFETY: `ensure_capacity` guarantees `self.write_offset + padded_len <= self.capacity`.
        unsafe {
            let dst = self.region.as_mut_slice();
            dst[self.write_offset..self.write_offset + bytes.len()].copy_from_slice(bytes);
            if pad_bytes > 0 {
                dst[self.write_offset + bytes.len()..self.write_offset + padded_len].fill(0);
            }
        }
        self.write_offset += padded_len;
        Ok(())
    }

    /// Append a serialized Arrow IPC `RecordBatch` frame, padding to an 8-byte boundary with `0u8`.
    pub fn append_ipc_frame(
        &mut self,
        frame_bytes: &[u8],
        rows_in_frame: usize,
    ) -> Result<(), String> {
        if frame_bytes.is_empty() {
            return Ok(());
        }
        self.write_padded_bytes(frame_bytes)?;
        self.row_count = self.row_count.saturating_add(rows_in_frame);
        self.batch_count = self.batch_count.saturating_add(1);
        Ok(())
    }

    /// Append a raw Arrow IPC stream chunk (stripping any trailing 8-byte EOS marker so multiple
    /// chunks concatenate into a single Arrow IPC stream) and pad to an 8-byte boundary.
    pub fn append_raw_stream_chunk(
        &mut self,
        stream_bytes: &[u8],
        rows_in_chunk: usize,
    ) -> Result<(), String> {
        let stripped = Self::strip_trailing_eos(stream_bytes);
        if stripped.is_empty() {
            return Ok(());
        }
        self.write_padded_bytes(stripped)?;
        self.row_count = self.row_count.saturating_add(rows_in_chunk);
        self.batch_count = self.batch_count.saturating_add(1);
        Ok(())
    }

    /// Finalize the stream by appending the 8-byte Arrow IPC EOS marker and stamping the 16-byte
    /// `ShmArrowFrameHeader` at offset `0..16`, returning `Arc<ShmRegion>` without inserting it
    /// into [`ACTIVE_ARROW_SHM_REGIONS`].
    pub fn finish_unregistered(mut self) -> Result<(String, usize, Arc<ShmRegion>), String> {
        self.ensure_capacity(0)?;
        // SAFETY: `ensure_capacity(0)` reserves space for `ARROW_IPC_EOS_MARKER` (`8` bytes).
        unsafe {
            let dst = self.region.as_mut_slice();
            dst[self.write_offset..self.write_offset + ARROW_IPC_EOS_MARKER.len()]
                .copy_from_slice(&ARROW_IPC_EOS_MARKER);
        }
        self.write_offset += ARROW_IPC_EOS_MARKER.len();
        let byte_len = self.write_offset - ShmArrowFrameHeader::SIZE;
        let header = ShmArrowFrameHeader {
            magic: *SHM_ARROW_MAGIC,
            byte_len: byte_len as u64,
        };
        let hdr_bytes = header.to_bytes();
        // SAFETY: `self.region` has at least `ShmArrowFrameHeader::SIZE` bytes and is exclusively owned.
        unsafe {
            let dst = self.region.as_mut_slice();
            dst[0..ShmArrowFrameHeader::SIZE].copy_from_slice(&hdr_bytes);
        }
        Ok((self.shm_name, byte_len, Arc::new(self.region)))
    }

    /// Finalize the stream by appending the 8-byte Arrow IPC EOS marker, stamping the 16-byte
    /// `ShmArrowFrameHeader` at offset `0..16`, and publishing `Arc<ShmRegion>` in
    /// [`ACTIVE_ARROW_SHM_REGIONS`].
    pub fn finish(self) -> Result<(String, usize, Arc<ShmRegion>), String> {
        let (shm_name, byte_len, arc_region) = self.finish_unregistered()?;
        ACTIVE_ARROW_SHM_REGIONS.insert(shm_name.clone(), Arc::clone(&arc_region));
        Ok((shm_name, byte_len, arc_region))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn invalid_named_allocation_fails_without_registry_entry() {
        for name in ["/bad/nested/seg", "/"] {
            let result = write_shm_arrow_frame(name, b"payload");
            if result.is_ok() {
                release_shm_arrow_frame(name);
            }
            let error = result.expect_err("invalid names must not allocate anonymous SHM");
            assert!(error.contains(name), "missing segment name: {error}");
            assert!(!ACTIVE_ARROW_SHM_REGIONS.contains_key(name));
        }
    }

    #[test]
    fn u64_max_byte_len_header_returns_error_without_overflow() {
        let page = os_page_size().unwrap();
        let name = format!("/vtb-arrow-overflow-{}", std::process::id());
        let mut region = ShmRegion::create(page).unwrap();
        let corrupt = ShmArrowFrameHeader {
            magic: *SHM_ARROW_MAGIC,
            byte_len: u64::MAX,
        };
        // SAFETY: `region` is exclusively owned in this unit test before wrapping in `Arc`.
        unsafe {
            region.as_mut_slice()[0..ShmArrowFrameHeader::SIZE]
                .copy_from_slice(&corrupt.to_bytes());
        }
        ACTIVE_ARROW_SHM_REGIONS.insert(name.clone(), Arc::new(region));

        assert!(open_shm_arrow_region(&name).is_err());
        assert!(read_shm_arrow_frame_with(&name, |s| s.len()).is_err());
        assert!(release_shm_arrow_frame(&name));

        #[cfg(unix)]
        if let Ok(mut named) = ShmRegion::create_named(&name, page) {
            // SAFETY: `named` is exclusively owned in this unit test.
            unsafe {
                named.as_mut_slice()[0..ShmArrowFrameHeader::SIZE]
                    .copy_from_slice(&corrupt.to_bytes());
            }
            assert!(open_shm_arrow_region(&name).is_err());
            assert!(read_shm_arrow_frame_with(&name, |s| s.len()).is_err());
            drop(named);
        }
    }

    #[test]
    fn shm_arrow_stream_writer_resize_and_finish_unregistered() {
        let shm_name = format!("test-vtb-arrow-unit-resize-{}", std::process::id());
        let _ = release_shm_arrow_frame(&shm_name);

        let schema = vec![0x33u8; 45];
        let mut writer =
            ShmArrowStreamWriter::create(&shm_name, 4096, &schema).expect("create writer");
        let batch = vec![0x44u8; 5000];
        writer.append_ipc_frame(&batch, 10).expect("batch 1");
        writer.append_ipc_frame(&batch, 10).expect("batch 2 resize");

        let (norm, byte_len, region) = writer.finish_unregistered().expect("finish unregistered");
        assert!(!ACTIVE_ARROW_SHM_REGIONS.contains_key(&norm));
        let slice = unsafe { region.as_slice() };
        let hdr = ShmArrowFrameHeader::from_slice(&slice[0..16]).expect("header");
        assert_eq!(hdr.byte_len as usize, byte_len);
        assert_eq!(
            &slice[16 + byte_len - ARROW_IPC_EOS_MARKER.len()..16 + byte_len],
            &ARROW_IPC_EOS_MARKER
        );
        drop(region);

        let mut writer2 =
            ShmArrowStreamWriter::create(&shm_name, 4096, &schema).expect("recreate after drop");
        writer2.append_ipc_frame(&batch, 5).expect("batch");
        let (norm2, _, _region2) = writer2.finish().expect("finish");
        assert!(release_shm_arrow_frame(&norm2));
    }
}
