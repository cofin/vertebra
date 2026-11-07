//! Shared-memory `VTBARROW` Arrow IPC segment naming and framing helpers for `vertebra-spanner`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::{ShmArrowStreamWriter, write_shm_arrow_region_unregistered};

use crate::client::SpannerError;

/// Default initial byte capacity (`64 KiB`) when opening a Spanner `VTBARROW` stream writer.
pub const DEFAULT_SPANNER_ARROW_SHM_CAPACITY: usize = 64 * 1024;

static SHM_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Allocate the next unique process-scoped POSIX shared-memory name (`/vtb-sp-<pid>-<seq>`)
/// for a Spanner `VTBARROW` result cursor.
#[inline]
pub fn next_spanner_shm_name() -> String {
    let seq = SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/vtb-sp-{pid:x}-{seq:x}")
}

/// Open a [`ShmArrowStreamWriter`] backed by a fresh `/vtb-sp-*` POSIX shared-memory segment
/// initialized with `schema_ipc_frame`.
pub fn open_spanner_shm_writer(
    schema_ipc_frame: &[u8],
) -> Result<ShmArrowStreamWriter, SpannerError> {
    let shm_name = next_spanner_shm_name();
    ShmArrowStreamWriter::create(
        &shm_name,
        DEFAULT_SPANNER_ARROW_SHM_CAPACITY,
        schema_ipc_frame,
    )
    .map_err(SpannerError::Protocol)
}

/// Write a complete Arrow IPC byte stream into an unregistered `VTBARROW` shared-memory region,
/// returning `(shm_name, payload_len, Arc<ShmRegion>)`.
pub fn write_spanner_shm_frame(
    ipc_bytes: &[u8],
) -> Result<(String, usize, Arc<ShmRegion>), SpannerError> {
    let shm_name = next_spanner_shm_name();
    write_shm_arrow_region_unregistered(&shm_name, ipc_bytes).map_err(SpannerError::Protocol)
}

#[cfg(test)]
mod tests {
    use vertebra_db::arrow_shm::{
        ARROW_IPC_EOS_MARKER, SHM_ARROW_MAGIC, ShmArrowFrameHeader, read_shm_arrow_frame_with,
        release_shm_arrow_frame,
    };

    use super::*;

    #[test]
    fn spanner_shm_frame_and_stream_writer_roundtrip() {
        let payload = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];
        let (shm_name, byte_len, region) =
            write_spanner_shm_frame(&payload).expect("write spanner shm frame");
        assert!(shm_name.starts_with("/vtb-sp-"));
        assert_eq!(byte_len, payload.len());

        // SAFETY: `region` was initialized by `write_spanner_shm_frame` and is immutable.
        let raw = unsafe { region.as_slice() };
        let header = ShmArrowFrameHeader::from_slice(raw).expect("valid VTBARROW header");
        assert_eq!(&header.magic, SHM_ARROW_MAGIC);
        assert_eq!(header.byte_len as usize, payload.len());
        assert_eq!(
            &raw[ShmArrowFrameHeader::SIZE..ShmArrowFrameHeader::SIZE + byte_len],
            &payload
        );

        let schema_prefix = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let writer = open_spanner_shm_writer(&schema_prefix).expect("open spanner shm writer");
        let (registered_name, stream_len, _stream_region) =
            writer.finish().expect("finish spanner shm writer");
        assert_eq!(stream_len, schema_prefix.len() + ARROW_IPC_EOS_MARKER.len());
        let read_back = read_shm_arrow_frame_with(&registered_name, |bytes| bytes.to_vec())
            .expect("read back spanner shm stream");
        assert_eq!(&read_back[..8], &schema_prefix);
        assert_eq!(&read_back[8..], &ARROW_IPC_EOS_MARKER);
        assert!(release_shm_arrow_frame(&registered_name));
    }
}
