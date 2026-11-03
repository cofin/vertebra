//! Integration tests for `vertebra_db::arrow_shm` and `ShmArrowStreamWriter`.

use vertebra_db::arrow_shm::{
    ACTIVE_ARROW_SHM_REGIONS, ARROW_IPC_EOS_MARKER, ShmArrowFrameHeader, ShmArrowStreamWriter,
    open_shm_arrow_region, read_shm_arrow_frame, read_shm_arrow_frame_with,
    release_shm_arrow_frame,
};

#[test]
fn shm_arrow_stream_writer_grows_pads_and_appends_eos_marker() {
    let shm_name = format!("test-vtb-arrow-grow-{}", std::process::id());
    let _ = release_shm_arrow_frame(&shm_name);

    // 61-byte synthetic schema frame (intentionally not a multiple of 8 bytes).
    let schema_frame = vec![0xA5u8; 61];
    let mut writer =
        ShmArrowStreamWriter::create(&shm_name, 4096, &schema_frame).expect("create stream writer");

    // Append 12 frames of 8,701 bytes each (> 96 KiB total, non-8-byte-multiple frame sizes)
    // to force multiple ShmRegion expansions and verify 8-byte padding.
    let single_frame = vec![0x5Au8; 8701];
    for _ in 0..12 {
        writer
            .append_ipc_frame(&single_frame, 100)
            .expect("append frame");
    }

    assert_eq!(writer.row_count(), 1200);
    assert_eq!(writer.batch_count(), 12);

    let (norm_name, byte_len, region) = writer.finish().expect("finish stream writer");
    assert!(norm_name.starts_with('/'));
    assert!(ACTIVE_ARROW_SHM_REGIONS.contains_key(&norm_name));

    let expected_schema_padded = (schema_frame.len() + 7) & !7;
    let expected_frame_padded = (single_frame.len() + 7) & !7;
    let expected_byte_len =
        expected_schema_padded + (12 * expected_frame_padded) + ARROW_IPC_EOS_MARKER.len();
    assert_eq!(byte_len, expected_byte_len);
    assert_eq!(byte_len % 8, 0);

    let raw_slice = unsafe { region.as_slice() };
    let header = ShmArrowFrameHeader::from_slice(&raw_slice[0..16]).expect("valid header");
    assert_eq!(header.byte_len as usize, byte_len);

    let payload = &raw_slice[16..16 + byte_len];
    assert_eq!(&payload[..schema_frame.len()], &schema_frame[..]);
    assert_eq!(
        &payload[schema_frame.len()..expected_schema_padded],
        &[0u8; 3]
    );
    assert_eq!(&payload[byte_len - 8..], &ARROW_IPC_EOS_MARKER);

    // Verify `open_shm_arrow_region`, `read_shm_arrow_frame_with`, and `read_shm_arrow_frame`
    // while active in the registry.
    let (_, opened_len) = open_shm_arrow_region(&norm_name).expect("open_shm_arrow_region");
    assert_eq!(opened_len, byte_len);
    let visited_len = read_shm_arrow_frame_with(&norm_name, |slice| slice.len())
        .expect("read_shm_arrow_frame_with");
    assert_eq!(visited_len, byte_len);
    let copied = read_shm_arrow_frame(&norm_name).expect("read_shm_arrow_frame");
    assert_eq!(copied.len(), byte_len);

    // Verify `release_shm_arrow_frame` removes the registry entry while the returned `Arc<ShmRegion>`
    // remains alive and readable.
    assert!(release_shm_arrow_frame(&norm_name));
    assert!(!ACTIVE_ARROW_SHM_REGIONS.contains_key(&norm_name));
    let still_readable = unsafe { &region.as_slice()[16..16 + byte_len] };
    assert_eq!(&still_readable[byte_len - 8..], &ARROW_IPC_EOS_MARKER);
}

#[test]
fn shm_arrow_stream_writer_strips_intermediate_eos_markers_on_raw_chunks() {
    let shm_name = format!("test-vtb-arrow-chunks-{}", std::process::id());
    let _ = release_shm_arrow_frame(&shm_name);

    let mut schema_with_eos = vec![0x11u8; 32];
    schema_with_eos.extend_from_slice(&ARROW_IPC_EOS_MARKER);

    let mut writer = ShmArrowStreamWriter::create(&shm_name, 4096, &schema_with_eos)
        .expect("create writer with schema+eos");

    for _ in 0..3 {
        let mut chunk_with_eos = vec![0x22u8; 48];
        chunk_with_eos.extend_from_slice(&ARROW_IPC_EOS_MARKER);
        writer
            .append_raw_stream_chunk(&chunk_with_eos, 50)
            .expect("append raw chunk");
    }

    assert_eq!(writer.row_count(), 150);
    assert_eq!(writer.batch_count(), 3);

    let (norm_name, byte_len, _region) = writer.finish().expect("finish");
    // 32-byte schema + 3 * 48-byte chunks + single 8-byte trailing EOS = 184 bytes.
    assert_eq!(byte_len, 32 + (3 * 48) + 8);
    read_shm_arrow_frame_with(&norm_name, |payload| {
        assert_eq!(&payload[0..32], &[0x11u8; 32]);
        assert_eq!(&payload[32..176], &[0x22u8; 144]);
        assert_eq!(&payload[176..184], &ARROW_IPC_EOS_MARKER);
    })
    .expect("read_shm_arrow_frame_with");

    assert!(release_shm_arrow_frame(&norm_name));
}
