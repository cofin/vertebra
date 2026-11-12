//! Integration tests for `vertebra_core::shm`.

use vertebra_core::shm::{ShmError, ShmRegion};

#[test]
fn create_anonymous_region() {
    let region = ShmRegion::create(4096).expect("create failed");
    assert!(region.len() >= 4096);
}

#[test]
fn write_and_read_anonymous() {
    let mut region = ShmRegion::create(4096).expect("create failed");
    unsafe {
        region.as_mut_slice()[0] = 0xDE;
        region.as_mut_slice()[1] = 0xAD;
        assert_eq!(region.as_slice()[0], 0xDE);
        assert_eq!(region.as_slice()[1], 0xAD);
    }
}

#[test]
fn zero_size_is_rejected() {
    let err = ShmRegion::create(0).unwrap_err();
    assert!(matches!(err, ShmError::InvalidSize));
}

#[cfg(unix)]
#[test]
fn named_region_create_open_cleanup() {
    let name = crate::unique_shm_name("/vtb-shm");
    let mut region = match ShmRegion::create_named(&name, 4096) {
        Ok(r) => r,
        Err(ShmError::Sys(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(e) => panic!("create_named failed: {e:?}"),
    };
    unsafe {
        region.as_mut_slice()[42] = 0xFF;
    }
    let second = ShmRegion::open_named(&name, 4096).expect("open_named failed");
    unsafe {
        assert_eq!(second.as_slice()[42], 0xFF, "data visible across mappings");
    }
    drop(region); // Triggers unlink via Drop
    drop(second);
    // After drop, the name should be gone from /dev/shm
    let err = ShmRegion::open_named(&name, 4096).unwrap_err();
    assert!(
        matches!(err, ShmError::Sys(_)),
        "expected Sys error after unlink, got {err:?}"
    );
}

#[cfg(unix)]
#[test]
fn invalid_name_no_leading_slash() {
    let err = ShmRegion::create_named("no-slash", 1024).unwrap_err();
    assert!(matches!(err, ShmError::InvalidName));
}

#[cfg(unix)]
#[test]
fn stale_region_replaced_on_create_named() {
    let name = crate::unique_shm_name("/vtb-stale");
    let first = match ShmRegion::create_named(&name, 4096) {
        Ok(r) => r,
        Err(ShmError::Sys(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(e) => panic!("{e:?}"),
    };
    // Forget without unlinking (simulate crash — drop without unlink)
    std::mem::forget(first);
    // Manually unlink to simulate what the OS would leave behind
    unsafe {
        let cname = std::ffi::CString::new(name.as_str()).unwrap();
        libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600);
    }
    let second = ShmRegion::create_named(&name, 4096).expect("create_named with stale name");
    drop(second);
}

#[test]
fn region_size_is_page_aligned() {
    // Request 1 byte; should get at least a page back.
    let region = ShmRegion::create(1).expect("create failed");
    assert!(
        region.len() >= 4096,
        "size should be page-aligned, got {}",
        region.len()
    );
}

#[test]
fn tiered_ring_set_default_absorbs_256_burst_active_mask_and_zero_copy_drain() {
    use std::sync::Arc;
    use vertebra_core::TieredRingSet;
    use vertebra_core::buffer::{ChannelMode, RingLayout};
    use vertebra_core::channel::MpmcRing;
    use vertebra_core::tiered_ring::TieredRingConfig;
    use vertebra_core::transport::{BatchSnapshot, TransportNode};

    let config = TieredRingConfig::default();
    assert!(
        config.small.slot_count >= 512,
        "default small tier must absorb c=256+ bursts without overflow"
    );

    let set = TieredRingSet::create(&config).expect("create TieredRingSet");
    assert_eq!(set.active_tier_mask(), 0);

    for i in 0..300u64 {
        set.send_response(i, 200, b"\x04host\x09\x00localhost", b"ok")
            .expect("300 small responses must fit without overflow");
    }
    assert_eq!(set.active_tier_mask(), 0b0001);

    let mut seen = Vec::with_capacity(300);
    let drained = set
        .drain_with(512, |corr_id, status, headers_raw, body| {
            assert_eq!(status, 200);
            assert_eq!(headers_raw, b"\x04host\x09\x00localhost");
            assert_eq!(body, b"ok");
            seen.push(corr_id);
        })
        .expect("drain_with");
    assert_eq!(drained, 300);
    assert_eq!(seen.len(), 300);
    assert_eq!(seen[0], 0);
    assert_eq!(seen[299], 299);
    assert_eq!(set.active_tier_mask(), 0);

    // Zero-copy SHM BatchSnapshot: body slice points directly inside ShmRegion mapping.
    let req_layout = RingLayout::new(4096, 8, ChannelMode::Mpmc);
    let req_region = Arc::new(ShmRegion::create(req_layout.total_size).expect("req region"));
    let region_start = req_region.as_ptr() as usize;
    let region_end = region_start + req_region.len();
    let req_ring = Arc::new(unsafe {
        MpmcRing::initialize(Arc::clone(&req_region), req_layout, 0, 0).expect("init mpmc")
    });

    let payload = b"/zero-copy-path{\"echo\":true}";
    let node = TransportNode {
        method_tag: 1,
        http_version_tag: 1,
        _pad: [0; 2],
        handler_id: 99,
        correlation_id: 777,
        path_offset: 0,
        path_len: 15,
        query_offset: 0,
        query_len: 0,
        headers_offset: 0,
        headers_len: 0,
        body_offset: 15,
        body_len: 13,
    };
    let src_snap = BatchSnapshot::new(payload.to_vec(), vec![node]);
    req_ring
        .try_send_with(0, |slot| src_snap.serialize_into(slot))
        .expect("serialize into shm slot");

    let slot_view = req_ring.try_recv_view().expect("recv mpmc slot view");
    let shm_snap = BatchSnapshot::from_mpmc_slot(slot_view).expect("from_mpmc_slot");
    assert_eq!(shm_snap.len(), 1);
    assert_eq!(shm_snap.path(0), "/zero-copy-path");
    assert_eq!(shm_snap.body(0), b"{\"echo\":true}");
    let body_ptr = shm_snap.body(0).as_ptr() as usize;
    assert!(
        (region_start..region_end).contains(&body_ptr),
        "SHM BatchSnapshot body slice must point directly into ShmRegion without heap copy"
    );
}

#[test]
fn tiered_ring_set_drains_non_small_tier_even_when_active_mask_is_cleared() {
    use std::sync::atomic::Ordering;
    use vertebra_core::TieredRingSet;
    use vertebra_core::tiered_ring::TieredRingConfig;

    let set = TieredRingSet::create(&TieredRingConfig::default()).expect("create TieredRingSet");
    let medium_body = vec![b'm'; 6000];

    set.send_response(41, 200, b"", &medium_body)
        .expect("send medium response 1");
    set.small.active_flags().store(0, Ordering::SeqCst);

    let mut seen_ids = Vec::new();
    let drained = set
        .drain_with(16, |corr_id, status, _headers, body| {
            assert_eq!(status, 200);
            assert_eq!(body.len(), 6000);
            seen_ids.push(corr_id);
        })
        .expect("drain_with fallback to has_data_acquire");
    assert_eq!(drained, 1);
    assert_eq!(seen_ids, vec![41]);

    set.send_response(42, 201, b"", &medium_body)
        .expect("send medium response 2");
    set.small.active_flags().store(0, Ordering::SeqCst);
    let resp = set
        .try_recv_response()
        .expect("try_recv_response")
        .expect("medium response must be discovered via has_data_acquire");
    assert_eq!(resp.correlation_id, 42);
    assert_eq!(resp.status, 201);
    assert_eq!(resp.body.len(), 6000);

    set.send_response(43, 202, b"", &medium_body)
        .expect("send medium response 3");
    set.small.active_flags().store(0, Ordering::SeqCst);
    let view = set
        .try_recv_view()
        .expect("try_recv_view")
        .expect("medium slot view must be discovered via has_data_acquire");
    assert!(!view.as_slice().is_empty());
}

#[test]
fn stream_frames_preserve_cross_size_order_and_unary_flags() {
    use vertebra_core::ipc::{FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING};
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
    let mut config = RingPairConfig::compact();
    config.response_tiers.xlarge.slot_count = 8;
    let pair = RingPair::create(config).unwrap();
    let large = vec![b'b'; 5000];
    pair.send_stream_frame(
        11,
        200,
        FLAG_STREAMING | FLAG_HEADERS_SENT,
        b"content-type",
        b"a",
    )
    .unwrap();
    pair.send_stream_frame(11, 200, FLAG_STREAMING, b"", &large)
        .unwrap();
    pair.send_stream_frame(11, 200, FLAG_STREAMING | FLAG_FINAL, b"", b"")
        .unwrap();
    let mut frames = Vec::new();
    assert_eq!(
        pair.drain_frames_with(8, |frame| frames.push((
            frame.correlation_id,
            frame.flags,
            frame.headers_raw.to_vec(),
            frame.body.to_vec()
        )))
        .unwrap(),
        3
    );
    assert_eq!(
        frames,
        vec![
            (
                11,
                FLAG_STREAMING | FLAG_HEADERS_SENT,
                b"content-type".to_vec(),
                b"a".to_vec()
            ),
            (11, FLAG_STREAMING, vec![], large),
            (11, FLAG_STREAMING | FLAG_FINAL, vec![], vec![])
        ]
    );
    pair.response_rings
        .send_response(12, 201, b"", b"ok")
        .unwrap();
    pair.drain_frames_with(8, |frame| {
        assert_eq!(frame.flags, FLAG_FINAL);
        assert_eq!(frame.status, 201);
        assert_eq!(frame.body, b"ok");
    })
    .unwrap();
    pair.response_rings
        .send_response(12, 201, b"", b"ok")
        .unwrap();
    pair.response_rings
        .drain_with(8, |id, status, headers, body| {
            assert_eq!(
                (id, status, headers, body),
                (12, 201, b"".as_slice(), b"ok".as_slice())
            )
        })
        .unwrap();
}

#[test]
fn stream_frames_validate_lengths_capacity_and_transport_backend() {
    use vertebra_core::transport::ring_pair::{
        RESPONSE_STREAM_MAGIC, ResponseData, RingPair, RingPairConfig, RingPairError,
    };
    use vertebra_core::transport::{Transport, TransportError};
    let pair = RingPair::create(RingPairConfig::compact()).unwrap();
    let capacity = pair.max_stream_frame_payload();
    pair.send_stream_frame(1, 200, 1, b"", &vec![b'x'; capacity])
        .unwrap();
    pair.drain_frames_with(1, |frame| assert_eq!(frame.body.len(), capacity))
        .unwrap();
    assert!(matches!(
        pair.send_stream_frame(1, 200, 1, b"", &vec![0; capacity + 1]),
        Err(RingPairError::ResponseTooLarge(_))
    ));
    assert!(matches!(
        pair.send_stream_frame(1, 200, 1, &vec![0; 65536], b""),
        Err(RingPairError::ResponseTooLarge(_))
    ));
    assert!(matches!(
        ResponseData::parse_frame(&[0; 32]),
        Err(RingPairError::InvalidResponse("bad magic"))
    ));
    let mut truncated = vec![0; 32];
    truncated[..4].copy_from_slice(&RESPONSE_STREAM_MAGIC.to_le_bytes());
    truncated[18..22].copy_from_slice(&100u32.to_le_bytes());
    assert!(matches!(
        ResponseData::parse_frame(&truncated),
        Err(RingPairError::InvalidResponse("truncated"))
    ));
    let (_tx, rx) = crossbeam_channel::unbounded();
    let transport = Transport::new_in_process(rx);
    assert_eq!(transport.max_stream_frame_payload(), None);
    assert!(matches!(
        transport.send_stream_frame(1, 200, 1, b"", b"x"),
        Err(TransportError::Disconnected)
    ));
    assert!(matches!(
        transport.send_stream_frame_blocking(1, 200, 1, b"", b"x"),
        Err(TransportError::Disconnected)
    ));
}

#[test]
fn stream_frame_backpressure_waits_until_the_reader_releases_a_slot() {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use vertebra_core::channel::ChannelError;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig, RingPairError};
    let mut config = RingPairConfig::compact();
    config.response_tiers.xlarge.slot_count = 2;
    let pair = Arc::new(RingPair::create(config).unwrap());
    pair.send_stream_frame(1, 200, 1, b"", b"a").unwrap();
    assert!(matches!(
        pair.send_stream_frame(1, 200, 1, b"", b"b"),
        Err(RingPairError::Channel(ChannelError::Full))
    ));
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer = pair.clone();
    let thread = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        writer
            .send_stream_frame_blocking(1, 200, 1, b"", b"b")
            .unwrap();
        finished_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        finished_rx.recv_timeout(Duration::from_millis(20)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    let mut bodies = Vec::new();
    pair.drain_frames_with(1, |frame| bodies.push(frame.body.to_vec()))
        .unwrap();
    finished_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    thread.join().unwrap();
    pair.drain_frames_with(8, |frame| bodies.push(frame.body.to_vec()))
        .unwrap();
    assert_eq!(bodies, vec![b"a".to_vec(), b"b".to_vec()]);
}
