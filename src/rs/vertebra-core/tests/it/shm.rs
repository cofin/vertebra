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
    // SAFETY: `region` is exclusively owned in this test and mapped for at least 4096 bytes.
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
    // SAFETY: `region` is exclusively owned in this test prior to opening `second`.
    unsafe {
        region.as_mut_slice()[42] = 0xFF;
    }
    let second = ShmRegion::open_named(&name, 4096).expect("open_named failed");
    // SAFETY: `second` maps the same SHM backing and no concurrent writes occur during this read.
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
fn live_owner_blocks_create_named_with_in_use_and_reaps_dead_owner() {
    if let Ok(mode) = std::env::var("VTB_SHM_OWNER_CHILD") {
        let name = std::env::var("VTB_SHM_OWNER_NAME").expect("child seg name");
        let region = ShmRegion::create_named(&name, 4096).expect("child create_named");
        if mode == "forget" {
            std::mem::forget(region);
            return;
        }
        if mode == "panic" {
            let name2 = std::env::var("VTB_SHM_OWNER_NAME_2").expect("child seg name 2");
            let region2 = ShmRegion::create_named(&name2, 4096).expect("child create_named 2");
            println!("CREATED");
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                vertebra_core::cleanup::cleanup_all();
                prev(info);
            }));
            std::mem::forget(region);
            std::mem::forget(region2);
            panic!("simulated supervisor panic");
        }
        if mode == "sigterm" {
            vertebra_core::cleanup::install_signal_cleanup().expect("install_signal_cleanup");
            std::mem::forget(region);
            println!("READY");
            loop {
                std::thread::park();
            }
        }
        return;
    }

    let live_name = crate::unique_shm_name("/vtb-live");
    let mut first = match ShmRegion::create_named(&live_name, 4096) {
        Ok(r) => r,
        Err(ShmError::Sys(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(e) => panic!("{e:?}"),
    };
    // SAFETY: `first` is exclusively owned in this test thread.
    unsafe {
        first.as_mut_slice()[0] = 0x5A;
    }
    assert_eq!(first.owner_pid(), Some(std::process::id()));
    assert!(first.owner_alive());

    let err = ShmRegion::create_named(&live_name, 4096).unwrap_err();
    assert!(matches!(err, ShmError::InUse { owner_pid } if owner_pid == std::process::id()));
    // SAFETY: `first` mapping remains valid while held.
    unsafe {
        assert_eq!(first.as_slice()[0], 0x5A);
    }
    drop(first);

    let exe = std::env::current_exe().expect("current_exe");
    let dead_name = crate::unique_shm_name("/vtb-dead");
    let dead_status = std::process::Command::new(&exe)
        .env("VTB_SHM_OWNER_CHILD", "forget")
        .env("VTB_SHM_OWNER_NAME", &dead_name)
        .arg("--exact")
        .arg("shm::live_owner_blocks_create_named_with_in_use_and_reaps_dead_owner")
        .status()
        .expect("spawn dead owner child");
    assert!(dead_status.success());

    let reclaimed = ShmRegion::create_named(&dead_name, 4096).expect("reclaim dead owner");
    assert_eq!(reclaimed.owner_pid(), Some(std::process::id()));
    assert!(reclaimed.owner_generation().unwrap_or(0) >= 2);
    drop(reclaimed);
}

#[cfg(unix)]
#[test]
fn open_or_create_named_multi_thread_single_creator_and_cleanup_hooks() {
    use std::os::unix::process::ExitStatusExt;
    use std::sync::Barrier;
    use vertebra_core::shm::{NamedAttach, SHM_INIT_READY};

    struct BarrierDropGuard<'a>(&'a Barrier);
    impl Drop for BarrierDropGuard<'_> {
        fn drop(&mut self) {
            self.0.wait();
        }
    }

    let name = crate::unique_shm_name("/vtb-ooc");
    let threads = 8usize;
    let barrier = std::sync::Arc::new(Barrier::new(threads));
    let done_barrier = std::sync::Arc::new(Barrier::new(threads));
    let results: Vec<(bool, u32)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let seg = name.clone();
            let bar = std::sync::Arc::clone(&barrier);
            let done_bar = std::sync::Arc::clone(&done_barrier);
            handles.push(s.spawn(move || {
                bar.wait();
                let attached = ShmRegion::open_or_create_named(&seg, 4096, |reg| {
                    // SAFETY: creator holds `SHM_INIT_PENDING` exclusivity during initialization.
                    unsafe {
                        reg.as_mut_slice()[0] = 0x7E;
                    }
                    Ok(())
                });
                let _done_guard = BarrierDropGuard(&done_bar);
                match attached.expect("open_or_create_named") {
                    NamedAttach::Created(r) => {
                        let pid = r.owner_pid().unwrap_or(0);
                        drop(_done_guard);
                        assert!(r.unlink_now());
                        (true, pid)
                    }
                    NamedAttach::Attached(r) => {
                        let pid = r.owner_pid().unwrap_or(0);
                        // SAFETY: initializer finished with `SHM_INIT_READY` Release before attach.
                        unsafe {
                            assert_eq!(r.as_slice()[0], 0x7E);
                        }
                        drop(_done_guard);
                        (false, pid)
                    }
                }
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(SHM_INIT_READY, 1);
    assert_eq!(results.iter().filter(|(created, _)| *created).count(), 1);
    assert_eq!(results.iter().filter(|(created, _)| !*created).count(), 7);
    assert!(results.iter().all(|(_, pid)| *pid == std::process::id()));
    let _ = vertebra_core::shm::shm_unlink_named(&name);

    let exe = std::env::current_exe().expect("current_exe");
    let p1 = crate::unique_shm_name("/vtb-panic1");
    let p2 = crate::unique_shm_name("/vtb-panic2");
    let panic_out = std::process::Command::new(&exe)
        .env("VTB_SHM_OWNER_CHILD", "panic")
        .env("VTB_SHM_OWNER_NAME", &p1)
        .env("VTB_SHM_OWNER_NAME_2", &p2)
        .arg("--exact")
        .arg("shm::live_owner_blocks_create_named_with_in_use_and_reaps_dead_owner")
        .arg("--nocapture")
        .output()
        .expect("spawn panic child");
    assert!(!panic_out.status.success());
    assert!(
        String::from_utf8_lossy(&panic_out.stdout).contains("CREATED"),
        "panic child exited before creating both segments"
    );
    assert!(ShmRegion::open_named(&p1, 4096).is_err());
    assert!(ShmRegion::open_named(&p2, 4096).is_err());
    #[cfg(target_os = "linux")]
    {
        assert!(!std::path::Path::new(&format!("/dev/shm{p1}")).exists());
        assert!(!std::path::Path::new(&format!("/dev/shm{p2}")).exists());
    }

    let reap_dead = crate::unique_shm_name("/vtb-reap-only");
    let child_dead = std::process::Command::new(&exe)
        .env("VTB_SHM_OWNER_CHILD", "forget")
        .env("VTB_SHM_OWNER_NAME", &reap_dead)
        .arg("--exact")
        .arg("shm::live_owner_blocks_create_named_with_in_use_and_reaps_dead_owner")
        .status()
        .expect("spawn reap child");
    assert!(child_dead.success());
    let reap_live = crate::unique_shm_name("/vtb-reap-only-live");
    let live_reg = ShmRegion::create_named(&reap_live, 4096).expect("live region");
    let reaped =
        vertebra_core::cleanup::reap_dead_owner_segments(&[reap_dead.as_str(), reap_live.as_str()]);
    assert_eq!(reaped, 1);
    assert!(ShmRegion::open_named(&reap_dead, 4096).is_err());
    assert!(ShmRegion::open_named(&reap_live, 4096).is_ok());
    #[cfg(target_os = "linux")]
    {
        assert!(!std::path::Path::new(&format!("/dev/shm{reap_dead}")).exists());
        assert!(std::path::Path::new(&format!("/dev/shm{reap_live}")).exists());
    }
    drop(live_reg);

    use std::io::BufRead;
    let sig_name = crate::unique_shm_name("/vtb-sigterm");
    let mut sig_child = std::process::Command::new(&exe)
        .env("VTB_SHM_OWNER_CHILD", "sigterm")
        .env("VTB_SHM_OWNER_NAME", &sig_name)
        .arg("--exact")
        .arg("shm::live_owner_blocks_create_named_with_in_use_and_reaps_dead_owner")
        .arg("--nocapture")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn sigterm child");
    let stdout = sig_child.stdout.take().expect("piped stdout");
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    let mut ready_seen = false;
    while reader.read_line(&mut line).expect("read child line") > 0 {
        if line.contains("READY") {
            ready_seen = true;
            break;
        }
        line.clear();
    }
    assert!(ready_seen, "sigterm child exited before READY");
    let child_pid = sig_child.id() as libc::pid_t;
    // SAFETY: `child_pid` is our live child process spawned above.
    assert_eq!(unsafe { libc::kill(child_pid, libc::SIGTERM) }, 0);
    let sig_status = sig_child.wait().expect("wait sigterm child");
    assert_eq!(sig_status.signal(), Some(libc::SIGTERM));
    assert!(ShmRegion::open_named(&sig_name, 4096).is_err());
    #[cfg(target_os = "linux")]
    assert!(!std::path::Path::new(&format!("/dev/shm{sig_name}")).exists());
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
    // SAFETY: `req_region` was freshly allocated with `req_layout.total_size` bytes and is
    // uniquely owned prior to initializing the ring header.
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

#[cfg(unix)]
#[test]
fn uds_broker_attach_passes_shm_fd_and_rejects_bad_secret_perms_and_truncated_frame() {
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;
    use vertebra_core::broker::ShmLeaseTable;
    use vertebra_core::broker_attach::{AttachError, BrokerAttachClient, BrokerAttachServer};

    let dir = tempfile::tempdir().expect("tempdir");
    let sock_dir = dir.path().join("sockdir");
    std::fs::create_dir(&sock_dir).expect("mkdir");
    std::fs::set_permissions(&sock_dir, std::fs::Permissions::from_mode(0o755)).expect("chmod 755");
    let sock_path = sock_dir.join("broker.sock");

    let req_size = ShmLeaseTable::required_size_with_statements(2, 1, 16);
    let region = ShmRegion::create(req_size).expect("anon shm");
    let shm_fd = region.handle().expect("shm handle");
    let table =
        ShmLeaseTable::init_in_region_with_statements(region, 2, 1, 16).expect("init table");

    // 0755 directory is rejected before binding.
    let bad_dir_err = BrokerAttachServer::bind(
        &sock_path,
        b"super-secret-key-32-bytes-long!!",
        shm_fd,
        req_size,
    )
    .unwrap_err();
    assert!(matches!(bad_dir_err, AttachError::InsecurePermissions(_)));

    std::fs::set_permissions(&sock_dir, std::fs::Permissions::from_mode(0o700)).expect("chmod 700");
    let server = BrokerAttachServer::bind(
        &sock_path,
        b"super-secret-key-32-bytes-long!!",
        shm_fd,
        req_size,
    )
    .expect("bind 0700 dir");
    let handle = server.start_background().expect("start background uds");

    // Wrong secret is rejected with AuthFailed and zero secret leakage.
    let wrong_err = BrokerAttachClient::attach(
        &sock_path,
        b"wrong-secret-key-value-mismatch!",
        Duration::from_secs(2),
    )
    .unwrap_err();
    assert!(matches!(wrong_err, AttachError::AuthFailed(_)));
    assert!(!wrong_err.to_string().contains("super-secret-key"));

    // Truncated handshake frame is handled cleanly without crashing the background server.
    let mut raw_client = UnixStream::connect(&sock_path).expect("raw connect");
    raw_client.write_all(b"VTBA\x01").expect("partial write");
    drop(raw_client);

    // Valid secret receives the SHM descriptor and maps identical lease table.
    let (recv_fd, recv_size) = BrokerAttachClient::attach(
        &sock_path,
        b"super-secret-key-32-bytes-long!!",
        Duration::from_secs(2),
    )
    .expect("valid attach");
    assert_eq!(recv_size, req_size);

    let raw_fd = BrokerAttachClient::into_raw_fd(recv_fd);
    let attached_region = ShmRegion::from_fd(raw_fd, recv_size).expect("from_fd");
    let attached_table = ShmLeaseTable::attach_region(attached_region).expect("attach_region");

    let g = table.try_acquire(999, 0, 0).expect("acquire on owner");
    assert_eq!(attached_table.stats().active_leases, 1);
    attached_table
        .release(g.slot_index, g.generation)
        .expect("release via attached table");
    assert_eq!(table.stats().active_leases, 0);

    // Relaxed parent directory permissions fail client-side before sending secret.
    std::fs::set_permissions(&sock_dir, std::fs::Permissions::from_mode(0o755)).expect("chmod 755");
    let client_perm_err = BrokerAttachClient::attach(
        &sock_path,
        b"super-secret-key-32-bytes-long!!",
        Duration::from_secs(2),
    )
    .unwrap_err();
    assert!(matches!(
        client_perm_err,
        AttachError::InsecurePermissions(_)
    ));
    std::fs::set_permissions(&sock_dir, std::fs::Permissions::from_mode(0o700))
        .expect("restore 700");
    let _ = g.slot_index;
    let _ = handle.socket_path().as_os_str();
    let _ = shm_fd.as_raw_fd();
}
