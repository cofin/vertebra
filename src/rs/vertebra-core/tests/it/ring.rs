//! Integration tests for `vertebra_core::channel::SpscRing`.

use std::sync::Arc;
use std::time::Duration;
use vertebra_core::buffer::{CACHELINE_BYTES, ChannelMode, RingLayout};
use vertebra_core::channel::{ChannelError, SpscRing};
use vertebra_core::shm::ShmRegion;

fn make_spsc(slots: usize, slot_size: usize) -> (Arc<ShmRegion>, SpscRing) {
    let layout = RingLayout::new(slot_size, slots, ChannelMode::Spsc);
    let alloc = std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
    let ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!ptr.is_null());
    let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
    let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
    (region, ring)
}

#[test]
fn spsc_single_thread_roundtrip() {
    let (_region, ring) = make_spsc(16, 256);
    let payload = b"hello vertebra";
    ring.try_send(payload, 0).expect("send failed");
    let mut buf = [0u8; 256];
    let len = ring.try_recv(&mut buf).expect("recv failed");
    assert_eq!(&buf[..len], payload);
}

#[test]
fn spsc_empty_returns_error() {
    let (_region, ring) = make_spsc(16, 256);
    let mut buf = [0u8; 256];
    let err = ring.try_recv(&mut buf).unwrap_err();
    assert!(
        matches!(err, ChannelError::Empty),
        "expected Empty, got {err:?}"
    );
}

#[test]
fn spsc_fill_then_drain() {
    // 8-slot ring holds 7 messages (one slot reserved for full/empty distinction)
    let (_region, ring) = make_spsc(8, 256);
    let payloads: Vec<Vec<u8>> = (0..7u8).map(|i| vec![i; 16]).collect();
    for p in &payloads {
        ring.try_send(p, 0).expect("send");
    }
    let mut buf = [0u8; 256];
    for expected in &payloads {
        let len = ring.try_recv(&mut buf).expect("recv");
        assert_eq!(&buf[..len], expected.as_slice());
    }
}

#[test]
fn spsc_multithreaded_producer_consumer() {
    const N: u64 = 10_000;
    let (_region, ring) = make_spsc(256, 256);
    let ring = Arc::new(ring);
    let ring_send = Arc::clone(&ring);

    let producer = std::thread::spawn(move || {
        for i in 0u64..N {
            let bytes = i.to_le_bytes();
            loop {
                match ring_send.try_send(&bytes, 0) {
                    Ok(()) => break,
                    Err(ChannelError::Full) => std::hint::spin_loop(),
                    Err(e) => panic!("send error: {e}"),
                }
            }
        }
    });

    let mut buf = [0u8; 256];
    for expected in 0u64..N {
        loop {
            match ring.try_recv(&mut buf) {
                Ok(len) => {
                    assert_eq!(len, 8);
                    let got = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    assert_eq!(got, expected, "sequence mismatch at {expected}");
                    break;
                }
                Err(ChannelError::Empty) => std::hint::spin_loop(),
                Err(e) => panic!("recv error: {e}"),
            }
        }
    }
    producer.join().expect("producer panicked");
}

#[test]
fn spsc_recv_hybrid_no_block_when_data_present() {
    let (_region, ring) = make_spsc(16, 256);
    ring.try_send(b"test", 0).unwrap();
    let mut buf = [0u8; 256];
    let start = std::time::Instant::now();
    let result = ring
        .recv_hybrid(&mut buf, 5, Duration::from_millis(100))
        .expect("recv_hybrid failed");
    let elapsed = start.elapsed();
    assert!(result.is_some(), "should receive data");
    assert!(
        elapsed.as_micros() < 500,
        "should not block when data present: {elapsed:?}"
    );
}

#[test]
fn spsc_send_sync_bounds() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SpscRing>();
}

#[test]
fn spsc_try_send_with_survives_panic() {
    let (_region, ring) = make_spsc(16, 256);

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = ring.try_send_with(32, 0, |_slice| {
            panic!("deliberate panic in spsc try_send_with");
        });
    }));

    // Verify ring continues to work correctly after panic
    ring.try_send(b"after_panic", 0)
        .expect("send after panic must succeed");
    let mut buf = [0u8; 256];
    let len = ring
        .try_recv(&mut buf)
        .expect("recv after panic must succeed");
    assert_eq!(&buf[..len], b"after_panic");
}

#[test]
fn read_slot_zero_copy_rejects_corrupted_shm_len() {
    let (region, ring) = make_spsc(16, 256);
    let ring = Arc::new(ring);

    ring.try_send(b"valid", 0).expect("send should succeed");

    // Corrupt the SlotHeader.len of the first slot in SHM to u32::MAX
    unsafe {
        let shm_header = &*(region.as_ptr() as *const vertebra_core::buffer::ShmHeader);
        let slot_header = (region.as_ptr().add(shm_header.ring_offset as usize))
            as *mut vertebra_core::buffer::SlotHeader;
        (*slot_header).len = u32::MAX;
    }

    let err = ring
        .try_recv_view()
        .expect_err("corrupted slot must return error");
    assert!(matches!(err, ChannelError::CorruptedSlot));
}

#[test]
fn spsc_abi_v3_dekker_sleep_wake_skips_awake_and_wakes_sleeping() {
    use vertebra_core::buffer::ABI_VERSION;
    use vertebra_core::notify::Notifier;

    assert_eq!(ABI_VERSION, 3);
    let (_region, ring) = make_spsc(16, 256);
    let notifier = Notifier::new().expect("notifier");

    // Consumer is awake (consumer_sleeping == 0): producer must skip syscall.
    ring.try_send(b"awake-msg", 0).expect("send");
    let notified = ring
        .notify_if_sleeping(&notifier)
        .expect("notify_if_sleeping");
    assert!(!notified, "awake consumer must not trigger notifier write");
    assert!(
        !notifier.wait(0).expect("poll"),
        "notifier fd must remain unreadable when consumer was awake"
    );

    // While data is queued, prepare_sleep must refuse to sleep.
    assert!(
        !ring.prepare_sleep(),
        "prepare_sleep must return false when ring already has unread data"
    );

    let mut out = [0u8; 64];
    let len = ring.try_recv(&mut out).expect("recv");
    assert_eq!(&out[..len], b"awake-msg");

    // Ring is now empty: prepare_sleep arms consumer_sleeping == 1.
    assert!(
        ring.prepare_sleep(),
        "prepare_sleep must return true when ring is empty"
    );
    ring.try_send(b"wake-msg", 0).expect("send while sleeping");
    let notified = ring
        .notify_if_sleeping(&notifier)
        .expect("notify_if_sleeping sleeping");
    assert!(notified, "sleeping consumer must be notified once");
    assert!(
        notifier.wait(50).expect("wait"),
        "notifier fd must become readable"
    );
    notifier.clear().expect("clear");

    // Second notify without re-arming prepare_sleep must coalesce (skip write).
    let notified_again = ring
        .notify_if_sleeping(&notifier)
        .expect("second notify_if_sleeping");
    assert!(
        !notified_again,
        "consumer_sleeping flag must be cleared on wake"
    );
    ring.cancel_sleep();
}

#[test]
fn spsc_drain_with_zero_copy_and_cached_heads_fifo() {
    let (_region, ring) = make_spsc(16, 256);
    let ring = Arc::new(ring);

    assert!(matches!(ring.try_recv_view(), Err(ChannelError::Empty)));

    // Exercise multiple wrap-arounds across 16-slot ring using cached heads + drain_with.
    let mut next_expected = 0u64;
    for round in 0..8u64 {
        for offset in 0..12u64 {
            let seq = round * 12 + offset;
            ring.try_send(&seq.to_le_bytes(), (seq & 0xff) as u32)
                .expect("try_send in burst");
        }
        let mut visited = 0usize;
        let drained = ring
            .drain_with(8, |slice, flags| {
                let got = u64::from_le_bytes(slice.try_into().expect("8 bytes"));
                assert_eq!(got, next_expected);
                assert_eq!(flags, (next_expected & 0xff) as u32);
                next_expected += 1;
                visited += 1;
            })
            .expect("drain_with first chunk");
        assert_eq!(drained, 8);
        assert_eq!(visited, 8);

        let drained_rest = ring
            .drain_with(8, |slice, flags| {
                let got = u64::from_le_bytes(slice.try_into().expect("8 bytes"));
                assert_eq!(got, next_expected);
                assert_eq!(flags, (next_expected & 0xff) as u32);
                next_expected += 1;
            })
            .expect("drain_with remainder");
        assert_eq!(drained_rest, 4);
    }
    assert_eq!(next_expected, 96);
}
