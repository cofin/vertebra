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
fn spsc_full_returns_error() {
    // SPSC ring reserves one slot to distinguish full from empty,
    // so a 4-slot ring holds 3 messages.
    let (_region, ring) = make_spsc(4, 256);
    let payload = [0u8; 64];
    for _ in 0..3 {
        ring.try_send(&payload, 0).expect("send should succeed");
    }
    let err = ring.try_send(&payload, 0).unwrap_err();
    assert!(
        matches!(err, ChannelError::Full),
        "expected Full, got {err:?}"
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
fn spsc_recv_hybrid_timeout() {
    let (_region, ring) = make_spsc(16, 256);
    let mut buf = [0u8; 256];
    let start = std::time::Instant::now();
    let result = ring
        .recv_hybrid(&mut buf, 5, Duration::from_millis(10))
        .expect("recv_hybrid failed");
    let elapsed = start.elapsed();
    assert!(result.is_none(), "should timeout");
    assert!(
        elapsed.as_millis() >= 8,
        "should wait ~10ms before timeout: {elapsed:?}"
    );
}

#[test]
fn spsc_send_sync_bounds() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SpscRing>();
}
