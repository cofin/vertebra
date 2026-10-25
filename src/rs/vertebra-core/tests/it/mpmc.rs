//! Integration tests for `vertebra_core::channel::MpmcRing`.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use vertebra_core::channel::{ChannelError, MpmcRing};

#[test]
fn mpmc_buffer_too_small_does_not_wedge() {
    let ring = MpmcRing::new(256, 2).expect("new failed");
    let payload = [42u8; 32];
    ring.send(&payload, 0).expect("send failed");

    let mut small_buf = [0u8; 8];
    let err = ring
        .try_recv(&mut small_buf)
        .expect_err("should fail with buffer too small");
    assert!(matches!(
        err,
        ChannelError::BufferTooSmall {
            len: 32,
            capacity: 8
        }
    ));

    // Verify ring wraps and does not wedge across 4 more frames
    for i in 0..4u8 {
        let msg = [i; 16];
        ring.send(&msg, 0).expect("subsequent send must succeed");
        let mut buf = [0u8; 32];
        let len = ring
            .try_recv(&mut buf)
            .expect("subsequent recv must succeed");
        assert_eq!(&buf[..len], &msg);
    }
}

#[test]
fn mpmc_try_send_with_clamps_oversized_and_survives_panic() {
    let ring = MpmcRing::new(256, 2).expect("new failed");

    // Test panic unwinding does not wedge slot sequence
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = ring.try_send_with(0, |_buf| {
            panic!("deliberate panic inside try_send_with");
        });
    }));

    // Next send and receive must succeed
    ring.send(b"after_panic", 0)
        .expect("send after panic must succeed");
    let mut buf = [0u8; 32];
    let len = ring
        .try_recv(&mut buf)
        .expect("recv after panic must succeed");
    assert_eq!(&buf[..len], b"after_panic");

    // Test returning written > payload_capacity() returns PayloadTooLarge and frees slot
    let cap = ring.payload_capacity();
    let err = ring
        .try_send_with(0, |_buf| cap + 10)
        .expect_err("oversized written must fail");
    assert!(matches!(err, ChannelError::PayloadTooLarge { .. }));

    // Verify slot was released and ring continues to function
    ring.send(b"after_oversized", 0)
        .expect("send after oversized must succeed");
    let len = ring
        .try_recv(&mut buf)
        .expect("recv after oversized must succeed");
    assert_eq!(&buf[..len], b"after_oversized");
}

#[test]
fn mpmc_single_thread_roundtrip() {
    // MpmcRing::new(slot_size, slots)
    let ring = MpmcRing::new(256, 16).expect("new failed");
    let payload = b"mpmc-test";
    ring.send(payload, 0).expect("send failed");
    let mut buf = [0u8; 256];
    let len = ring.try_recv(&mut buf).expect("recv failed");
    assert_eq!(&buf[..len], payload);
}

#[test]
fn mpmc_4p_4c_message_counts() {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const MESSAGES_PER_PRODUCER: u64 = 2_500;
    const TOTAL: u64 = PRODUCERS as u64 * MESSAGES_PER_PRODUCER;

    let ring = Arc::new(MpmcRing::new(256, 1024).expect("new failed"));
    let received = Arc::new(AtomicU64::new(0));
    let producers_active = Arc::new(std::sync::atomic::AtomicUsize::new(PRODUCERS));

    let mut handles = Vec::new();

    // Spawn producers
    for p in 0..PRODUCERS {
        let ring = Arc::clone(&ring);
        let producers_active = Arc::clone(&producers_active);
        handles.push(std::thread::spawn(move || {
            let payload = (p as u64).to_le_bytes();
            for _ in 0..MESSAGES_PER_PRODUCER {
                loop {
                    match ring.send(&payload, 0) {
                        Ok(()) => break,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            }
            producers_active.fetch_sub(1, Ordering::Release);
        }));
    }

    // Spawn consumers — they drain until producers are done AND ring is empty
    for _ in 0..CONSUMERS {
        let ring = Arc::clone(&ring);
        let received = Arc::clone(&received);
        let producers_active = Arc::clone(&producers_active);
        handles.push(std::thread::spawn(move || {
            let mut buf = [0u8; 256];
            let mut local_count = 0u64;
            loop {
                match ring.try_recv(&mut buf) {
                    Ok(_) => {
                        local_count += 1;
                        // Periodically flush count
                        if local_count.is_multiple_of(100) {
                            received.fetch_add(local_count, Ordering::AcqRel);
                            local_count = 0;
                        }
                    }
                    Err(_) => {
                        if producers_active.load(Ordering::Acquire) == 0 {
                            // Producers done; try one more drain pass
                            match ring.try_recv(&mut buf) {
                                Ok(_) => local_count += 1,
                                Err(_) => break,
                            }
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                }
            }
            received.fetch_add(local_count, Ordering::AcqRel);
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    // Drain any remaining after all threads join
    let mut buf = [0u8; 256];
    while ring.try_recv(&mut buf).is_ok() {
        received.fetch_add(1, Ordering::AcqRel);
    }

    assert_eq!(
        received.load(Ordering::Acquire),
        TOTAL,
        "should receive all {TOTAL} messages"
    );
}

#[test]
fn mpmc_send_sync_bounds() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MpmcRing>();
}

#[test]
fn mpmc_try_recv_empty_is_error() {
    let ring = MpmcRing::new(256, 16).unwrap();
    let mut buf = [0u8; 256];
    let result = ring.try_recv(&mut buf);
    assert!(result.is_err(), "empty ring should return error");
}

#[test]
fn mpmc_padded_sequences_zero_copy_slot_views_and_dekker_wake() {
    use vertebra_core::buffer::{CACHELINE_BYTES, ChannelMode, RingLayout};
    use vertebra_core::notify::Notifier;

    let layout = RingLayout::new(256, 16, ChannelMode::Mpmc);
    assert_eq!(
        layout.seq_size,
        16 * CACHELINE_BYTES,
        "each MPMC sequence counter must occupy its own cache line"
    );

    let ring = Arc::new(MpmcRing::new(256, 4).expect("new mpmc"));
    assert!(matches!(ring.try_recv_view(), Err(ChannelError::Empty)));

    // Hold multiple concurrent MpmcSlotViews simultaneously and release out-of-order.
    ring.try_send(b"slot-0", 10).expect("send 0");
    ring.try_send(b"slot-1", 20).expect("send 1");

    let view0 = ring.try_recv_view().expect("view 0");
    let view1 = ring.try_recv_view().expect("view 1");
    assert_eq!(view0.as_slice(), b"slot-0");
    assert_eq!(view0.flags(), 10);
    assert_eq!(view1.as_slice(), b"slot-1");
    assert_eq!(view1.flags(), 20);

    drop(view1);
    drop(view0);

    // Wrap around all 4 slots to prove out-of-order release restored sequences cleanly.
    for i in 0..8u32 {
        ring.try_send(&i.to_le_bytes(), i).expect("wrap send");
        let v = ring.try_recv_view().expect("wrap view");
        assert_eq!(u32::from_le_bytes(v.as_slice().try_into().unwrap()), i);
        assert_eq!(v.flags(), i);
    }

    // Dekker sleep/wake on MPMC ring.
    let notifier = Notifier::new().expect("notifier");
    assert!(ring.prepare_sleep());
    ring.try_send(b"wake-mpmc", 1).expect("send wake");
    assert!(ring.notify_if_sleeping(&notifier).expect("notify"));
    assert!(notifier.wait(50).expect("wait"));
    notifier.clear().expect("clear");
    let view = ring.try_recv_view().expect("recv woken");
    assert_eq!(view.as_slice(), b"wake-mpmc");
    drop(view);
}
