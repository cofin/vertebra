//! Integration tests for `vertebra_core::channel::MpmcRing`.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use vertebra_core::channel::MpmcRing;

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
