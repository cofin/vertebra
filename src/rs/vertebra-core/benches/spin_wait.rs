//! Benchmarks for hybrid spin-wait IPC latency.
//!
//! Target latencies:
//! - Immediate data (data already available): < 5µs
//! - Blocked wake (data arrives after wait starts): < 10µs

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use vertebra_core::async_ring::AsyncRing;

/// Benchmark immediate data path - data is already in the ring.
fn bench_recv_hybrid_immediate(c: &mut Criterion) {
    let ring = AsyncRing::new(4096, 64).expect("failed to create ring");
    let payload = vec![42u8; 64];
    let mut buffer = vec![0u8; 4096];

    // Pre-fill with one message
    ring.try_send(&payload, 0).expect("send failed");

    c.bench_function("recv_hybrid_immediate", |b| {
        b.iter(|| {
            // Receive the pre-existing message
            let len = ring
                .recv_hybrid(&mut buffer, 5, 100)
                .expect("recv failed")
                .expect("no data");
            assert_eq!(len, 64);

            // Re-fill for next iteration
            ring.try_send(&payload, 0).expect("refill failed");
        })
    });
}

/// Benchmark try_recv fast path - no syscall when data available.
fn bench_try_recv_immediate(c: &mut Criterion) {
    let ring = AsyncRing::new(4096, 64).expect("failed to create ring");
    let payload = vec![42u8; 64];
    let mut buffer = vec![0u8; 4096];

    // Pre-fill
    ring.try_send(&payload, 0).expect("send failed");

    c.bench_function("try_recv_immediate", |b| {
        b.iter(|| {
            let len = ring
                .try_recv(&mut buffer)
                .expect("recv failed")
                .expect("no data");
            assert_eq!(len, 64);
            ring.try_send(&payload, 0).expect("refill failed");
        })
    });
}

/// Benchmark blocked wake - data arrives after wait starts.
fn bench_recv_hybrid_wakeup(c: &mut Criterion) {
    let mut group = c.benchmark_group("recv_hybrid_wakeup");

    for delay_us in [10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(delay_us),
            &delay_us,
            |b, &delay_us| {
                let ring = Arc::new(AsyncRing::new(4096, 64).expect("failed to create ring"));
                let payload = vec![42u8; 64];
                let mut buffer = vec![0u8; 4096];

                b.iter(|| {
                    let ring_clone = Arc::clone(&ring);
                    let payload_clone = payload.clone();

                    // Spawn sender that waits before sending
                    let sender = thread::spawn(move || {
                        thread::sleep(Duration::from_micros(delay_us));
                        ring_clone.try_send(&payload_clone, 0).expect("send failed");
                    });

                    // Receiver waits for data
                    let len = ring
                        .recv_hybrid(&mut buffer, 5, 1000)
                        .expect("recv failed")
                        .expect("timeout");
                    assert_eq!(len, 64);

                    sender.join().expect("sender panicked");
                })
            },
        );
    }
    group.finish();
}

/// Benchmark spin phase efficiency - varying spin durations.
fn bench_spin_duration(c: &mut Criterion) {
    let mut group = c.benchmark_group("spin_duration");

    for spin_us in [0, 2, 5, 10, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(spin_us),
            &spin_us,
            |b, &spin_us| {
                let ring = AsyncRing::new(4096, 64).expect("failed to create ring");
                let payload = vec![42u8; 64];
                let mut buffer = vec![0u8; 4096];

                // Pre-fill
                ring.try_send(&payload, 0).expect("send failed");

                b.iter(|| {
                    let len = ring
                        .recv_hybrid(&mut buffer, spin_us, 100)
                        .expect("recv failed")
                        .expect("no data");
                    assert_eq!(len, 64);
                    ring.try_send(&payload, 0).expect("refill failed");
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_recv_hybrid_immediate,
    bench_try_recv_immediate,
    bench_recv_hybrid_wakeup,
    bench_spin_duration,
);
criterion_main!(benches);
