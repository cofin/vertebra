//! Ring buffer throughput benchmark.
//!
//! Target: >1 M ops/s single-thread SPSC ping-pong at 256-byte slots.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use std::time::Duration;
use vertebra_core::buffer::{CACHELINE_BYTES, ChannelMode, RingLayout};
use vertebra_core::channel::{MpmcRing, SpscRing};
use vertebra_core::shm::ShmRegion;

/// Allocate a SPSC ring backed by heap memory (not SHM) for benchmarking.
fn make_spsc_heap(slots: usize, slot_size: usize) -> (std::mem::ManuallyDrop<Vec<u8>>, SpscRing) {
    let layout = RingLayout::new(slots, slot_size, ChannelMode::Spsc);
    let alloc = std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
    let raw_ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!raw_ptr.is_null());
    let region = Arc::new(unsafe { ShmRegion::from_raw(raw_ptr, layout.total_size) });
    let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
    let sentinel =
        std::mem::ManuallyDrop::new(unsafe { Vec::from_raw_parts(raw_ptr, 0, layout.total_size) });
    (sentinel, ring)
}

/// Single-threaded SPSC ping-pong: send 1, recv 1, repeat.
/// Target: >1 M iterations/second at 256-byte slots.
fn bench_spsc_1m_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(100);

    for slot_size in [64usize, 256, 1024] {
        group.bench_with_input(
            BenchmarkId::new("ping_pong", slot_size),
            &slot_size,
            |b, &slot_size| {
                let (_sentinel, ring) = make_spsc_heap(256, slot_size);
                let payload = vec![0xABu8; slot_size.min(64)];
                let mut buf = vec![0u8; slot_size];
                b.iter(|| {
                    ring.send(&payload, 0).unwrap();
                    let _ = ring.recv(&mut buf).unwrap();
                });
            },
        );
    }
    group.finish();
}

/// Multi-slot burst: fill ring, then drain. Measures sustained throughput.
fn bench_spsc_burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_burst");
    group.measurement_time(Duration::from_secs(5));

    for burst in [16usize, 64, 256] {
        group.bench_with_input(
            BenchmarkId::new("fill_drain", burst),
            &burst,
            |b, &burst| {
                let (_sentinel, ring) = make_spsc_heap(burst, 256);
                let payload = [0u8; 64];
                let mut buf = [0u8; 256];
                b.iter(|| {
                    for _ in 0..burst {
                        ring.send(&payload, 0).unwrap();
                    }
                    for _ in 0..burst {
                        ring.recv(&mut buf).unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

/// MPMC single-thread baseline (compare to SPSC overhead).
fn bench_mpmc_baseline(c: &mut Criterion) {
    let ring = MpmcRing::new(256, 256).unwrap();
    let payload = [0u8; 64];
    let mut buf = [0u8; 256];
    c.bench_function("mpmc_single_thread_baseline", |b| {
        b.iter(|| {
            ring.send(&payload, 0).unwrap();
            ring.recv(&mut buf).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_spsc_1m_ops,
    bench_spsc_burst,
    bench_mpmc_baseline
);
criterion_main!(benches);
