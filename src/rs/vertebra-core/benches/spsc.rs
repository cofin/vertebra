use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use vertebra_core::buffer::RingLayout;
use vertebra_core::channel::SpscRing;

use vertebra_core::shm::ShmRegion;

fn bench_spsc_ping_pong(c: &mut Criterion) {
    let layout = RingLayout::new(256, 1024);
    let alloc = std::alloc::Layout::from_size_align(
        layout.total_size,
        vertebra_core::buffer::CACHELINE_BYTES,
    )
    .unwrap();
    let ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!ptr.is_null());
    let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
    let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
    let payload = [7u8; 8];
    let mut out = [0u8; 8];
    c.bench_function("spsc_ping_pong", |b| {
        b.iter(|| {
            ring.send(&payload, 0).unwrap();
            let _ = ring.recv(&mut out).unwrap();
        })
    });
    unsafe {
        std::alloc::dealloc(ptr, alloc);
    }
}

fn bench_spsc_throughput(c: &mut Criterion) {
    let layout = RingLayout::new(256, 1024);
    let alloc = std::alloc::Layout::from_size_align(
        layout.total_size,
        vertebra_core::buffer::CACHELINE_BYTES,
    )
    .unwrap();
    let ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!ptr.is_null());
    let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
    let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
    let payload = [3u8; 32];
    let mut out = [0u8; 32];
    c.bench_function("spsc_throughput", |b| {
        b.iter(|| {
            for _ in 0..256 {
                ring.send(&payload, 0).unwrap();
                let _ = ring.recv(&mut out).unwrap();
            }
        })
    });
    unsafe {
        std::alloc::dealloc(ptr, alloc);
    }
}

criterion_group!(benches, bench_spsc_ping_pong, bench_spsc_throughput);
criterion_main!(benches);
