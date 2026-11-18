use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use vertebra_core::buffer::{ChannelMode, RingLayout};
use vertebra_core::channel::SpscRing;

use vertebra_core::shm::ShmRegion;

fn bench_spsc_ping_pong(c: &mut Criterion) {
    let layout = RingLayout::new(256, 1024, ChannelMode::Spsc);
    let alloc = std::alloc::Layout::from_size_align(
        layout.total_size,
        vertebra_core::buffer::CACHELINE_BYTES,
    )
    .unwrap();
    // SAFETY: `alloc` has non-zero `layout.total_size` and `CACHELINE_BYTES` alignment.
    let ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!ptr.is_null());
    // SAFETY: `ptr` is non-null, `CACHELINE_BYTES`-aligned, and valid for `layout.total_size` bytes
    // until `dealloc` at the end of this benchmark function.
    let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
    // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
    let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
    let payload = [7u8; 8];
    let mut out = [0u8; 8];
    c.bench_function("spsc_ping_pong", |b| {
        b.iter(|| {
            ring.send(&payload, 0).unwrap();
            let _ = ring.recv(&mut out).unwrap();
        })
    });
    drop(ring);
    drop(region);
    // SAFETY: `ptr` was allocated with `alloc` and all ring/region handles are dropped.
    unsafe {
        std::alloc::dealloc(ptr, alloc);
    }
}

fn bench_spsc_throughput(c: &mut Criterion) {
    let layout = RingLayout::new(256, 1024, ChannelMode::Spsc);
    let alloc = std::alloc::Layout::from_size_align(
        layout.total_size,
        vertebra_core::buffer::CACHELINE_BYTES,
    )
    .unwrap();
    // SAFETY: `alloc` has non-zero `layout.total_size` and `CACHELINE_BYTES` alignment.
    let ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!ptr.is_null());
    // SAFETY: `ptr` is non-null, `CACHELINE_BYTES`-aligned, and valid for `layout.total_size` bytes
    // until `dealloc` at the end of this benchmark function.
    let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
    // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
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
    drop(ring);
    drop(region);
    // SAFETY: `ptr` was allocated with `alloc` and all ring/region handles are dropped.
    unsafe {
        std::alloc::dealloc(ptr, alloc);
    }
}

/// Benchmark recv_hybrid with data already available (immediate path).
fn bench_spsc_recv_hybrid_immediate(c: &mut Criterion) {
    let layout = RingLayout::new(256, 1024, ChannelMode::Spsc);
    let alloc = std::alloc::Layout::from_size_align(
        layout.total_size,
        vertebra_core::buffer::CACHELINE_BYTES,
    )
    .unwrap();
    // SAFETY: `alloc` has non-zero `layout.total_size` and `CACHELINE_BYTES` alignment.
    let ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!ptr.is_null());
    // SAFETY: `ptr` is non-null, `CACHELINE_BYTES`-aligned, and valid for `layout.total_size` bytes
    // until `dealloc` at the end of this benchmark function.
    let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
    // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
    let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
    let payload = [42u8; 64];
    let mut out = [0u8; 256];

    // Pre-fill with one message
    ring.send(&payload, 0).unwrap();

    c.bench_function("spsc_recv_hybrid_immediate", |b| {
        b.iter(|| {
            // Receive the pre-existing message
            let len = ring
                .recv_hybrid(&mut out, 5, Duration::from_millis(100))
                .unwrap()
                .expect("should receive immediately");
            assert_eq!(len, 64);

            // Re-fill for next iteration
            ring.send(&payload, 0).unwrap();
        })
    });

    drop(ring);
    drop(region);
    // SAFETY: `ptr` was allocated with `alloc` and all ring/region handles are dropped.
    unsafe {
        std::alloc::dealloc(ptr, alloc);
    }
}

/// Benchmark recv_hybrid blocked wake - data arrives after wait starts.
///
/// This tests the futex-based wakeup path latency.
fn bench_spsc_recv_hybrid_wakeup(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_recv_hybrid_wakeup");

    for delay_us in [10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(delay_us),
            &delay_us,
            |b, &delay_us| {
                let layout = RingLayout::new(256, 64, ChannelMode::Spsc);
                let alloc = std::alloc::Layout::from_size_align(
                    layout.total_size,
                    vertebra_core::buffer::CACHELINE_BYTES,
                )
                .unwrap();
                // SAFETY: `alloc` has non-zero `layout.total_size` and `CACHELINE_BYTES` alignment.
                let ptr = unsafe { std::alloc::alloc(alloc) };
                assert!(!ptr.is_null());
                // SAFETY: `ptr` is non-null, `CACHELINE_BYTES`-aligned, and valid for `layout.total_size`
                // bytes until `dealloc` after all threads and handles finish.
                let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
                // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
                let ring = Arc::new(unsafe {
                    SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap()
                });
                let payload = vec![42u8; 64];
                let mut buffer = vec![0u8; 256];

                b.iter(|| {
                    let ring_clone = Arc::clone(&ring);
                    let payload_clone = payload.clone();

                    // Spawn sender that waits before sending
                    let sender = thread::spawn(move || {
                        thread::sleep(Duration::from_micros(delay_us));
                        ring_clone.send(&payload_clone, 0).unwrap();
                    });

                    // Receiver waits for data using hybrid recv
                    let len = ring
                        .recv_hybrid(&mut buffer, 5, Duration::from_millis(1000))
                        .expect("recv_hybrid failed")
                        .expect("timeout");
                    assert_eq!(len, 64);

                    sender.join().expect("sender panicked");
                });

                drop(ring);
                drop(region);
                // SAFETY: `ptr` was allocated with `alloc` and all ring/region handles are dropped.
                unsafe {
                    std::alloc::dealloc(ptr, alloc);
                }
            },
        );
    }
    group.finish();
}

/// Benchmark pure wakeup latency with persistent sender thread.
///
/// This measures the actual IPC wakeup time without thread spawn/join overhead.
fn bench_spsc_recv_hybrid_pure_wakeup(c: &mut Criterion) {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let layout = RingLayout::new(256, 64, ChannelMode::Spsc);
    let alloc = std::alloc::Layout::from_size_align(
        layout.total_size,
        vertebra_core::buffer::CACHELINE_BYTES,
    )
    .unwrap();
    // SAFETY: `alloc` has non-zero `layout.total_size` and `CACHELINE_BYTES` alignment.
    let ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!ptr.is_null());
    // SAFETY: `ptr` is non-null, `CACHELINE_BYTES`-aligned, and valid for `layout.total_size` bytes
    // until `dealloc` after the sender thread joins and handles drop.
    let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
    // SAFETY: exclusive initialization of freshly allocated `region` matching `layout`.
    let ring =
        Arc::new(unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() });

    let stop = Arc::new(AtomicBool::new(false));
    let request_counter = Arc::new(AtomicU64::new(0));
    let payload = vec![42u8; 64];
    let mut buffer = vec![0u8; 256];

    // Start persistent sender thread
    let ring_sender = Arc::clone(&ring);
    let stop_sender = Arc::clone(&stop);
    let request_counter_sender = Arc::clone(&request_counter);
    let payload_sender = payload.clone();

    let sender_handle = thread::spawn(move || {
        let mut last_seen = 0u64;
        while !stop_sender.load(Ordering::Relaxed) {
            let current = request_counter_sender.load(Ordering::Acquire);
            if current > last_seen {
                // Request detected, send response
                ring_sender.send(&payload_sender, 0).unwrap();
                last_seen = current;
            } else {
                // Busy wait for next request
                std::hint::spin_loop();
            }
        }
    });

    c.bench_function("spsc_recv_hybrid_pure_wakeup", |b| {
        b.iter(|| {
            // Signal sender to send
            request_counter.fetch_add(1, Ordering::Release);

            // Wait for response using hybrid recv
            let len = ring
                .recv_hybrid(&mut buffer, 5, Duration::from_millis(100))
                .expect("recv_hybrid failed")
                .expect("timeout");
            assert_eq!(len, 64);
        })
    });

    stop.store(true, Ordering::Release);
    sender_handle.join().unwrap();

    drop(ring);
    drop(region);
    // SAFETY: `ptr` was allocated with `alloc`, the sender thread joined, and all handles are dropped.
    unsafe {
        std::alloc::dealloc(ptr, alloc);
    }
}

criterion_group!(
    benches,
    bench_spsc_ping_pong,
    bench_spsc_throughput,
    bench_spsc_recv_hybrid_immediate,
    bench_spsc_recv_hybrid_wakeup,
    bench_spsc_recv_hybrid_pure_wakeup,
);
criterion_main!(benches);
