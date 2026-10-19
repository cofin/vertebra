//! Benchmarks for hybrid spin-wait IPC latency.
//!
//! Target latencies:
//! - Immediate data (data already available): < 5µs
//! - Blocked wake (data arrives after wait starts): < 10µs

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use vertebra_core::buffer::{ChannelMode, RingLayout};
use vertebra_core::channel::{ChannelError, SpscRing};
use vertebra_core::notify::Notifier;
use vertebra_core::shm::ShmRegion;

struct BenchRing {
    ring: SpscRing,
    notifier: Notifier,
    _region: Arc<ShmRegion>,
}

impl BenchRing {
    fn new(slot_size: usize, slots: usize) -> Self {
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::create(layout.total_size).expect("create shm"));
        let ring =
            unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).expect("init") };
        let notifier = Notifier::new().expect("notifier");
        Self {
            ring,
            notifier,
            _region: region,
        }
    }

    fn try_send(&self, data: &[u8], flags: u32) {
        self.ring.try_send(data, flags).expect("try_send");
        self.notifier.notify().expect("notify");
    }

    fn try_recv(&self, buffer: &mut [u8]) -> Option<usize> {
        match self.ring.try_recv(buffer) {
            Ok(len) => Some(len),
            Err(ChannelError::Empty) => None,
            Err(e) => panic!("recv error: {e}"),
        }
    }

    fn recv_hybrid(&self, buffer: &mut [u8], spin_us: u64, timeout_ms: i32) -> Option<usize> {
        if let Some(len) = self.try_recv(buffer) {
            let _ = self.notifier.clear();
            return Some(len);
        }
        if !self
            .notifier
            .spin_wait_or_park_with(spin_us, timeout_ms, || self.ring.has_data())
            .expect("spin_wait")
        {
            return None;
        }
        let _ = self.notifier.clear();
        self.try_recv(buffer)
    }
}

/// Benchmark immediate data path - data is already in the ring.
fn bench_recv_hybrid_immediate(c: &mut Criterion) {
    let ring = BenchRing::new(4096, 64);
    let payload = vec![42u8; 64];
    let mut buffer = vec![0u8; 4096];

    ring.try_send(&payload, 0);

    c.bench_function("recv_hybrid_immediate", |b| {
        b.iter(|| {
            let len = ring.recv_hybrid(&mut buffer, 5, 100).expect("no data");
            assert_eq!(len, 64);
            ring.try_send(&payload, 0);
        })
    });
}

/// Benchmark try_recv fast path - no syscall when data available.
fn bench_try_recv_immediate(c: &mut Criterion) {
    let ring = BenchRing::new(4096, 64);
    let payload = vec![42u8; 64];
    let mut buffer = vec![0u8; 4096];

    ring.try_send(&payload, 0);

    c.bench_function("try_recv_immediate", |b| {
        b.iter(|| {
            let len = ring.try_recv(&mut buffer).expect("no data");
            assert_eq!(len, 64);
            ring.try_send(&payload, 0);
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
                let ring = Arc::new(BenchRing::new(4096, 64));
                let payload = vec![42u8; 64];
                let mut buffer = vec![0u8; 4096];

                b.iter(|| {
                    let ring_clone = Arc::clone(&ring);
                    let payload_clone = payload.clone();

                    let sender = thread::spawn(move || {
                        thread::sleep(Duration::from_micros(delay_us));
                        ring_clone.try_send(&payload_clone, 0);
                    });

                    let len = ring.recv_hybrid(&mut buffer, 5, 1000).expect("timeout");
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
                let ring = BenchRing::new(4096, 64);
                let payload = vec![42u8; 64];
                let mut buffer = vec![0u8; 4096];

                ring.try_send(&payload, 0);

                b.iter(|| {
                    let len = ring
                        .recv_hybrid(&mut buffer, spin_us, 100)
                        .expect("no data");
                    assert_eq!(len, 64);
                    ring.try_send(&payload, 0);
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
