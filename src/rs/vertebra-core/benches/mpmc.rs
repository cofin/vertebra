use criterion::{criterion_group, criterion_main, Criterion};
use vertebra_core::channel::MpmcRing;

fn bench_mpmc_single_thread(c: &mut Criterion) {
    let ring = MpmcRing::new(64, 1024).unwrap();
    let payload = [1u8; 8];
    let mut out = [0u8; 8];
    c.bench_function("mpmc_single_thread", |b| {
        b.iter(|| {
            ring.send(&payload, 0).unwrap();
            let _ = ring.recv(&mut out).unwrap();
        })
    });
}

criterion_group!(benches, bench_mpmc_single_thread);
criterion_main!(benches);
