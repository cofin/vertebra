use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use _storage::store::{BackendConfig, ObjectStoreEngine, PutConditionalOptions};

const MIB: usize = 1024 * 1024;
const PAYLOAD_SIZES: [(&str, usize); 2] = [("1MiB", MIB), ("64MiB", 64 * MIB)];

fn bench_put_variant(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    engine: &ObjectStoreEngine,
    group_name: &str,
    checksum: bool,
) {
    let mut group = c.benchmark_group(group_name);
    for (label, size) in PAYLOAD_SIZES {
        let payload = Bytes::from(vec![0x5Au8; size]);
        let opts = PutConditionalOptions {
            checksum,
            ..Default::default()
        };
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &payload, |b, data| {
            b.iter(|| {
                runtime
                    .block_on(engine.put_opts("bench/payload.bin", data.clone(), opts.clone()))
                    .expect("put_opts failed");
            });
        });
    }
    group.finish();
}

fn bench_put_memory(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build benchmark runtime");
    let engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("create memory object store");

    bench_put_variant(c, &runtime, &engine, "put_memory_default", false);
    bench_put_variant(c, &runtime, &engine, "put_memory_checksum", true);
}

criterion_group!(benches, bench_put_memory);
criterion_main!(benches);
