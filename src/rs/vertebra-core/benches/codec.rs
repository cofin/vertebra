//! Benchmarks for compression codec throughput.
//!
//! Tests lz4 and zstd at representative IPC payload sizes:
//! 256B (minimal), 4KB (typical), 64KB (headers-heavy), 1MB (large body).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use vertebra_core::protocol::codec::{compress, decompress, CompressionCodec};

fn make_payload(size: usize) -> Vec<u8> {
    // Semi-realistic: repeating JSON-like pattern (compresses moderately)
    let pattern = br#"{"handler_id":12345,"path":"/api/v1/items","method":"GET","ts":1234567890}"#;
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        let remaining = size - data.len();
        let chunk = &pattern[..remaining.min(pattern.len())];
        data.extend_from_slice(chunk);
    }
    data
}

fn bench_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec/compress");
    let sizes: &[(usize, &str)] = &[
        (256, "256B"),
        (4096, "4KB"),
        (65536, "64KB"),
        (1_048_576, "1MB"),
    ];

    for &(size, label) in sizes {
        let payload = make_payload(size);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("lz4", label), &payload, |b, data| {
            b.iter(|| compress(data, CompressionCodec::Lz4));
        });

        group.bench_with_input(BenchmarkId::new("zstd", label), &payload, |b, data| {
            b.iter(|| compress(data, CompressionCodec::Zstd));
        });
    }
    group.finish();
}

fn bench_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec/decompress");
    let sizes: &[(usize, &str)] = &[
        (256, "256B"),
        (4096, "4KB"),
        (65536, "64KB"),
        (1_048_576, "1MB"),
    ];

    for &(size, label) in sizes {
        let payload = make_payload(size);
        let lz4_data = compress(&payload, CompressionCodec::Lz4);
        let zstd_data = compress(&payload, CompressionCodec::Zstd);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("lz4", label), &lz4_data, |b, data| {
            b.iter(|| decompress(data, CompressionCodec::Lz4).unwrap());
        });

        group.bench_with_input(BenchmarkId::new("zstd", label), &zstd_data, |b, data| {
            b.iter(|| decompress(data, CompressionCodec::Zstd).unwrap());
        });
    }
    group.finish();
}

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec/roundtrip");
    let payload = make_payload(4096);
    group.throughput(Throughput::Bytes(4096));

    group.bench_function("lz4", |b| {
        b.iter(|| {
            let c = compress(&payload, CompressionCodec::Lz4);
            decompress(&c, CompressionCodec::Lz4).unwrap()
        });
    });

    group.bench_function("zstd", |b| {
        b.iter(|| {
            let c = compress(&payload, CompressionCodec::Zstd);
            decompress(&c, CompressionCodec::Zstd).unwrap()
        });
    });

    group.bench_function("none", |b| {
        b.iter(|| {
            let c = compress(&payload, CompressionCodec::None);
            decompress(&c, CompressionCodec::None).unwrap()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_compress, bench_decompress, bench_roundtrip);
criterion_main!(benches);
