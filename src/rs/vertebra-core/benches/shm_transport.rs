//! SHM transport benchmarks — measures the core primitives for multi-process IPC.
//!
//! Benchmarks:
//! - SPSC ring latency at various payload sizes
//! - BatchSnapshot serialize/deserialize throughput
//! - Tiered ring tier selection cost
//! - Transport parity: InProcess vs SharedMemory poll_batch
//! - CorrelationSlab insert/complete vs baseline

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;
use std::time::Duration;

use vertebra_core::buffer::{ChannelMode, RingLayout};
use vertebra_core::channel::SpscRing;
use vertebra_core::shm::ShmRegion;
use vertebra_core::tiered_ring::TieredRingConfig;
use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig, TieredRingSet};
use vertebra_core::transport::{BatchSnapshot, TransportNode};

/// Helper: create an SPSC ring backed by heap memory.
fn create_ring(slot_size: usize, slots: usize) -> Arc<SpscRing> {
    let layout = RingLayout::new(slot_size, slots, ChannelMode::Spsc);
    let region = Arc::new(ShmRegion::create(layout.total_size).unwrap());
    Arc::new(unsafe { SpscRing::initialize(region, layout, 0, 0).unwrap() })
}

// ---------------------------------------------------------------------------
// 1. SPSC Ring Latency (various payload sizes)
// ---------------------------------------------------------------------------

fn bench_spsc_ring_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_ring_latency");

    for payload_size in [64, 1024, 4096, 65536] {
        let slot_size = payload_size + 64; // account for SlotHeader
        let ring = create_ring(slot_size, 64);
        let payload = vec![0xABu8; payload_size];
        let mut recv_buf = vec![0u8; payload_size + 64];

        group.throughput(Throughput::Bytes(payload_size as u64));
        group.bench_with_input(
            BenchmarkId::new("send_recv", payload_size),
            &payload_size,
            |b, _| {
                b.iter(|| {
                    ring.try_send(&payload, 0).unwrap();
                    ring.try_recv(&mut recv_buf).unwrap();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. BatchSnapshot Serialize/Deserialize
// ---------------------------------------------------------------------------

fn bench_batch_snapshot_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_snapshot_serde");

    for node_count in [1, 16, 64] {
        let slab = vec![0u8; 4096]; // 4KB slab
        let nodes: Vec<TransportNode> = (0..node_count)
            .map(|i| TransportNode {
                method_tag: 0,
                http_version_tag: 1,
                _pad: [0; 2],
                handler_id: i as u64,
                correlation_id: i as u64 + 1000,
                path_offset: 0,
                path_len: 10,
                query_offset: 0,
                query_len: 0,
                headers_offset: 0,
                headers_len: 0,
                body_offset: 0,
                body_len: 0,
            })
            .collect();

        let snap = BatchSnapshot::new(slab.clone(), nodes);
        let size = snap.serialized_size();
        let mut buf = vec![0u8; size];

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(
            BenchmarkId::new("serialize", node_count),
            &node_count,
            |b, _| {
                b.iter(|| {
                    snap.serialize_into(&mut buf);
                });
            },
        );

        // Pre-serialize for deserialize bench
        snap.serialize_into(&mut buf);

        group.bench_with_input(
            BenchmarkId::new("deserialize", node_count),
            &node_count,
            |b, _| {
                b.iter(|| {
                    let _ = BatchSnapshot::deserialize_from(&buf).unwrap();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Tiered Ring Tier Selection
// ---------------------------------------------------------------------------

fn bench_tiered_ring_selection(c: &mut Criterion) {
    let config = TieredRingConfig::default();

    c.bench_function("tiered_ring_select_tier", |b| {
        let sizes = [100, 2000, 5000, 50_000, 100_000, 1_500_000];
        let mut idx = 0;
        b.iter(|| {
            let _ = config.select_tier(sizes[idx % sizes.len()]);
            idx += 1;
        });
    });
}

// ---------------------------------------------------------------------------
// 4. Transport Parity: InProcess vs SharedMemory
// ---------------------------------------------------------------------------

fn bench_transport_parity(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport_parity");

    // Build a test BatchSnapshot
    let slab = vec![0u8; 512];
    let nodes: Vec<TransportNode> = (0..8)
        .map(|i| TransportNode {
            method_tag: 0,
            http_version_tag: 1,
            _pad: [0; 2],
            handler_id: i,
            correlation_id: i + 100,
            path_offset: 0,
            path_len: 5,
            query_offset: 0,
            query_len: 0,
            headers_offset: 0,
            headers_len: 0,
            body_offset: 0,
            body_len: 0,
        })
        .collect();

    let snap = Arc::new(BatchSnapshot::new(slab.clone(), nodes.clone()));

    // InProcess path: crossbeam channel send + recv
    group.bench_function("inprocess_crossbeam", |b| {
        let (tx, rx) = crossbeam_channel::bounded(64);
        b.iter(|| {
            tx.send(Arc::clone(&snap)).unwrap();
            let _ = rx.recv().unwrap();
        });
    });

    // SharedMemory path: serialize into ring + recv from ring + deserialize
    group.bench_function("shm_ring_roundtrip", |b| {
        let pair = RingPair::create(RingPairConfig::default()).unwrap();
        let mut ser_buf = vec![0u8; snap.serialized_size()];
        let mut recv_buf = vec![0u8; pair.request_ring.payload_capacity()];

        b.iter(|| {
            snap.serialize_into(&mut ser_buf);
            pair.request_ring.try_send(&ser_buf, 0).unwrap();
            let len = pair.request_ring.try_recv(&mut recv_buf).unwrap();
            let _ = BatchSnapshot::deserialize_from(&recv_buf[..len]).unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Response Ring Tiered Send/Recv
// ---------------------------------------------------------------------------

fn bench_response_ring(c: &mut Criterion) {
    let mut group = c.benchmark_group("response_ring");

    let config = TieredRingConfig::default();
    let set = TieredRingSet::create(&config).unwrap();

    for body_size in [64, 1024, 4096] {
        let body = vec![0xCDu8; body_size];
        let headers = b"\x04host\x09\x00localhost";

        group.throughput(Throughput::Bytes(body_size as u64));
        group.bench_with_input(
            BenchmarkId::new("send_recv_response", body_size),
            &body_size,
            |b, _| {
                b.iter(|| {
                    set.send_response(42, 200, headers, &body).unwrap();
                    let _ = set.try_recv_response().unwrap().unwrap();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

criterion_group! {
    name = shm_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(3))
        .sample_size(100);
    targets =
        bench_spsc_ring_latency,
        bench_batch_snapshot_serde,
        bench_tiered_ring_selection,
        bench_transport_parity,
        bench_response_ring,
}

criterion_main!(shm_benches);
