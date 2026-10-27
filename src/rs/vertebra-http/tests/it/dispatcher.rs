//! Unit tests for the dispatcher.

use std::sync::Arc;
use std::time::Duration;

use crate::BRIDGE_TEST_LOCK as TEST_LOCK;
use tokio::sync::oneshot;
use vertebra_core::bridge::BatchBridge;
use vertebra_core::transport::BatchSnapshot;
use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
use vertebra_http::{
    batcher::{Batch, PendingRequest},
    body::PendingResponse,
    dispatcher::Dispatcher,
};

/// Ensure the global bridge is initialized (idempotent).
fn ensure_bridge() {
    let _ = BatchBridge::init(256, 1_048_576, 4, 1);
}

/// Drain stale batches from the bridge.
fn drain_stale() {
    if let Some(bridge) = BatchBridge::get() {
        while bridge.poll_batch(Duration::from_millis(1)).is_some() {}
        while bridge.try_recv_responses().is_some() {}
    }
}

fn make_batch(count: usize) -> (Batch, Vec<oneshot::Receiver<PendingResponse>>) {
    let mut requests = Vec::new();
    let mut receivers = Vec::new();
    for i in 0..count as u64 {
        let (tx, rx) = oneshot::channel();
        requests.push(PendingRequest {
            handler_id: i,
            method_tag: 0,
            http_version_tag: 1,
            flags: 0,
            path_and_query: http::uri::PathAndQuery::from_static("/"),
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            reply: tx,
        });
        receivers.push(rx);
    }
    (Batch { requests }, receivers)
}

#[tokio::test]
async fn test_dispatch_sends_to_bridge() {
    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale();

    let dispatcher = Arc::new(Dispatcher::new());

    let (batch, _receivers) = make_batch(2);
    dispatcher.dispatch(batch);

    let bridge = BatchBridge::get().unwrap();
    let received = bridge
        .poll_batch(Duration::from_millis(100))
        .expect("dispatch did not produce bridge batch");
    assert_eq!(received.len(), 2);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_complete_unblocks_hyper() {
    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale();

    let dispatcher = Arc::new(Dispatcher::new());

    let (batch, mut receivers) = make_batch(1);
    dispatcher.dispatch(batch);

    // Read the batch from the bridge to get the correlation_id
    let bridge = BatchBridge::get().unwrap();
    let received = bridge.poll_batch(Duration::from_millis(100)).unwrap();
    let id = received.node(0).correlation_id;

    dispatcher.complete(
        id,
        PendingResponse {
            status: 201,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::from_static(b"created"),
            stream_rx: None,
        },
    );

    let resp = tokio::time::timeout(Duration::from_millis(100), receivers.remove(0))
        .await
        .expect("oneshot did not fire")
        .unwrap();

    assert_eq!(resp.status, 201);
    assert_eq!(resp.body.as_ref(), b"created");
}

#[tokio::test]
async fn test_complete_unknown_id_is_noop() {
    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale();

    let dispatcher = Dispatcher::new();
    // Should not panic.
    dispatcher.complete(
        999999,
        PendingResponse {
            status: 200,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            stream_rx: None,
        },
    );
}

#[tokio::test]
async fn test_shm_ring_full_returns_503_without_correlation_capacity() {
    let pair = Arc::new(RingPair::create(RingPairConfig::default()).expect("create ring pair"));
    let dispatcher = Dispatcher::new_shm(vec![Arc::clone(&pair)], 0);

    let payload = vec![0u8; 1024];
    while pair.request_ring.try_send(&payload, 0).is_ok() {}

    let (batch, mut receivers) = make_batch(1);
    dispatcher.dispatch(batch);

    let resp = tokio::time::timeout(Duration::from_millis(100), receivers.remove(0))
        .await
        .expect("dispatcher should fail fast when worker ring is full")
        .expect("reply channel should receive a response");

    assert_eq!(resp.status, 503);
    assert_eq!(resp.body.as_ref(), b"Service Unavailable");
}

#[tokio::test]
async fn test_shm_slab_full_returns_503_instead_of_dropping_request() {
    let pair = Arc::new(RingPair::create(RingPairConfig::default()).expect("create ring pair"));
    let dispatcher = Dispatcher::new_shm(vec![pair], 0);

    let (batch, mut receivers) = make_batch(1);
    dispatcher.dispatch(batch);

    let resp = tokio::time::timeout(Duration::from_millis(100), receivers.remove(0))
        .await
        .expect("dispatcher should fail fast when correlation slab is full")
        .expect("reply channel should receive a response");

    assert_eq!(resp.status, 503);
    assert_eq!(resp.body.as_ref(), b"Service Unavailable");
}

#[tokio::test]
async fn test_shm_complete_many_unblocks_multiple_waiters() {
    let pair = Arc::new(RingPair::create(RingPairConfig::default()).expect("create ring pair"));
    let dispatcher = Dispatcher::new_shm(vec![Arc::clone(&pair)], 16);

    let (batch, receivers) = make_batch(4);
    dispatcher.dispatch(batch);

    let mut correlation_ids = Vec::new();
    let mut buf = vec![0u8; pair.request_ring.payload_capacity()];
    while correlation_ids.len() < 4 {
        let len = pair
            .request_ring
            .try_recv(&mut buf)
            .expect("request should be queued");
        let snap = BatchSnapshot::deserialize_from(&buf[..len]).expect("deserialize request");
        for idx in 0..snap.len() {
            correlation_ids.push(snap.node(idx).correlation_id);
        }
    }

    dispatcher.complete_many(
        correlation_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    PendingResponse {
                        status: 200,
                        headers_mp: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"ok"),
                        stream_rx: None,
                    },
                )
            })
            .collect(),
    );

    for rx in receivers {
        let resp = tokio::time::timeout(Duration::from_millis(100), rx)
            .await
            .expect("oneshot did not fire")
            .expect("reply channel should receive a response");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_ref(), b"ok");
    }
}

#[tokio::test]
async fn test_shm_dispatch_batches_multiple_requests_into_single_ring_slot() {
    let pair = Arc::new(RingPair::create(RingPairConfig::default()).expect("create ring pair"));
    let dispatcher = Dispatcher::new_shm(vec![Arc::clone(&pair)], 16);

    let (batch, _receivers) = make_batch(4);
    dispatcher.dispatch(batch);

    let mut buf = vec![0u8; pair.request_ring.payload_capacity()];
    let len = pair
        .request_ring
        .try_recv(&mut buf)
        .expect("batched request should be queued");
    let snap = BatchSnapshot::deserialize_from(&buf[..len]).expect("deserialize batch");
    assert_eq!(snap.len(), 4);
    assert!(matches!(
        pair.request_ring.try_recv(&mut buf),
        Err(vertebra_core::channel::ChannelError::Empty)
    ));
}

#[tokio::test]
async fn shm_correlation_preserves_high_generation_bits() {
    use vertebra_http::dispatcher::correlation::{CorrelationId, InFlightRequests};

    let mut slab = InFlightRequests::new(64);
    let (tx, rx) = oneshot::channel();
    let shard: u8 = 3;
    let high_gen: u32 = 0x0102_0304;

    slab.set_generation(0, high_gen.wrapping_sub(1));
    let id = slab.insert(shard, tx, 0).expect("insert should succeed");
    assert_eq!(id.generation(), high_gen);
    assert_eq!(id.shard(), shard as usize);
    assert_eq!(id.slot(), 0);

    // Roundtrip raw wire u64
    let wire_raw = id.raw();
    let decoded = CorrelationId::from_raw(wire_raw);
    assert_eq!(decoded.generation(), high_gen);
    assert_eq!(decoded.shard(), shard as usize);
    assert_eq!(decoded.slot(), 0);

    let completed = slab.complete(
        decoded,
        PendingResponse {
            status: 200,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::from_static(b"high-gen-ok"),
            stream_rx: None,
        },
    );
    assert!(
        completed,
        "complete must succeed with generation >= 0x0100_0000"
    );

    let resp = rx.await.expect("channel should receive response");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body.as_ref(), b"high-gen-ok");
}

#[tokio::test]
async fn shm_worker_crash_sweeps_inflight_with_502_and_reclaims_rings() {
    use std::sync::Mutex;
    use vertebra_http::dispatcher::ShmDispatcher;
    use vertebra_http::workers::{RingFactory, Supervisor, WorkerLifecycle};

    let mut factory = RingFactory::new(1, RingPairConfig::default()).expect("create factory");
    let pair_0 = factory.ring_pair(0);

    // Create a mock child process using `true`
    let child = std::process::Command::new("true")
        .spawn()
        .expect("spawn child");
    let lifecycle = WorkerLifecycle::new(
        child,
        Arc::clone(&pair_0),
        0,
        "app:app".into(),
        "python".into(),
    );
    factory.add_lifecycle(lifecycle);

    let factory = Arc::new(Mutex::new(factory));
    let shm = ShmDispatcher::new(vec![Arc::clone(&pair_0)], 16);
    let dispatcher = Dispatcher::from_shm(Arc::clone(&shm));

    let (batch, mut receivers) = make_batch(1);
    dispatcher.dispatch(batch);

    // Verify request was put into worker's request ring
    let mut buf = vec![0u8; pair_0.request_ring.payload_capacity()];
    let len = pair_0
        .request_ring
        .try_recv(&mut buf)
        .expect("request in ring");
    assert!(len > 0);

    // Simulate worker 0 crash via Supervisor
    Supervisor::handle_worker_exit(&factory, &shm, 0);

    // Caller must receive 502 Bad Gateway within 50 ms
    let rx = receivers.pop().unwrap();
    let resp = tokio::time::timeout(Duration::from_millis(50), rx)
        .await
        .expect("receiver timed out")
        .expect("channel recv");
    assert_eq!(resp.status, 502);
    assert_eq!(resp.body.as_ref(), b"Bad Gateway");

    // Ring pair must be reclaimed and usable
    assert!(pair_0.request_ring.try_recv(&mut buf).is_err());
    pair_0
        .request_ring
        .try_send(b"after_reclaim", 0)
        .expect("send after reclaim");
    let len = pair_0
        .request_ring
        .try_recv(&mut buf)
        .expect("recv after reclaim");
    assert_eq!(&buf[..len], b"after_reclaim");
}

#[tokio::test]
async fn shm_reader_and_sweeper_exit_on_cancellation() {
    use tokio_util::sync::CancellationToken;
    use vertebra_http::dispatcher::{ShmDispatcher, shm_reader};

    let pair = Arc::new(RingPair::create(RingPairConfig::default()).expect("create ring pair"));
    let shm = ShmDispatcher::new(vec![Arc::clone(&pair)], 16);
    let dispatcher = Arc::new(Dispatcher::from_shm(Arc::clone(&shm)));

    let shutdown = CancellationToken::new();

    let sweeper_handle = shm.spawn_sweeper(shutdown.clone());
    let reader_handle =
        shm_reader::spawn_response_readers(vec![pair], Arc::clone(&dispatcher), shutdown.clone());

    // Brief pause to allow tasks to enter their wait loops
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Trigger cancellation
    shutdown.cancel();

    // Both JoinHandles must complete within 200 ms
    let sweeper_res = tokio::time::timeout(Duration::from_millis(200), sweeper_handle).await;
    let reader_res = tokio::time::timeout(Duration::from_millis(200), reader_handle).await;

    assert!(
        sweeper_res.is_ok(),
        "sweeper did not exit on cancellation within 200ms"
    );
    assert!(
        reader_res.is_ok(),
        "reader did not exit on cancellation within 200ms"
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn test_bridge_256_concurrent_requests_complete_via_direct_response_sink_without_deadlock() {
    use vertebra_core::bridge::BridgeResponse;
    use vertebra_http::dispatcher::DispatchBackend;

    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale();

    let dispatcher = Arc::new(Dispatcher::new());
    let mut all_receivers = Vec::with_capacity(256);

    for batch_idx in 0..8usize {
        let mut requests = Vec::with_capacity(32);
        for item_idx in 0..32usize {
            let seq = (batch_idx * 32 + item_idx) as u64;
            let (tx, rx) = oneshot::channel();
            requests.push(PendingRequest {
                handler_id: seq,
                method_tag: 0,
                http_version_tag: 1,
                flags: 0,
                path_and_query: format!("/item/{seq}?k=v").parse().unwrap(),
                headers_mp: bytes::Bytes::from_static(b"\x06x-test\x02\x00ok"),
                body: bytes::Bytes::from(vec![b'x'; 4096]),
                reply: tx,
            });
            all_receivers.push((seq, rx));
        }
        assert_eq!(dispatcher.dispatch(Batch { requests }), 32);
    }

    match dispatcher.backend() {
        DispatchBackend::Bridge(corr) => assert_eq!(corr.in_flight(), 256),
        _ => panic!("expected bridge backend"),
    }

    let bridge = BatchBridge::get().unwrap();
    let mut polled = 0usize;
    while polled < 256 {
        let snap = bridge
            .poll_batch(Duration::from_millis(100))
            .expect("expected in-process batch snapshot");
        for idx in 0..snap.len() {
            let node = snap.node(idx);
            bridge
                .submit_response(BridgeResponse {
                    request_id: node.correlation_id,
                    status: 200,
                    headers_raw: bytes::Bytes::new(),
                    body: bytes::Bytes::from(format!("done-{}", node.handler_id)),
                })
                .expect("direct response sink submission");
            polled += 1;
        }
    }

    for (seq, rx) in all_receivers {
        let resp = tokio::time::timeout(Duration::from_millis(100), rx)
            .await
            .expect("response oneshot timed out")
            .expect("response oneshot dropped");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_ref(), format!("done-{seq}").as_bytes());
    }

    match dispatcher.backend() {
        DispatchBackend::Bridge(corr) => assert!(corr.is_empty()),
        _ => panic!("expected bridge backend"),
    }
}

#[tokio::test]
async fn shm_dispatcher_handles_oversized_request_and_skips_unhealthy_worker() {
    use vertebra_http::dispatcher::ShmDispatcher;

    let pair_0 = Arc::new(RingPair::create(RingPairConfig::default()).expect("pair 0"));
    let pair_1 = Arc::new(RingPair::create(RingPairConfig::default()).expect("pair 1"));
    let shm = ShmDispatcher::new(vec![Arc::clone(&pair_0), Arc::clone(&pair_1)], 32);
    let dispatcher = Dispatcher::from_shm(Arc::clone(&shm));

    shm.set_worker_healthy(0, false);
    assert!(!shm.is_worker_healthy(0));
    assert!(shm.is_worker_healthy(1));

    for _ in 0..4 {
        let (batch, _rxs) = make_batch(1);
        assert_eq!(dispatcher.dispatch(batch), 1);
    }

    assert!(pair_0.request_ring.try_recv_view().is_err());
    let mut received_on_1 = 0usize;
    while let Ok(view) = pair_1.request_ring.try_recv_view() {
        let snap = BatchSnapshot::from_mpmc_slot(view).expect("snapshot on worker 1");
        received_on_1 += snap.len();
    }
    assert_eq!(received_on_1, 4);

    shm.set_worker_healthy(0, true);
    assert!(shm.is_worker_healthy(0));

    #[cfg(unix)]
    {
        let (tx, _rx) = oneshot::channel();
        let big_body = bytes::Bytes::from(vec![0x5Au8; 256 * 1024]);
        let mut requests = vec![PendingRequest {
            handler_id: 99,
            method_tag: 1,
            http_version_tag: 1,
            flags: 0,
            path_and_query: "/upload".parse().unwrap(),
            headers_mp: bytes::Bytes::new(),
            body: big_body.clone(),
            reply: tx,
        }];
        assert_eq!(dispatcher.dispatch_requests(&mut requests), 1);

        let view = pair_0
            .request_ring
            .try_recv_view()
            .or_else(|_| pair_1.request_ring.try_recv_view())
            .expect("overflow envelope must be queued in request ring");
        let snap = BatchSnapshot::from_mpmc_slot(view).expect("overflow snapshot");
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.body(0).len(), 256 * 1024);
        assert_eq!(snap.body(0), big_body.as_ref());
    }
}
