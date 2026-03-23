//! Unit tests for the dispatcher.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::oneshot;
use vertebra_core::bridge::BatchBridge;
use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
use vertebra_core::transport::BatchSnapshot;
use vertebra_http::{
    collector::{Batch, PendingRequest, PendingResponse},
    dispatcher::Dispatcher,
};

/// Serialize dispatcher tests to prevent global bridge interference.
static TEST_LOCK: std::sync::LazyLock<StdMutex<()>> =
    std::sync::LazyLock::new(|| StdMutex::new(()));

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
            path: "/".to_owned(),
            query: None,
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
