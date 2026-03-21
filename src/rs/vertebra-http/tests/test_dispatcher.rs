//! Unit tests for the dispatcher.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::oneshot;
use vertebra_core::bridge::BatchBridge;
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
