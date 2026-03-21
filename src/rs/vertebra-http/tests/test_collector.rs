#![allow(clippy::await_holding_lock)]
//! Unit tests for the batch collector.
//!
//! The collector drains through `dispatch_to_bridge()`, which uses the global
//! `BatchBridge` singleton. Because all tests in a binary share that global,
//! we serialize access with a static `Mutex` to prevent cross-test interference.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::oneshot;

use vertebra_core::bridge::BatchBridge;
use vertebra_http::{
    collector::{BatchCollector, PendingRequest, PendingResponse},
    dispatcher::Dispatcher,
    policy::DrainPolicy,
};

/// Serialize collector tests to prevent cross-test bridge interference.
static TEST_LOCK: std::sync::LazyLock<StdMutex<()>> =
    std::sync::LazyLock::new(|| StdMutex::new(()));

/// Ensure the global bridge is initialized (idempotent across tests).
fn ensure_bridge() {
    let _ = BatchBridge::init(256, 1_048_576, 4, 1);
}

/// Drain any stale batches from the bridge.
fn drain_stale_batches() {
    if let Some(bridge) = BatchBridge::get() {
        while bridge.poll_batch(Duration::from_millis(1)).is_some() {}
    }
}

fn dummy_request(handler_id: u64) -> (PendingRequest, oneshot::Receiver<PendingResponse>) {
    let (tx, rx) = oneshot::channel();
    let req = PendingRequest {
        handler_id,
        method_tag: 0,
        http_version_tag: 1,
        path: "/hello".to_owned(),
        query: None,
        headers_mp: bytes::Bytes::new(),
        body: bytes::Bytes::new(),
        reply: tx,
    };
    (req, rx)
}

/// Collect all batches from the bridge until a timeout, returning the total
/// number of requests received.
fn collect_all_requests(timeout: Duration) -> usize {
    let bridge = BatchBridge::get().unwrap();
    let mut total = 0;
    let start = std::time::Instant::now();
    loop {
        let remaining = timeout.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            break;
        }
        match bridge.poll_batch(remaining) {
            Some(batch) => total += batch.len(),
            None => break,
        }
    }
    total
}

#[tokio::test]
async fn test_drain_by_size() {
    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale_batches();

    let dispatcher = Dispatcher::new();
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 4,
        timeout: Duration::from_secs(60),
    };
    let (collector, tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    for i in 0..4u64 {
        let (req, _rx) = dummy_request(i);
        tx.send(req).await.unwrap();
    }

    let total = tokio::task::spawn_blocking(|| collect_all_requests(Duration::from_millis(500)))
        .await
        .unwrap();

    assert_eq!(total, 4);
}

#[tokio::test]
async fn test_drain_by_timeout() {
    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale_batches();

    let dispatcher = Dispatcher::new();
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 64,
        timeout: Duration::from_millis(5),
    };
    let (collector, tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    for i in 0..3u64 {
        let (req, _rx) = dummy_request(i);
        tx.send(req).await.unwrap();
    }

    let total = tokio::task::spawn_blocking(|| collect_all_requests(Duration::from_millis(500)))
        .await
        .unwrap();

    assert_eq!(total, 3);
}

#[tokio::test]
async fn test_no_spurious_drain() {
    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale_batches();

    let dispatcher = Dispatcher::new();
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 64,
        timeout: Duration::from_millis(5),
    };
    let (collector, _tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    let total = tokio::task::spawn_blocking(|| collect_all_requests(Duration::from_millis(30)))
        .await
        .unwrap();

    assert_eq!(total, 0, "no drain should fire with zero requests");
}

#[tokio::test]
async fn test_partial_batch_on_shutdown() {
    let _guard = TEST_LOCK.lock().unwrap();
    ensure_bridge();
    drain_stale_batches();

    let dispatcher = Dispatcher::new();
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 64,
        timeout: Duration::from_secs(60),
    };
    let (collector, tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    for i in 0..5u64 {
        let (req, _rx) = dummy_request(i);
        tx.send(req).await.unwrap();
    }

    // Close the sender; collector should flush remaining.
    drop(tx);

    let total = tokio::task::spawn_blocking(|| collect_all_requests(Duration::from_millis(500)))
        .await
        .unwrap();

    assert_eq!(total, 5);
}
