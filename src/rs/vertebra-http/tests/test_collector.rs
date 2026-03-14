//! Unit tests for the batch collector.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use vertebra_http::{
    collector::{BatchCollector, PendingRequest, PendingResponse},
    dispatcher::Dispatcher,
    policy::DrainPolicy,
};

fn dummy_request(handler_id: u64) -> (PendingRequest, oneshot::Receiver<PendingResponse>) {
    let (tx, rx) = oneshot::channel();
    let req = PendingRequest {
        handler_id,
        method_tag: 0,
        path: "/hello".to_owned(),
        query: None,
        headers_mp: bytes::Bytes::new(),
        body: bytes::Bytes::new(),
        reply: tx,
    };
    (req, rx)
}

#[tokio::test]
async fn test_drain_by_size() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
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

    let batch = tokio::time::timeout(Duration::from_millis(200), ring_rx.recv())
        .await
        .expect("drain did not fire")
        .unwrap();

    assert_eq!(batch.correlation_ids.len(), 4);
}

#[tokio::test]
async fn test_drain_by_timeout() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
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

    let batch = tokio::time::timeout(Duration::from_millis(100), ring_rx.recv())
        .await
        .expect("timeout drain did not fire")
        .unwrap();

    assert_eq!(batch.correlation_ids.len(), 3);
}

#[tokio::test]
async fn test_no_spurious_drain() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 64,
        timeout: Duration::from_millis(5),
    };
    let (collector, _tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    let result = tokio::time::timeout(Duration::from_millis(20), ring_rx.recv()).await;
    assert!(result.is_err(), "no drain should fire with zero requests");
}

#[tokio::test]
async fn test_partial_batch_on_shutdown() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
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

    let batch = tokio::time::timeout(Duration::from_millis(200), ring_rx.recv())
        .await
        .expect("flush on shutdown did not fire")
        .unwrap();

    assert_eq!(batch.correlation_ids.len(), 5);
}
