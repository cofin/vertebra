//! Unit tests for the dispatcher.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use vertebra_http::{
    collector::{Batch, PendingRequest, PendingResponse},
    dispatcher::Dispatcher,
};

fn make_batch(count: usize) -> (Batch, Vec<oneshot::Receiver<PendingResponse>>) {
    let mut requests = Vec::new();
    let mut receivers = Vec::new();
    for i in 0..count as u64 {
        let (tx, rx) = oneshot::channel();
        requests.push(PendingRequest {
            handler_id: i,
            method_tag: 0,
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
async fn test_dispatch_roundtrip() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let (batch, _receivers) = make_batch(2);
    dispatcher.dispatch(batch).await;

    let serialized = tokio::time::timeout(Duration::from_millis(100), ring_rx.recv())
        .await
        .expect("dispatch did not produce ring entry")
        .unwrap();

    assert_eq!(serialized.correlation_ids.len(), 2);
    // Each request produces a 16-byte header + 0-byte body.
    assert_eq!(serialized.payload.len(), 2 * 16);
}

#[tokio::test]
async fn test_complete_unblocks_hyper() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let (batch, mut receivers) = make_batch(1);
    dispatcher.dispatch(batch).await;

    let serialized = ring_rx.recv().await.unwrap();
    let id = serialized.correlation_ids[0];

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
async fn test_back_pressure() {
    // Ring channel of capacity 1.
    let (dispatcher, _ring_rx) = Dispatcher::new(1);
    let dispatcher = Arc::new(dispatcher);

    // First dispatch fills the channel.
    let (batch1, _) = make_batch(1);
    dispatcher.dispatch(batch1).await;

    // Second dispatch should log and drop, NOT panic.
    let (batch2, _) = make_batch(1);
    // Use a timeout so we don't hang if the channel blocks.
    tokio::time::timeout(Duration::from_millis(200), dispatcher.dispatch(batch2))
        .await
        .expect("dispatch should not hang on full channel");
}
