//! Integration-style tests for the dispatcher across all three modes.

use std::sync::Arc;

use tokio::sync::oneshot;

use super::{Dispatcher, ShmDispatcher};
use crate::collector::{Batch, PendingRequest, PendingResponse};

#[test]
fn dispatcher_creates_with_empty_state() {
    let d = Dispatcher::new();
    assert!(d.inner.pending.is_empty());
}

#[test]
fn complete_with_no_pending_is_noop() {
    let d = Dispatcher::new();
    d.complete(
        999,
        PendingResponse {
            status: 200,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::from_static(b"ok"),
        },
    );
}

#[tokio::test]
async fn direct_mode_sends_batch() {
    let (d, mut rx) = Dispatcher::new_direct(16);
    let (reply_tx, _reply_rx) = oneshot::channel();

    let batch = Batch {
        requests: vec![PendingRequest {
            handler_id: 1,
            method_tag: 0,
            http_version_tag: 1,
            flags: 0,
            path: "/test".to_owned(),
            query: None,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            reply: reply_tx,
        }],
    };

    d.dispatch(batch);

    let direct_batch = rx.recv().await.expect("should receive direct batch");
    assert_eq!(direct_batch.requests.len(), 1);
    assert_eq!(direct_batch.requests[0].path, "/test");
    assert!(direct_batch.requests[0].reply.is_some());
}

#[tokio::test]
async fn direct_mode_fnf_has_no_reply() {
    let (d, mut rx) = Dispatcher::new_direct(16);
    let (reply_tx, _reply_rx) = oneshot::channel();

    let batch = Batch {
        requests: vec![PendingRequest {
            handler_id: 1,
            method_tag: 0,
            http_version_tag: 1,
            flags: vertebra_core::protocol::flags::FIRE_AND_FORGET,
            path: "/fire".to_owned(),
            query: None,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            reply: reply_tx,
        }],
    };

    d.dispatch(batch);

    let direct_batch = rx.recv().await.expect("should receive direct batch");
    assert_eq!(direct_batch.requests.len(), 1);
    assert!(
        direct_batch.requests[0].reply.is_none(),
        "F&F request should have no reply channel"
    );
}

#[tokio::test]
async fn shm_dispatcher_round_robin() {
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pairs: Vec<Arc<RingPair>> = (0..4)
        .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
        .collect();

    let dispatcher = Dispatcher::new_shm(pairs.clone(), 1024);

    // Dispatch 8 requests — should round-robin across 4 workers
    for i in 0..8u64 {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let batch = Batch {
            requests: vec![PendingRequest {
                handler_id: i,
                method_tag: 0,
                http_version_tag: 1,
                flags: 0,
                path: format!("/test/{i}"),
                query: None,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::new(),
                reply: reply_tx,
            }],
        };
        dispatcher.dispatch(batch);
    }

    // Each worker ring should have 2 requests
    let mut total = 0;
    for pair in &pairs {
        let mut buf = vec![0u8; pair.request_ring.payload_capacity()];
        let mut count = 0;
        while pair.request_ring.try_recv(&mut buf).is_ok() {
            count += 1;
        }
        total += count;
        assert!(
            (1..=3).contains(&count),
            "worker got {count} requests, expected ~2"
        );
    }
    assert_eq!(total, 8);
    let _ = ShmDispatcher::shard_count; // anchor the re-export so it's kept
}

#[tokio::test]
async fn shm_dispatcher_complete_through_correlation() {
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pair = Arc::new(RingPair::create(RingPairConfig::default()).unwrap());
    let dispatcher = Dispatcher::new_shm(vec![pair.clone()], 1024);

    let (reply_tx, mut reply_rx) = oneshot::channel();
    let batch = Batch {
        requests: vec![PendingRequest {
            handler_id: 1,
            method_tag: 0,
            http_version_tag: 1,
            flags: 0,
            path: "/hello".to_owned(),
            query: None,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            reply: reply_tx,
        }],
    };

    dispatcher.dispatch(batch);

    // Read the request from the ring to get the correlation ID
    let mut buf = vec![0u8; pair.request_ring.payload_capacity()];
    let len = pair.request_ring.try_recv(&mut buf).expect("should recv");
    let snap = vertebra_core::transport::BatchSnapshot::deserialize_from(&buf[..len])
        .expect("deserialize");
    let correlation_id = snap.node(0).correlation_id;
    assert!(correlation_id > 0);

    // Complete the correlation
    dispatcher.complete(
        correlation_id,
        PendingResponse {
            status: 200,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::from_static(b"world"),
        },
    );

    // The oneshot should have the response
    let resp = reply_rx.try_recv().expect("should receive response");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body.as_ref(), b"world");
}
