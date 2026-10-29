//! Integration-style tests for the dispatcher across all three modes.

use std::sync::Arc;

use http::uri::PathAndQuery;
use tokio::sync::oneshot;

use super::{DispatchBackend, Dispatcher, ShmDispatcher};
use crate::batcher::{Batch, PendingRequest};
use crate::body::PendingResponse;

#[test]
fn dispatcher_creates_with_empty_state() {
    let d = Dispatcher::new();
    match d.backend() {
        DispatchBackend::Bridge(b) => {
            assert!(b.is_empty());
            assert_eq!(b.in_flight(), 0);
        }
        _ => panic!("expected bridge backend"),
    }
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
            stream_rx: None,
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
            path_and_query: PathAndQuery::from_static("/test"),
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            reply: reply_tx,
        }],
    };

    d.dispatch(batch);

    let direct_batch = rx.recv().await.expect("should receive direct batch");
    assert_eq!(direct_batch.requests.len(), 1);
    assert_eq!(direct_batch.requests[0].path(), "/test");
    assert!(!direct_batch.requests[0].reply.is_closed());
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
            path_and_query: PathAndQuery::from_static("/fire"),
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            reply: reply_tx,
        }],
    };

    d.dispatch(batch);

    let direct_batch = rx.recv().await.expect("should receive direct batch");
    assert_eq!(direct_batch.requests.len(), 1);
    assert_ne!(
        direct_batch.requests[0].flags & vertebra_core::protocol::flags::FIRE_AND_FORGET,
        0,
        "F&F request should have FIRE_AND_FORGET flag set"
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
        let path_str = format!("/test/{i}");
        let batch = Batch {
            requests: vec![PendingRequest {
                handler_id: i,
                method_tag: 0,
                http_version_tag: 1,
                flags: 0,
                path_and_query: path_str.parse().unwrap(),
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
            path_and_query: PathAndQuery::from_static("/hello"),
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
            stream_rx: None,
        },
    );

    // The oneshot should have the response
    let resp = reply_rx.try_recv().expect("should receive response");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body.as_ref(), b"world");
}

#[tokio::test]
async fn shm_dispatcher_warm_worker_affinity_whole_batch_and_dekker_wake() {
    use vertebra_core::transport::BatchSnapshot;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pairs: Vec<Arc<RingPair>> = (0..4)
        .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
        .collect();
    let dispatcher = Dispatcher::new_shm(pairs.clone(), 1024);

    let mut reqs = Vec::new();
    let mut rxs = Vec::new();
    for i in 0..4u64 {
        let (tx, rx) = oneshot::channel();
        reqs.push(PendingRequest {
            handler_id: i,
            method_tag: 0,
            http_version_tag: 1,
            flags: 0,
            path_and_query: PathAndQuery::from_static("/batch"),
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::from_static(b"payload"),
            reply: tx,
        });
        rxs.push(rx);
    }

    assert_eq!(dispatcher.dispatch(Batch { requests: reqs }), 4);

    assert!(
        !pairs[0].request_notifier.wait(0).unwrap(),
        "awake worker must not receive redundant eventfd write"
    );

    let mut buf = vec![0u8; pairs[0].request_ring.payload_capacity()];
    let len = pairs[0]
        .request_ring
        .try_recv(&mut buf)
        .expect("whole batch should route to worker 0");
    let snap = BatchSnapshot::deserialize_from(&buf[..len]).unwrap();
    assert_eq!(snap.len(), 4);
    for other in &pairs[1..] {
        assert!(
            other.request_ring.try_recv(&mut buf).is_err(),
            "whole batch must not scatter across other workers"
        );
    }

    let completions: Vec<(u64, PendingResponse)> = (0..snap.len())
        .map(|idx| {
            (
                snap.node(idx).correlation_id,
                PendingResponse {
                    status: 200,
                    headers_mp: bytes::Bytes::new(),
                    body: bytes::Bytes::from_static(b"ok"),
                    stream_rx: None,
                },
            )
        })
        .collect();
    dispatcher.complete_many(completions);
    for mut rx in rxs {
        assert_eq!(rx.try_recv().unwrap().status, 200);
    }

    assert!(pairs[0].prepare_request_sleep());
    let (tx, _rx) = oneshot::channel();
    dispatcher.dispatch(Batch {
        requests: vec![PendingRequest {
            handler_id: 99,
            method_tag: 0,
            http_version_tag: 1,
            flags: 0,
            path_and_query: PathAndQuery::from_static("/warm"),
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            reply: tx,
        }],
    });
    assert!(
        pairs[0].request_notifier.wait(50).unwrap(),
        "sleeping warm worker 0 must receive Dekker wakeup and sticky sequential request"
    );
    pairs[0].cancel_request_sleep();
    let _ = pairs[0].request_notifier.clear();
    assert!(pairs[0].request_ring.try_recv(&mut buf).is_ok());
}

#[test]
fn mount_route_registration_includes_options_and_head() {
    use crate::routing::{RouteTable, Router};

    let mut table = RouteTable::new();
    table.insert_mount_prefix("/api");
    table.insert_mount_prefix("/");
    let router = Router::from_entries(table.into_entries()).expect("compile router");

    for method in &["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"] {
        assert!(
            router.lookup(method, "/api/v1/resource").is_ok(),
            "mounted /api prefix must match {method}"
        );
        assert!(
            router.lookup(method, "/root-check").is_ok(),
            "mounted / prefix must match {method}"
        );
    }
}

#[tokio::test]
async fn shm_dispatcher_shard_sticky_affinity_and_least_loaded_spillover() {
    use vertebra_core::transport::BatchSnapshot;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pairs: Vec<Arc<RingPair>> = (0..4)
        .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
        .collect();
    let shm = ShmDispatcher::new(pairs.clone(), 1024);
    let dispatcher = Dispatcher::from_shm(Arc::clone(&shm));

    let make_req = |id: u64| {
        let (tx, rx) = oneshot::channel();
        (
            PendingRequest {
                handler_id: id,
                method_tag: 0,
                http_version_tag: 1,
                flags: 0,
                path_and_query: PathAndQuery::from_static("/sticky"),
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::new(),
                reply: tx,
            },
            rx,
        )
    };

    // First batch when idle (`prev_active == 0`) routes to warm worker 0.
    let (r0, _rx0) = make_req(1);
    let mut batch0 = vec![r0];
    assert_eq!(
        dispatcher.dispatch_requests_from_shard(Some(0), &mut batch0),
        1
    );
    assert_eq!(shm.worker_in_flight(0), 1);

    // Subsequent concurrent batches from shards 1, 2, 3 route to their sticky home workers 1, 2, 3.
    for shard in 1..4usize {
        let (req, _rx) = make_req(shard as u64 + 10);
        let mut reqs = vec![req];
        assert_eq!(
            dispatcher.dispatch_requests_from_shard(Some(shard), &mut reqs),
            1
        );
        assert_eq!(shm.worker_in_flight(shard), 1);
    }

    // If worker 2 is marked unhealthy, shard 2 spills over to a healthy least-loaded worker.
    shm.set_worker_healthy(2, false);
    let (spill_req, _spill_rx) = make_req(99);
    let mut spill_batch = vec![spill_req];
    assert_eq!(
        dispatcher.dispatch_requests_from_shard(Some(2), &mut spill_batch),
        1
    );
    assert_eq!(shm.worker_in_flight(2), 1);
    assert_eq!(shm.worker_in_flight(3), 2);
    shm.set_worker_healthy(2, true);

    // Drain and complete all correlations and verify per-worker in-flight returns to 0.
    for pair in &pairs {
        while let Ok(view) = pair.request_ring.try_recv_view() {
            let snap = BatchSnapshot::from_mpmc_slot(view).unwrap();
            let completions: Vec<(u64, PendingResponse)> = (0..snap.len())
                .map(|i| {
                    (
                        snap.node(i).correlation_id,
                        PendingResponse {
                            status: 200,
                            headers_mp: bytes::Bytes::new(),
                            body: bytes::Bytes::from_static(b"ok"),
                            stream_rx: None,
                        },
                    )
                })
                .collect();
            dispatcher.complete_many(completions);
        }
    }

    for worker_id in 0..4 {
        assert_eq!(shm.worker_in_flight(worker_id), 0);
    }
}
