//! Integration-style tests for the dispatcher across all three modes.

use std::sync::Arc;

use http::uri::PathAndQuery;
use tokio::sync::oneshot;

use super::{BridgeCorrelation, DispatchBackend, Dispatcher, ShmDispatcher, ShmTargetError};
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

fn make_pending_request(
    handler_id: u64,
    path: &'static str,
    body: bytes::Bytes,
) -> (PendingRequest, oneshot::Receiver<PendingResponse>) {
    let (tx, rx) = oneshot::channel();
    (
        PendingRequest {
            handler_id,
            method_tag: 0,
            http_version_tag: 1,
            flags: 0,
            path_and_query: PathAndQuery::from_static(path),
            headers_mp: bytes::Bytes::new(),
            body,
            reply: tx,
        },
        rx,
    )
}

#[test]
fn shm_worker_count_and_healthy_worker_ids() {
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pairs: Vec<Arc<RingPair>> = (0..3)
        .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
        .collect();
    let shm = ShmDispatcher::new(pairs, 64);

    assert_eq!(shm.worker_count(), 3);
    assert_eq!(shm.healthy_worker_ids(), vec![0, 1, 2]);

    shm.set_worker_healthy(1, false);
    assert_eq!(shm.worker_count(), 3);
    assert_eq!(shm.healthy_worker_ids(), vec![0, 2]);
}

#[tokio::test]
async fn shm_dispatch_to_worker_delivers_to_exact_ring_and_completes() {
    use vertebra_core::transport::BatchSnapshot;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pairs: Vec<Arc<RingPair>> = (0..3)
        .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
        .collect();
    let shm = ShmDispatcher::new(pairs.clone(), 64);

    let (req, rx) = make_pending_request(42, "/taskz?format=json", bytes::Bytes::new());
    let cid = shm
        .dispatch_to_worker(2, req)
        .expect("targeted dispatch to healthy worker 2 should succeed");

    assert_eq!(shm.in_flight(), 1);
    assert_eq!(shm.worker_in_flight(0), 0);
    assert_eq!(shm.worker_in_flight(1), 0);
    assert_eq!(shm.worker_in_flight(2), 1);

    assert!(pairs[0].request_ring.try_recv_view().is_err());
    assert!(pairs[1].request_ring.try_recv_view().is_err());

    let view = pairs[2]
        .request_ring
        .try_recv_view()
        .expect("worker 2 ring should contain the dispatched request");
    let snap = BatchSnapshot::from_mpmc_slot(view).unwrap();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap.node(0).handler_id, 42);
    assert_eq!(snap.node(0).correlation_id, cid);
    assert_eq!(snap.path(0), "/taskz");
    assert_eq!(snap.query(0), Some("format=json"));

    assert!(shm.complete_one(
        cid,
        PendingResponse {
            status: 200,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::from_static(b"worker-2"),
            stream_rx: None,
        },
    ));
    let resp = rx.await.expect("reply channel should receive response");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, bytes::Bytes::from_static(b"worker-2"));
    assert_eq!(shm.in_flight(), 0);
    assert_eq!(shm.worker_in_flight(2), 0);
}

#[test]
fn shm_dispatch_to_worker_wakes_sleeping_worker() {
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pairs: Vec<Arc<RingPair>> = (0..3)
        .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
        .collect();
    let shm = ShmDispatcher::new(pairs.clone(), 64);

    assert!(pairs[1].prepare_request_sleep());

    let (req, _rx) = make_pending_request(7, "/taskz", bytes::Bytes::new());
    let cid = shm
        .dispatch_to_worker(1, req)
        .expect("dispatch to sleeping worker should succeed");

    assert!(pairs[1].request_notifier.wait(50).unwrap());
    assert!(shm.cancel_correlation(cid));
    assert_eq!(shm.in_flight(), 0);
    assert_eq!(shm.worker_in_flight(1), 0);
}

#[test]
fn shm_dispatch_to_worker_error_paths_preserve_state() {
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let config = RingPairConfig {
        request_slots: 2,
        request_slot_size: 256,
        ..RingPairConfig::default()
    };
    let pairs: Vec<Arc<RingPair>> = (0..3)
        .map(|_| Arc::new(RingPair::create(config.clone()).unwrap()))
        .collect();
    let shm = ShmDispatcher::new(pairs.clone(), 64);

    // Out-of-range worker ID.
    let (req, _rx) = make_pending_request(1, "/taskz", bytes::Bytes::new());
    assert_eq!(
        shm.dispatch_to_worker(3, req),
        Err(ShmTargetError::NotFound)
    );
    assert_eq!(shm.in_flight(), 0);

    // Unhealthy worker rejected without spillover.
    shm.set_worker_healthy(1, false);
    let (req, _rx) = make_pending_request(2, "/taskz", bytes::Bytes::new());
    assert_eq!(
        shm.dispatch_to_worker(1, req),
        Err(ShmTargetError::Unhealthy)
    );
    assert_eq!(shm.in_flight(), 0);
    for pair in &pairs {
        assert!(pair.request_ring.try_recv_view().is_err());
    }
    shm.set_worker_healthy(1, true);

    // Oversized payload rejected before allocating correlation.
    let (req, _rx) = make_pending_request(3, "/taskz", bytes::Bytes::from(vec![b'x'; 512]));
    assert_eq!(
        shm.dispatch_to_worker(0, req),
        Err(ShmTargetError::SendFailed)
    );
    assert_eq!(shm.in_flight(), 0);
    assert_eq!(shm.worker_in_flight(0), 0);

    // Fill worker 0 ring to capacity, then confirm RingFull rolls back cleanly.
    let mut filled_cids = Vec::new();
    loop {
        let (req, _rx) = make_pending_request(4, "/taskz", bytes::Bytes::new());
        match shm.dispatch_to_worker(0, req) {
            Ok(cid) => filled_cids.push(cid),
            Err(ShmTargetError::RingFull) => break,
            Err(other) => panic!("unexpected error while filling ring: {other:?}"),
        }
    }
    let expected_count = filled_cids.len();
    assert!(expected_count >= 1);
    assert_eq!(shm.in_flight(), expected_count);
    assert_eq!(shm.worker_in_flight(0), expected_count);

    let (overflow_req, _overflow_rx) = make_pending_request(5, "/taskz", bytes::Bytes::new());
    assert_eq!(
        shm.dispatch_to_worker(0, overflow_req),
        Err(ShmTargetError::RingFull)
    );
    assert_eq!(shm.in_flight(), expected_count);
    assert_eq!(shm.worker_in_flight(0), expected_count);

    for cid in filled_cids {
        assert!(shm.cancel_correlation(cid));
    }
    assert_eq!(shm.in_flight(), 0);
    assert_eq!(shm.worker_in_flight(0), 0);
}

#[test]
fn shm_dispatch_to_worker_correlation_full_returns_error() {
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pairs: Vec<Arc<RingPair>> = vec![Arc::new(
        RingPair::create(RingPairConfig::default()).unwrap(),
    )];
    let shm = ShmDispatcher::new(pairs, 8);

    let mut cids = Vec::new();
    for i in 0..8 {
        let (req, _rx) = make_pending_request(i, "/taskz", bytes::Bytes::new());
        cids.push(
            shm.dispatch_to_worker(0, req)
                .expect("initial 8 correlations should fit across 8 shards"),
        );
    }

    let (extra_req, _extra_rx) = make_pending_request(99, "/taskz", bytes::Bytes::new());
    assert_eq!(
        shm.dispatch_to_worker(0, extra_req),
        Err(ShmTargetError::CorrelationFull)
    );
    assert_eq!(shm.in_flight(), 8);
    assert_eq!(shm.worker_in_flight(0), 8);

    for cid in cids {
        assert!(shm.cancel_correlation(cid));
    }
    assert_eq!(shm.in_flight(), 0);
    assert_eq!(shm.worker_in_flight(0), 0);
}

#[tokio::test]
async fn immediate_correlation_cancellation_shm_and_bridge() {
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    let pairs: Vec<Arc<RingPair>> = (0..2)
        .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
        .collect();
    let shm = ShmDispatcher::new(pairs, 64);

    let (req, rx) = make_pending_request(11, "/taskz", bytes::Bytes::new());
    let cid = shm
        .dispatch_to_worker(1, req)
        .expect("dispatch_to_worker should succeed");
    assert_eq!(shm.in_flight(), 1);
    assert_eq!(shm.worker_in_flight(1), 1);

    assert!(shm.cancel_correlation(cid));
    assert_eq!(shm.in_flight(), 0);
    assert_eq!(shm.worker_in_flight(1), 0);
    assert!(!shm.cancel_correlation(cid));
    assert!(rx.await.is_err());

    let bridge = BridgeCorrelation::with_capacity(64);
    let (b_tx, b_rx) = oneshot::channel();
    let b_cid = bridge
        .register_pending(b_tx)
        .expect("register_pending should succeed");
    assert_eq!(bridge.in_flight(), 1);

    assert!(bridge.cancel_correlation(b_cid));
    assert_eq!(bridge.in_flight(), 0);
    assert!(!bridge.cancel_correlation(b_cid));
    assert!(b_rx.await.is_err());
}
