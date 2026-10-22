use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use vertebra_http::pubsub::{PubSubBroker, ShmBroadcastRing};
use vertebra_http::sse::SseHub;

#[tokio::test]
async fn pubsub_broker_fans_out_to_websocket_and_sse_subscribers_in_o1() {
    let sse_hub = Arc::new(SseHub::new(32, Duration::from_secs(30)));
    let broker = PubSubBroker::new(1, 32, Some(Arc::clone(&sse_hub)), None);

    let mut raw_rx1 = broker.subscribe_raw("orders");
    let mut raw_rx2 = broker.subscribe_raw("orders");
    let mut pattern_rx = broker.subscribe_pattern("orders.*");
    let sse_resp = sse_hub.subscribe_response("orders", None);

    let event_id = broker
        .pubsub_publish("orders", Bytes::from_static(b"{\"order_id\":101}"), None)
        .expect("published event id");
    assert_eq!(event_id, 1);

    assert_eq!(
        raw_rx1.recv().await.unwrap(),
        Bytes::from_static(b"{\"order_id\":101}")
    );
    assert_eq!(
        raw_rx2.recv().await.unwrap(),
        Bytes::from_static(b"{\"order_id\":101}")
    );

    let mut body = sse_resp.into_body();
    let sse_frame = body
        .frame()
        .await
        .expect("sse frame")
        .expect("infallible")
        .into_data()
        .expect("data");
    assert_eq!(
        sse_frame,
        Bytes::from_static(b"id: 1\r\nevent: orders\r\ndata: {\"order_id\":101}\r\n\r\n")
    );

    broker
        .pubsub_publish(
            "orders.created",
            Bytes::from_static(b"{\"order_id\":102}"),
            None,
        )
        .expect("wildcard event id");
    assert_eq!(
        pattern_rx.recv().await.unwrap(),
        Bytes::from_static(b"{\"order_id\":102}")
    );
}

#[test]
fn pubsub_broker_replays_missed_events_after_last_event_id() {
    let broker = PubSubBroker::new(1, 8, None, None);

    for i in 1..=10_u64 {
        broker.pubsub_publish("metrics", Bytes::from(format!("val-{i}")), None);
    }

    let (replayed, _rx) = broker.subscribe_with_replay("metrics", Some(5));
    let ids: Vec<u64> = replayed.iter().map(|env| env.event_id).collect();
    assert_eq!(ids, vec![6, 7, 8, 9, 10]);
}

#[test]
fn pubsub_broker_deduplicates_1024_idempotency_keys() {
    let broker = PubSubBroker::new(1, 32, None, None);

    let first = broker.pubsub_publish(
        "payments",
        Bytes::from_static(b"charge-1"),
        Some("idem-alpha"),
    );
    assert_eq!(first, Some(1));

    let duplicate = broker.pubsub_publish(
        "payments",
        Bytes::from_static(b"charge-1"),
        Some("idem-alpha"),
    );
    assert_eq!(duplicate, None);

    for idx in 0..1024 {
        let key = format!("idem-fill-{idx}");
        assert!(
            broker
                .pubsub_publish("payments", Bytes::from_static(b"x"), Some(&key))
                .is_some()
        );
    }

    let after_eviction = broker.pubsub_publish(
        "payments",
        Bytes::from_static(b"charge-1-retry"),
        Some("idem-alpha"),
    );
    assert!(after_eviction.is_some());
}

#[tokio::test]
async fn pubsub_broker_forwards_across_workers_via_shm_broadcast_ring() {
    let shm_ring = Arc::new(ShmBroadcastRing::new(64, 4096));
    let sse_worker2 = Arc::new(SseHub::new(16, Duration::from_secs(30)));

    let worker1 = PubSubBroker::new(1, 32, None, Some(Arc::clone(&shm_ring)));
    let worker2 = PubSubBroker::new(
        2,
        32,
        Some(Arc::clone(&sse_worker2)),
        Some(Arc::clone(&shm_ring)),
    );

    let mut worker2_ws_rx = worker2.subscribe_raw("cluster.events");
    let mut worker2_cursor = 0_u64;

    worker1.pubsub_publish(
        "cluster.events",
        Bytes::from_static(b"{\"node\":\"w1\"}"),
        Some("cluster-key-1"),
    );

    let drained = worker2.poll_shm_ring(&mut worker2_cursor);
    assert_eq!(drained, 1);

    let received = worker2_ws_rx.recv().await.unwrap();
    assert_eq!(received, Bytes::from_static(b"{\"node\":\"w1\"}"));

    let mut worker1_cursor = 0_u64;
    let self_drained = worker1.poll_shm_ring(&mut worker1_cursor);
    assert_eq!(self_drained, 0);
}
