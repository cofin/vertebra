use std::convert::Infallible;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use hyper::body::Frame;
use tokio::sync::mpsc;

use vertebra_http::body::{
    FrameReceiver, HttpResponse, PendingResponse, ResponseBody, full_body, stream_body,
};
use vertebra_http::dispatcher::shm_reader::{
    FLAG_ERROR, FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING, FLAG_TRAILERS,
};
use vertebra_http::response::build_response;

#[tokio::test]
async fn unary_full_body_yields_single_frame_without_channel() {
    let body: ResponseBody = full_body(Bytes::from_static(b"unary-fast-path"));
    let collected = body.collect().await.expect("collect unary body");
    assert_eq!(collected.to_bytes(), Bytes::from_static(b"unary-fast-path"));
}

#[tokio::test]
async fn stream_body_delivers_progressive_chunks_and_trailers() {
    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(8);

    let producer = tokio::spawn(async move {
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-1:"))))
            .await
            .expect("chunk 1");
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-2:"))))
            .await
            .expect("chunk 2");
        tx.send(Ok(Frame::data(Bytes::from_static(b"chunk-3"))))
            .await
            .expect("chunk 3");

        let mut trailers = HeaderMap::new();
        trailers.insert(
            HeaderName::from_static("grpc-status"),
            HeaderValue::from_static("0"),
        );
        tx.send(Ok(Frame::trailers(trailers)))
            .await
            .expect("trailers frame");
    });

    let resp: HttpResponse = http::Response::builder()
        .status(StatusCode::OK)
        .body(stream_body(rx))
        .expect("build streaming response");

    let collected = resp
        .into_body()
        .collect()
        .await
        .expect("collect streaming body");
    producer.await.expect("producer task");

    let trailers = collected.trailers().expect("trailers present");
    assert_eq!(
        trailers.get("grpc-status").and_then(|v| v.to_str().ok()),
        Some("0")
    );
    assert_eq!(
        collected.to_bytes(),
        Bytes::from_static(b"chunk-1:chunk-2:chunk-3")
    );
}

#[tokio::test]
async fn build_response_constructs_streaming_variant_from_pending_response() {
    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(4);
    tx.send(Ok(Frame::data(Bytes::from_static(b"part-a"))))
        .await
        .unwrap();
    tx.send(Ok(Frame::data(Bytes::from_static(b"part-b"))))
        .await
        .unwrap();
    drop(tx);

    let pending = PendingResponse {
        status: 200,
        headers_mp: Bytes::new(),
        body: Bytes::new(),
        stream_rx: Some(FrameReceiver::new(rx)),
    };
    let response = build_response(pending);
    assert_eq!(response.status(), StatusCode::OK);
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(collected.to_bytes(), Bytes::from_static(b"part-apart-b"));
}

fn stream_frame(
    id: u64,
    flags: u16,
    body: &'static [u8],
) -> vertebra_core::bridge::ResponseStreamFrame {
    vertebra_core::bridge::ResponseStreamFrame {
        request_id: id,
        status: 200,
        flags: FLAG_STREAMING | flags,
        headers_raw: Bytes::new(),
        body: Bytes::from_static(body),
    }
}

#[tokio::test]
async fn registry_delivers_progressive_data_trailers_and_final() {
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::{BridgeCorrelation, ResponseStreamRegistry};
    let correlation = BridgeCorrelation::default();
    let registry = ResponseStreamRegistry::default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let id = correlation.register_pending(tx).unwrap();
    let mut first = stream_frame(id, FLAG_HEADERS_SENT, b"data: 1\n\n");
    first.headers_raw = vertebra_http::response::encode_response_headers(&[(
        b"content-type".to_vec(),
        b"text/event-stream".to_vec(),
    )]);
    assert_eq!(
        registry
            .deliver(first, |id, response| correlation.complete(id, response))
            .await,
        StreamDelivery::Accepted
    );
    let pending = rx.await.unwrap();
    assert!(pending.stream_rx.is_some());
    assert_eq!(correlation.in_flight(), 0);
    assert_eq!(correlation.sweep_expired(std::time::Duration::ZERO), 0);
    assert_eq!(
        registry
            .deliver(stream_frame(id, 0, b"data: 2\n\n"), |_, _| panic!(
                "continuation must not complete again"
            ))
            .await,
        StreamDelivery::Accepted
    );
    let mut last = stream_frame(id, FLAG_FINAL | FLAG_TRAILERS, b"");
    last.headers_raw = vertebra_http::response::encode_response_headers(&[
        (b"grpc-status".to_vec(), b"0".to_vec()),
        (b"invalid header".to_vec(), b"ignored".to_vec()),
    ]);
    assert_eq!(
        registry.deliver(last, |_, _| false).await,
        StreamDelivery::Accepted
    );
    let response = build_response(pending);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let body = response.into_body().collect().await.unwrap();
    assert_eq!(body.trailers().unwrap()["grpc-status"], "0");
    assert_eq!(
        body.to_bytes(),
        Bytes::from_static(b"data: 1\n\ndata: 2\n\n")
    );
    assert_eq!(registry.active_streams(), 0);
}

#[tokio::test]
async fn registry_terminal_first_is_unary_and_missing_clients_are_closed() {
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::ResponseStreamRegistry;
    let registry = ResponseStreamRegistry::default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    assert_eq!(
        registry
            .deliver(
                stream_frame(1, FLAG_HEADERS_SENT | FLAG_FINAL, b"x"),
                |_, response| tx.send(response).is_ok()
            )
            .await,
        StreamDelivery::Accepted
    );
    let response = rx.await.unwrap();
    assert!(response.stream_rx.is_none());
    assert_eq!(response.body, Bytes::from_static(b"x"));
    assert_eq!(registry.active_streams(), 0);
    assert_eq!(
        registry
            .deliver(stream_frame(999, 0, b"x"), |_, _| false)
            .await,
        StreamDelivery::Closed
    );
    assert_eq!(
        registry
            .deliver(stream_frame(2, FLAG_HEADERS_SENT, b"x"), |_, _| false)
            .await,
        StreamDelivery::Closed
    );
    assert_eq!(registry.active_streams(), 0);
}

#[tokio::test]
async fn registry_disconnect_and_error_finish_the_stream() {
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::ResponseStreamRegistry;
    let registry = ResponseStreamRegistry::default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    assert_eq!(
        registry
            .deliver(stream_frame(1, FLAG_HEADERS_SENT, b"x"), |_, response| tx
                .send(response)
                .is_ok())
            .await,
        StreamDelivery::Accepted
    );
    drop(rx.await.unwrap());
    assert_eq!(
        registry
            .deliver(stream_frame(1, 0, b"y"), |_, _| false)
            .await,
        StreamDelivery::Closed
    );
    assert_eq!(registry.active_streams(), 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    registry
        .deliver(
            stream_frame(2, FLAG_HEADERS_SENT, b"before"),
            |_, response| tx.send(response).is_ok(),
        )
        .await;
    assert_eq!(
        registry
            .deliver(stream_frame(2, FLAG_ERROR, b"discard"), |_, _| false)
            .await,
        StreamDelivery::Accepted
    );
    assert_eq!(
        build_response(rx.await.unwrap())
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes(),
        Bytes::from_static(b"before")
    );
    assert_eq!(registry.active_streams(), 0);
}

#[tokio::test]
async fn registry_blocking_delivery_backpressures_and_guards_runtime() {
    use std::sync::Arc;
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::ResponseStreamRegistry;
    let registry = Arc::new(ResponseStreamRegistry::with_capacity(1));
    assert_eq!(
        registry.deliver_blocking(stream_frame(0, FLAG_HEADERS_SENT, b"bad"), |_, _| panic!(
            "runtime guard"
        )),
        StreamDelivery::Closed
    );
    assert_eq!(registry.active_streams(), 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    let producer_registry = registry.clone();
    let producer = std::thread::spawn(move || {
        assert_eq!(
            producer_registry
                .deliver_blocking(stream_frame(1, FLAG_HEADERS_SENT, b"a"), |_, response| tx
                    .send(response)
                    .is_ok()),
            StreamDelivery::Accepted
        );
        started_tx.send(()).unwrap();
        let result = producer_registry.deliver_blocking(stream_frame(1, 0, b"b"), |_, _| false);
        done_tx.send(result).unwrap();
        producer_registry.deliver_blocking(stream_frame(1, FLAG_FINAL, b""), |_, _| false)
    });
    started_rx.await.unwrap();
    let mut body = rx.await.unwrap().stream_rx.unwrap().into_inner();
    assert!(matches!(
        done_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(
        body.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"a")
    );
    assert_eq!(done_rx.await.unwrap(), StreamDelivery::Accepted);
    assert_eq!(
        body.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"b")
    );
    assert!(body.recv().await.is_none());
    assert_eq!(producer.join().unwrap(), StreamDelivery::Accepted);
    assert_eq!(registry.active_streams(), 0);
}

#[tokio::test]
async fn registry_async_backpressure_waits_for_receiver() {
    use std::sync::Arc;
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::ResponseStreamRegistry;
    let registry = Arc::new(ResponseStreamRegistry::with_capacity(0));
    let (tx, rx) = tokio::sync::oneshot::channel();
    registry
        .deliver(stream_frame(1, FLAG_HEADERS_SENT, b"a"), |_, response| {
            tx.send(response).is_ok()
        })
        .await;
    let mut receiver = rx.await.unwrap().stream_rx.unwrap().into_inner();
    let send = registry.deliver(stream_frame(1, FLAG_FINAL, b"b"), |_, _| false);
    tokio::pin!(send);
    tokio::select! { biased; _ = &mut send => panic!("full channel must backpressure"), _ = std::future::ready(()) => {} }
    assert_eq!(
        receiver.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"a")
    );
    assert_eq!(send.await, StreamDelivery::Accepted);
    assert_eq!(
        receiver.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"b")
    );
    assert!(receiver.recv().await.is_none());
}

#[test]
fn bridge_stream_sink_routes_registered_correlations() {
    use std::sync::Arc;
    use vertebra_core::bridge::{BatchBridge, StreamDelivery};
    use vertebra_http::dispatcher::BridgeCorrelation;
    let _ = BatchBridge::init(4096, 4096, 16, 1);
    let bridge = BatchBridge::get().unwrap();
    let correlation = Arc::new(BridgeCorrelation::default());
    correlation.ensure_sink_registered(bridge);
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let id = correlation.register_pending(tx).unwrap();
    assert_eq!(
        bridge
            .submit_stream_frame(stream_frame(id, FLAG_HEADERS_SENT, b"hello"))
            .unwrap(),
        StreamDelivery::Accepted
    );
    let response = rx.try_recv().unwrap();
    assert!(response.stream_rx.is_some());
    assert_eq!(
        bridge
            .submit_stream_frame(stream_frame(id, FLAG_FINAL, b""))
            .unwrap(),
        StreamDelivery::Accepted
    );
    assert_eq!(correlation.stream_registry().active_streams(), 0);
    drop(response);
}

#[test]
fn bridge_correlation_registers_until_full_and_reuses_completed_slots() {
    use vertebra_http::dispatcher::BridgeCorrelation;
    let correlation = BridgeCorrelation::with_capacity(1);
    let mut receivers = Vec::new();
    let mut ids = Vec::new();
    for _ in 0..512 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        ids.push(correlation.register_pending(tx).unwrap());
        receivers.push(rx);
    }
    let (tx, _rx) = tokio::sync::oneshot::channel();
    assert!(correlation.register_pending(tx).is_none());
    assert_eq!(_rx.blocking_recv().unwrap().status, 503);
    assert_eq!(correlation.in_flight(), 512);
    assert!(correlation.complete(
        ids[0],
        PendingResponse::unary(200, Bytes::new(), Bytes::new())
    ));
    let (tx, _rx) = tokio::sync::oneshot::channel();
    assert!(correlation.register_pending(tx).is_some());
    assert_eq!(correlation.in_flight(), 512);
}

#[tokio::test]
async fn registry_empty_final_reports_disconnected_client() {
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::ResponseStreamRegistry;
    let registry = ResponseStreamRegistry::default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    registry
        .deliver(stream_frame(1, FLAG_HEADERS_SENT, b"a"), |_, response| {
            tx.send(response).is_ok()
        })
        .await;
    drop(rx.await.unwrap());
    assert_eq!(
        registry
            .deliver(stream_frame(1, FLAG_FINAL, b""), |_, _| false)
            .await,
        StreamDelivery::Closed
    );
    assert_eq!(registry.active_streams(), 0);
}

#[tokio::test]
async fn abandoned_bridge_stream_is_pruned_by_existing_timeout_sweep() {
    use std::sync::Arc;
    use vertebra_http::dispatcher::BridgeCorrelation;
    let correlation = Arc::new(BridgeCorrelation::default());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let id = correlation.register_pending(tx).unwrap();
    correlation
        .stream_registry()
        .deliver(stream_frame(id, FLAG_HEADERS_SENT, b"a"), |id, response| {
            correlation.complete(id, response)
        })
        .await;
    drop(rx.await.unwrap());
    assert_eq!(correlation.sweep_expired(std::time::Duration::ZERO), 0);
    assert_eq!(correlation.stream_registry().active_streams(), 0);
}

fn register_shm_stream(
    shm: &vertebra_http::dispatcher::ShmDispatcher,
) -> (u64, tokio::sync::oneshot::Receiver<PendingResponse>) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let id = shm
        .correlation_shard(0)
        .lock()
        .unwrap()
        .insert(0, tx, 0)
        .unwrap()
        .raw();
    (id, rx)
}

#[tokio::test]
async fn worker_sweep_closes_stream_and_cancels_a_backpressured_sender_clone() {
    use std::sync::Arc;
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
    use vertebra_http::dispatcher::ShmDispatcher;
    let pair = Arc::new(RingPair::create(RingPairConfig::compact()).unwrap());
    let shm = ShmDispatcher::new(vec![pair], 128);
    let (id, response) = register_shm_stream(&shm);
    assert_eq!(
        shm.deliver_stream_frame(stream_frame(id, FLAG_HEADERS_SENT, b"a"))
            .await,
        StreamDelivery::Accepted
    );
    for _ in 1..64 {
        assert_eq!(
            shm.deliver_stream_frame(stream_frame(id, 0, b"b")).await,
            StreamDelivery::Accepted
        );
    }
    let pending = response.await.unwrap();
    let send = shm.deliver_stream_frame(stream_frame(id, 0, b"blocked"));
    tokio::pin!(send);
    tokio::select! { biased; _ = &mut send => panic!("full stream must backpressure"), _ = std::future::ready(()) => {} }
    assert_eq!(shm.sweep_worker(0), 1);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), send)
            .await
            .unwrap(),
        StreamDelivery::Closed
    );
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        build_response(pending).into_body().collect(),
    )
    .await
    .unwrap()
    .unwrap()
    .to_bytes();
    assert_eq!(bytes.len(), 64);
    assert_eq!(bytes[0], b'a');
    assert!(bytes[1..].iter().all(|byte| *byte == b'b'));
    assert_eq!(shm.active_streams(), 0);
}

#[tokio::test]
async fn shm_reader_streams_progressively_then_keeps_serving_unary_responses() {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
    use vertebra_http::dispatcher::shm_reader::spawn_response_readers;
    use vertebra_http::dispatcher::{Dispatcher, ShmDispatcher};
    let mut config = RingPairConfig::compact();
    config.response_tiers.xlarge.slot_count = 8;
    let pair = Arc::new(RingPair::create(config).unwrap());
    let shm = ShmDispatcher::new(vec![pair.clone()], 128);
    let dispatcher = Arc::new(Dispatcher::from_shm(shm.clone()));
    let shutdown = CancellationToken::new();
    let reader = spawn_response_readers(vec![pair.clone()], dispatcher, shutdown.clone());
    let (id, response) = register_shm_stream(&shm);
    pair.send_stream_frame(id, 200, FLAG_STREAMING | FLAG_HEADERS_SENT, b"", b"a")
        .unwrap();
    pair.response_notifier.notify().unwrap();
    let pending = tokio::time::timeout(std::time::Duration::from_secs(2), response)
        .await
        .unwrap()
        .unwrap();
    assert!(pending.stream_rx.is_some());
    let continuation = vec![b'b'; 5000];
    pair.send_stream_frame(id, 200, FLAG_STREAMING, b"", &continuation)
        .unwrap();
    pair.send_stream_frame(id, 200, FLAG_STREAMING | FLAG_FINAL, b"", b"")
        .unwrap();
    pair.response_notifier.notify().unwrap();
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        build_response(pending).into_body().collect(),
    )
    .await
    .unwrap()
    .unwrap()
    .to_bytes();
    assert_eq!(bytes.len(), 5001);
    assert_eq!(bytes[0], b'a');
    assert_eq!(&bytes[1..], continuation);
    let (id, response) = register_shm_stream(&shm);
    pair.response_rings
        .send_response(id, 201, b"", b"ok")
        .unwrap();
    pair.response_notifier.notify().unwrap();
    let unary = tokio::time::timeout(std::time::Duration::from_secs(2), response)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unary.status, 201);
    assert_eq!(unary.body.as_ref(), b"ok");
    assert!(unary.stream_rx.is_none());
    shutdown.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), reader)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(shm.active_streams(), 0);
}

#[tokio::test]
async fn shm_reader_shutdown_cancels_a_full_stream_channel() {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
    use vertebra_http::dispatcher::shm_reader::spawn_response_readers;
    use vertebra_http::dispatcher::{Dispatcher, ShmDispatcher};
    let mut config = RingPairConfig::compact();
    config.response_tiers.xlarge.slot_size = 4096;
    config.response_tiers.xlarge.slot_count = 128;
    let pair = Arc::new(RingPair::create(config).unwrap());
    let shm = ShmDispatcher::new(vec![pair.clone()], 128);
    let (id, response) = register_shm_stream(&shm);
    pair.send_stream_frame(id, 200, FLAG_STREAMING | FLAG_HEADERS_SENT, b"", b"a")
        .unwrap();
    for _ in 0..65 {
        pair.send_stream_frame(id, 200, FLAG_STREAMING, b"", b"b")
            .unwrap();
    }
    let shutdown = CancellationToken::new();
    let reader = spawn_response_readers(
        vec![pair.clone()],
        Arc::new(Dispatcher::from_shm(shm.clone())),
        shutdown.clone(),
    );
    pair.response_notifier.notify().unwrap();
    let pending = tokio::time::timeout(std::time::Duration::from_secs(2), response)
        .await
        .unwrap()
        .unwrap();
    let mut receiver = pending.stream_rx.unwrap().into_inner();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while receiver.len() < 64 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), reader)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(shm.active_streams(), 0);
    let mut bytes = Vec::new();
    while let Some(frame) = receiver.recv().await {
        bytes.extend_from_slice(&frame.unwrap().into_data().unwrap());
    }
    assert_eq!(bytes.len(), 64);
}

#[tokio::test]
async fn dropped_bridge_receiver_releases_a_blocked_producer() {
    use std::sync::Arc;
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::ResponseStreamRegistry;
    let registry = Arc::new(ResponseStreamRegistry::with_capacity(1));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let worker_registry = registry.clone();
    let worker = std::thread::spawn(move || {
        worker_registry
            .deliver_blocking(stream_frame(1, FLAG_HEADERS_SENT, b"a"), |_, response| {
                tx.send(response).is_ok()
            });
        started_tx.send(()).unwrap();
        done_tx
            .send(worker_registry.deliver_blocking(stream_frame(1, 0, b"b"), |_, _| false))
            .unwrap();
    });
    started_rx.await.unwrap();
    drop(rx.await.unwrap());
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), done_rx)
            .await
            .unwrap()
            .unwrap(),
        StreamDelivery::Closed
    );
    worker.join().unwrap();
    assert_eq!(registry.active_streams(), 0);
}

#[tokio::test]
async fn stalled_slow_stream_does_not_block_concurrent_fast_stream_or_unary() {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
    use vertebra_http::dispatcher::shm_reader::spawn_response_readers;
    use vertebra_http::dispatcher::{Dispatcher, ShmDispatcher};

    let mut config = RingPairConfig::compact();
    config.response_tiers.xlarge.slot_size = 4096;
    config.response_tiers.xlarge.slot_count = 256;
    let pair = Arc::new(RingPair::create(config).unwrap());
    let shm = ShmDispatcher::new(vec![pair.clone()], 128);
    let dispatcher = Arc::new(Dispatcher::from_shm(shm.clone()));
    let shutdown = CancellationToken::new();
    let reader = spawn_response_readers(vec![pair.clone()], dispatcher, shutdown.clone());

    let (slow_id, slow_resp) = register_shm_stream(&shm);
    let (fast_id, fast_resp) = register_shm_stream(&shm);
    let (unary_id, unary_resp) = register_shm_stream(&shm);

    // Slow stream saturates both its response channel (64) and bounded staging queue (64)
    pair.send_stream_frame(slow_id, 200, FLAG_STREAMING | FLAG_HEADERS_SENT, b"", b"s0")
        .unwrap();
    for _ in 0..150 {
        pair.send_stream_frame(slow_id, 200, FLAG_STREAMING, b"", b"s")
            .unwrap();
    }

    // Sibling fast stream and unary response arrive after the stalled slow stream on the same worker
    pair.send_stream_frame(
        fast_id,
        200,
        FLAG_STREAMING | FLAG_HEADERS_SENT,
        b"",
        b"fast-1:",
    )
    .unwrap();
    pair.send_stream_frame(fast_id, 200, FLAG_STREAMING, b"", b"fast-2")
        .unwrap();
    pair.send_stream_frame(fast_id, 200, FLAG_STREAMING | FLAG_FINAL, b"", b"")
        .unwrap();
    pair.response_rings
        .send_response(unary_id, 200, b"", b"unary-ok")
        .unwrap();
    pair.response_notifier.notify().unwrap();

    // Never poll slow_pending while verifying fast stream and unary completion
    let _slow_pending = tokio::time::timeout(std::time::Duration::from_secs(2), slow_resp)
        .await
        .expect("slow stream initial response")
        .unwrap();

    let unary = tokio::time::timeout(std::time::Duration::from_secs(2), unary_resp)
        .await
        .expect("unary response must not be stalled by slow stream")
        .unwrap();
    assert_eq!(unary.status, 200);
    assert_eq!(unary.body.as_ref(), b"unary-ok");

    let fast_pending = tokio::time::timeout(std::time::Duration::from_secs(2), fast_resp)
        .await
        .expect("fast stream must not be stalled by slow stream")
        .unwrap();
    let fast_bytes = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        build_response(fast_pending).into_body().collect(),
    )
    .await
    .expect("fast stream body must complete promptly")
    .unwrap()
    .to_bytes();
    assert_eq!(fast_bytes.as_ref(), b"fast-1:fast-2");

    shutdown.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), reader)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(shm.active_streams(), 0);
}

#[tokio::test]
async fn backpressured_slow_stream_drains_after_sibling_fast_stream_completes() {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
    use vertebra_http::dispatcher::shm_reader::spawn_response_readers;
    use vertebra_http::dispatcher::{Dispatcher, ShmDispatcher};

    let mut config = RingPairConfig::compact();
    config.response_tiers.xlarge.slot_size = 4096;
    config.response_tiers.xlarge.slot_count = 128;
    let pair = Arc::new(RingPair::create(config).unwrap());
    let shm = ShmDispatcher::new(vec![pair.clone()], 128);
    let dispatcher = Arc::new(Dispatcher::from_shm(shm.clone()));
    let shutdown = CancellationToken::new();
    let reader = spawn_response_readers(vec![pair.clone()], dispatcher, shutdown.clone());

    let (slow_id, slow_resp) = register_shm_stream(&shm);
    let (fast_id, fast_resp) = register_shm_stream(&shm);

    // 65 frames: fills channel (64) + blocks forwarder task on frame 65 without saturating staging
    pair.send_stream_frame(slow_id, 200, FLAG_STREAMING | FLAG_HEADERS_SENT, b"", b"a")
        .unwrap();
    for _ in 0..63 {
        pair.send_stream_frame(slow_id, 200, FLAG_STREAMING, b"", b"b")
            .unwrap();
    }
    pair.send_stream_frame(slow_id, 200, FLAG_STREAMING | FLAG_FINAL, b"", b"z")
        .unwrap();

    pair.send_stream_frame(
        fast_id,
        200,
        FLAG_STREAMING | FLAG_HEADERS_SENT | FLAG_FINAL,
        b"",
        b"fast-ok",
    )
    .unwrap();
    pair.response_notifier.notify().unwrap();

    let slow_pending = tokio::time::timeout(std::time::Duration::from_secs(2), slow_resp)
        .await
        .unwrap()
        .unwrap();

    let fast_pending = tokio::time::timeout(std::time::Duration::from_secs(2), fast_resp)
        .await
        .expect("fast stream must complete while slow stream is backpressured")
        .unwrap();
    assert_eq!(fast_pending.body.as_ref(), b"fast-ok");

    let slow_bytes = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        build_response(slow_pending).into_body().collect(),
    )
    .await
    .unwrap()
    .unwrap()
    .to_bytes();
    assert_eq!(slow_bytes.len(), 65);
    assert_eq!(slow_bytes[0], b'a');
    assert_eq!(slow_bytes[64], b'z');

    shutdown.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), reader)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(shm.active_streams(), 0);
}

#[tokio::test]
async fn dispatcher_new_with_queue_depth_enforces_configured_backpressure() {
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::{BridgeCorrelation, DispatchBackend, Dispatcher};

    let zero_clamped = BridgeCorrelation::with_capacity_and_queue_depth(128, 0);
    assert_eq!(zero_clamped.stream_registry().capacity(), 1);

    let dispatcher = Dispatcher::new_with_queue_depth(2);
    let DispatchBackend::Bridge(correlation) = dispatcher.backend() else {
        panic!("expected bridge backend");
    };
    assert_eq!(correlation.stream_registry().capacity(), 2);

    let (tx, rx) = tokio::sync::oneshot::channel();
    let id = correlation.register_pending(tx).unwrap();
    assert_eq!(
        correlation
            .stream_registry()
            .deliver(stream_frame(id, FLAG_HEADERS_SENT, b"a"), |id, resp| {
                correlation.complete(id, resp)
            })
            .await,
        StreamDelivery::Accepted
    );
    assert_eq!(
        correlation
            .stream_registry()
            .deliver(stream_frame(id, 0, b"b"), |_, _| false)
            .await,
        StreamDelivery::Accepted
    );

    let mut receiver = rx.await.unwrap().stream_rx.unwrap().into_inner();
    let third = correlation
        .stream_registry()
        .deliver(stream_frame(id, FLAG_FINAL, b"c"), |_, _| false);
    tokio::pin!(third);
    tokio::select! {
        biased;
        _ = &mut third => panic!("configured queue depth 2 must backpressure on 3rd frame"),
        _ = std::future::ready(()) => {}
    }

    assert_eq!(
        receiver.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"a")
    );
    assert_eq!(third.await, StreamDelivery::Accepted);
    assert_eq!(
        receiver.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"b")
    );
    assert_eq!(
        receiver.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"c")
    );
    assert!(receiver.recv().await.is_none());
    assert_eq!(correlation.stream_registry().active_streams(), 0);
}

#[tokio::test]
async fn shm_dispatcher_new_with_queue_depth_enforces_configured_backpressure() {
    use std::sync::Arc;
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
    use vertebra_http::dispatcher::{Dispatcher, ShmDispatcher};

    let pair = Arc::new(RingPair::create(RingPairConfig::compact()).unwrap());
    let shm = ShmDispatcher::new_with_queue_depth(vec![pair.clone()], 128, 2);
    let (id, response) = register_shm_stream(&shm);
    assert_eq!(
        shm.deliver_stream_frame(stream_frame(id, FLAG_HEADERS_SENT, b"a"))
            .await,
        StreamDelivery::Accepted
    );
    assert_eq!(
        shm.deliver_stream_frame(stream_frame(id, 0, b"b")).await,
        StreamDelivery::Accepted
    );

    let mut receiver = response.await.unwrap().stream_rx.unwrap().into_inner();
    let third = shm.deliver_stream_frame(stream_frame(id, FLAG_FINAL, b"c"));
    tokio::pin!(third);
    tokio::select! {
        biased;
        _ = &mut third => panic!("SHM queue depth 2 must backpressure on 3rd frame"),
        _ = std::future::ready(()) => {}
    }

    assert_eq!(
        receiver.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"a")
    );
    assert_eq!(third.await, StreamDelivery::Accepted);
    assert_eq!(
        receiver.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"b")
    );
    assert_eq!(
        receiver.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"c")
    );
    assert!(receiver.recv().await.is_none());
    assert_eq!(shm.active_streams(), 0);

    let dispatcher = Dispatcher::new_shm_with_queue_depth(vec![pair], 128, 2);
    let shm_from_disp = dispatcher.shm_dispatcher().unwrap();
    let (id2, response2) = register_shm_stream(shm_from_disp);
    assert_eq!(
        dispatcher
            .deliver_stream_frame(stream_frame(id2, FLAG_HEADERS_SENT, b"x"))
            .await,
        StreamDelivery::Accepted
    );
    assert_eq!(
        dispatcher
            .deliver_stream_frame(stream_frame(id2, 0, b"y"))
            .await,
        StreamDelivery::Accepted
    );
    let mut rx2 = response2.await.unwrap().stream_rx.unwrap().into_inner();
    let third2 = dispatcher.deliver_stream_frame(stream_frame(id2, FLAG_FINAL, b"z"));
    tokio::pin!(third2);
    tokio::select! {
        biased;
        _ = &mut third2 => panic!("Dispatcher SHM queue depth 2 must backpressure on 3rd frame"),
        _ = std::future::ready(()) => {}
    }
    assert_eq!(
        rx2.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"x")
    );
    assert_eq!(third2.await, StreamDelivery::Accepted);
    assert_eq!(
        rx2.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"y")
    );
    assert_eq!(
        rx2.recv().await.unwrap().unwrap().into_data().unwrap(),
        Bytes::from_static(b"z")
    );
    assert!(rx2.recv().await.is_none());
    assert_eq!(shm_from_disp.active_streams(), 0);
}

#[tokio::test]
async fn bridge_correlation_with_queue_depth_sweeps_abandoned_streams() {
    use vertebra_core::bridge::StreamDelivery;
    use vertebra_http::dispatcher::BridgeCorrelation;

    let correlation = BridgeCorrelation::with_capacity_and_queue_depth(128, 2);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let id = correlation.register_pending(tx).unwrap();
    assert_eq!(
        correlation
            .stream_registry()
            .deliver(stream_frame(id, FLAG_HEADERS_SENT, b"a"), |id, resp| {
                correlation.complete(id, resp)
            })
            .await,
        StreamDelivery::Accepted
    );
    assert_eq!(correlation.stream_registry().active_streams(), 1);
    drop(rx.await.unwrap());
    assert_eq!(correlation.sweep_expired(std::time::Duration::ZERO), 0);
    assert_eq!(correlation.stream_registry().active_streams(), 0);
}
