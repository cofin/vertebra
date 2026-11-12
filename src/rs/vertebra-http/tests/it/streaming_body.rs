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
    FLAG_ERROR, FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING, FLAG_TRAILERS, PendingRequests,
};
use vertebra_http::response::build_response;

fn encode_test_frame(request_id: u64, status: u16, flags: u16, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + payload.len());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

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

#[tokio::test]
async fn pending_requests_keeps_stream_open_until_flag_final_or_error() {
    let pending = PendingRequests::new();
    let request_id = 77_u64;
    let mut rx = pending.register_stream(request_id, 8);
    assert_eq!(pending.len(), 1);

    let frame1 = encode_test_frame(
        request_id,
        200,
        FLAG_STREAMING | FLAG_HEADERS_SENT,
        b"alpha",
    );
    let frame2 = encode_test_frame(request_id, 200, FLAG_STREAMING, b"beta");
    let frame3 = encode_test_frame(
        request_id,
        200,
        FLAG_STREAMING | FLAG_TRAILERS | FLAG_FINAL,
        b"grpc-status:0",
    );

    assert!(pending.deliver_raw_frame(frame1));
    assert_eq!(
        pending.len(),
        1,
        "stream entry must remain active after start frame"
    );

    assert!(pending.deliver_raw_frame(frame2));
    assert_eq!(
        pending.len(),
        1,
        "stream entry must remain active after middle chunk"
    );

    assert!(pending.deliver_raw_frame(frame3));
    assert_eq!(
        pending.len(),
        0,
        "stream entry must be removed when FLAG_FINAL is set"
    );

    let recv1 = rx.recv().await.expect("frame 1");
    let recv2 = rx.recv().await.expect("frame 2");
    let recv3 = rx.recv().await.expect("frame 3");
    assert!(
        rx.recv().await.is_none(),
        "stream must close after FLAG_FINAL"
    );
    assert_eq!(&recv1[16..], b"alpha");
    assert_eq!(&recv2[16..], b"beta");
    assert_eq!(&recv3[16..], b"grpc-status:0");

    let err_id = 88_u64;
    let mut err_rx = pending.register_stream(err_id, 4);
    let err_frame = encode_test_frame(err_id, 500, FLAG_STREAMING | FLAG_ERROR, b"boom");
    assert!(pending.deliver_raw_frame(err_frame));
    assert_eq!(
        pending.len(),
        0,
        "stream entry must be removed when FLAG_ERROR is set"
    );
    assert!(err_rx.recv().await.is_some());
    assert!(err_rx.recv().await.is_none());
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
