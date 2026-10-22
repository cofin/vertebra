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
