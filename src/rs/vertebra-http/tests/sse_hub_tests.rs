use std::time::Duration;

use bytes::Bytes;
use http::header;
use http_body_util::BodyExt;
use vertebra_http::sse::{SseEvent, SseHub, SseTopicBuffer, encode_sse_event};

#[test]
fn sse_event_encodes_multiline_data_and_all_fields() {
    let event = SseEvent::new("line-1\r\nline-2\rline-3\nline-4")
        .with_id("42")
        .with_event("chat.message")
        .with_retry_ms(3000)
        .with_comment("trace-1\ntrace-2");

    let encoded = encode_sse_event(&event);
    let text = std::str::from_utf8(&encoded).expect("valid utf-8");

    assert_eq!(
        text,
        ": trace-1\r\n: trace-2\r\nid: 42\r\nevent: chat.message\r\nretry: 3000\r\ndata: line-1\r\ndata: line-2\r\ndata: line-3\r\ndata: line-4\r\n\r\n"
    );
}

#[test]
fn sse_event_strips_crlf_and_nul_from_id_and_event_fields() {
    let event = SseEvent::new("payload")
        .with_id("99\r\ndata: injected\0")
        .with_event("update\nretry: 1");

    let encoded = encode_sse_event(&event);
    let text = std::str::from_utf8(&encoded).expect("valid utf-8");

    assert!(!text.contains("\r\ndata: injected"));
    assert!(!text.contains("\r\nretry: 1\r\n"));
    assert!(text.contains("id: 99data: injected\r\n"));
    assert!(text.contains("event: updateretry: 1\r\n"));
}

#[test]
fn sse_topic_buffer_replays_events_after_last_event_id_with_ring_eviction() {
    let mut buffer = SseTopicBuffer::new(3);

    for seq in 1..=5_u64 {
        let ev = SseEvent::new(format!("msg-{seq}")).with_id(seq.to_string());
        buffer.push(ev);
    }

    assert_eq!(buffer.len(), 3);

    let replayed = buffer.replay_since("3");
    assert_eq!(replayed.len(), 2);
    assert_eq!(
        std::str::from_utf8(&replayed[0]).unwrap(),
        "id: 4\r\ndata: msg-4\r\n\r\n"
    );
    assert_eq!(
        std::str::from_utf8(&replayed[1]).unwrap(),
        "id: 5\r\ndata: msg-5\r\n\r\n"
    );
}

#[tokio::test]
async fn sse_hub_replays_missed_events_streams_live_frames_and_emits_keepalive_ping() {
    let hub = SseHub::new(16, Duration::from_millis(40));

    hub.publish("alerts", SseEvent::new("first").with_id("1"));
    hub.publish("alerts", SseEvent::new("second").with_id("2"));

    let response = hub.subscribe_response("alerts", Some("1"));
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream; charset=utf-8")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok()),
        Some("no-cache, no-transform")
    );
    assert_eq!(
        response
            .headers()
            .get("x-accel-buffering")
            .and_then(|v| v.to_str().ok()),
        Some("no")
    );

    let mut body = response.into_body();

    let replay_frame = body
        .frame()
        .await
        .expect("replay frame")
        .expect("infallible")
        .into_data()
        .expect("data frame");
    assert_eq!(
        replay_frame,
        Bytes::from_static(b"id: 2\r\ndata: second\r\n\r\n")
    );

    hub.publish("alerts", SseEvent::new("third").with_id("3"));
    let live_frame = body
        .frame()
        .await
        .expect("live frame")
        .expect("infallible")
        .into_data()
        .expect("data frame");
    assert_eq!(
        live_frame,
        Bytes::from_static(b"id: 3\r\ndata: third\r\n\r\n")
    );

    let ping_frame = tokio::time::timeout(Duration::from_millis(200), body.frame())
        .await
        .expect("heartbeat within timeout")
        .expect("heartbeat frame")
        .expect("infallible")
        .into_data()
        .expect("data frame");
    assert_eq!(ping_frame, Bytes::from_static(b": ping\r\n\r\n"));
}
