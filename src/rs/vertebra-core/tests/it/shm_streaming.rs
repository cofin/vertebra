use std::sync::Arc;

use vertebra_core::buffer::{ChannelMode, RingLayout};
use vertebra_core::channel::SpscRing;
use vertebra_core::ipc::{
    FLAG_ERROR, FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING, FLAG_TRAILERS, IpcError,
    ResponseSlot,
};
use vertebra_core::protocol::response_flags::{STREAM_CHUNK, STREAM_END, STREAM_START};
use vertebra_core::shm::ShmRegion;

#[test]
fn response_slot_encode_raw_parts_and_o1_peeking() {
    let headers = vec![
        ("content-type".to_string(), "text/event-stream".to_string()),
        ("cache-control".to_string(), "no-cache".to_string()),
    ];
    let body = b"data: hello\n\n";
    let mut buf = vec![0u8; 256];

    let written = ResponseSlot::encode_raw_parts_into(
        4242,
        200,
        FLAG_STREAMING | FLAG_HEADERS_SENT,
        &headers,
        body,
        &mut buf,
    )
    .expect("encode_raw_parts_into should succeed");

    assert_eq!(ResponseSlot::peek_request_id(&buf[..written]), Some(4242));
    assert_eq!(
        ResponseSlot::peek_flags(&buf[..written]),
        Some(FLAG_STREAMING | FLAG_HEADERS_SENT)
    );

    let decoded = ResponseSlot::decode_from(&buf[..written]).expect("decode ResponseSlot");
    assert_eq!(decoded.request_id, 4242);
    assert_eq!(decoded.status, 200);
    assert_eq!(decoded.flags, FLAG_STREAMING | FLAG_HEADERS_SENT);
    assert_eq!(decoded.headers, headers);
    assert_eq!(decoded.body, body);
}

#[test]
fn response_slot_encode_buffer_too_small_returns_error() {
    let headers = vec![("x-test".to_string(), "value".to_string())];
    let mut tiny = [0u8; 8];
    let err =
        ResponseSlot::encode_raw_parts_into(1, 200, FLAG_FINAL, &headers, b"payload", &mut tiny)
            .expect_err("should fail on small destination");
    assert!(matches!(err, IpcError::BufferTooSmall { .. }));
}

#[test]
fn spsc_ring_multi_frame_streaming_lifecycle() {
    let layout = RingLayout::new(16 * 1024, 8, ChannelMode::Spsc);
    let region = Arc::new(ShmRegion::create(layout.total_size).expect("create shm"));
    // SAFETY: `region` was freshly created with `layout.total_size` bytes for exclusive initialization.
    let ring = Arc::new(unsafe {
        SpscRing::initialize(region, layout, std::process::id(), 1).expect("init ring")
    });

    let start_headers = vec![(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    )];
    let start_len = ResponseSlot::encoded_size(&start_headers, b"chunk-0");
    ring.try_send_with(start_len, STREAM_START, |slot| {
        ResponseSlot::encode_raw_parts_into(
            9001,
            200,
            FLAG_STREAMING | FLAG_HEADERS_SENT,
            &start_headers,
            b"chunk-0",
            slot,
        )
        .expect("write start frame");
    })
    .expect("send start");

    let mid_len = ResponseSlot::encoded_size(&[], b"chunk-1");
    ring.try_send_with(mid_len, STREAM_CHUNK, |slot| {
        ResponseSlot::encode_raw_parts_into(9001, 200, FLAG_STREAMING, &[], b"chunk-1", slot)
            .expect("write middle chunk");
    })
    .expect("send middle");

    let trailer_headers = vec![("grpc-status".to_string(), "0".to_string())];
    let end_len = ResponseSlot::encoded_size(&trailer_headers, b"");
    ring.try_send_with(end_len, STREAM_END, |slot| {
        ResponseSlot::encode_raw_parts_into(
            9001,
            200,
            FLAG_STREAMING | FLAG_TRAILERS | FLAG_FINAL,
            &trailer_headers,
            b"",
            slot,
        )
        .expect("write final trailer frame");
    })
    .expect("send end");

    let mut assembled = Vec::new();
    let mut final_seen = false;
    let mut trailers_seen = Vec::new();

    while ring.has_data() {
        let view = ring.try_recv_view().expect("recv view");
        let raw = view.as_slice();
        assert_eq!(ResponseSlot::peek_request_id(raw), Some(9001));
        let flags = ResponseSlot::peek_flags(raw).expect("peek flags");
        let slot = ResponseSlot::decode_from(raw).expect("decode slot");
        if (flags & FLAG_TRAILERS) != 0 {
            trailers_seen = slot.headers;
        } else {
            assembled.extend_from_slice(&slot.body);
        }
        if (flags & (FLAG_FINAL | FLAG_ERROR)) != 0 {
            final_seen = true;
        }
    }

    assert!(final_seen);
    assert_eq!(assembled, b"chunk-0chunk-1");
    assert_eq!(
        trailers_seen,
        vec![("grpc-status".to_string(), "0".to_string())]
    );
}
