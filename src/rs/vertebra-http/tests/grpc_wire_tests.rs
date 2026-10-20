use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::BodyExt;

use vertebra_http::grpc::{
    FLAG_TRAILERS, GRPC_HEADER_LEN, GrpcFrame, GrpcStatus, GrpcWireFormat,
    build_grpc_response_with_trailers, build_grpc_trailers, build_grpc_trailers_only_response,
    decode_grpc_frames, encode_grpc_frame, is_grpc_request, parse_grpc_timeout,
    percent_decode_grpc_message, percent_encode_grpc_message,
};

#[test]
fn test_grpc_status_codes_0_through_16() {
    for code in 0u8..=16u8 {
        let status = GrpcStatus::from_u8(code).expect("valid status code");
        assert_eq!(status.as_u8(), code);
        assert!(!status.as_str().is_empty());
    }
    assert_eq!(GrpcStatus::from_u8(99), None);
    assert_eq!(GrpcStatus::Ok.as_u8(), 0);
    assert_eq!(GrpcStatus::DeadlineExceeded.as_u8(), 4);
    assert_eq!(GrpcStatus::ResourceExhausted.as_u8(), 8);
    assert_eq!(GrpcStatus::Unimplemented.as_u8(), 12);
    assert_eq!(GrpcStatus::Unauthenticated.as_u8(), 16);
}

#[test]
fn test_is_grpc_request_and_wire_format_detection() {
    let req_msgpack = Request::builder()
        .method(Method::POST)
        .uri("/pkg.EchoService/Echo")
        .header("content-type", "application/grpc+msgpack")
        .body(())
        .unwrap();
    assert!(is_grpc_request(&req_msgpack));
    assert_eq!(
        GrpcWireFormat::from_headers(req_msgpack.headers()),
        GrpcWireFormat::Msgpack
    );

    let req_proto = Request::builder()
        .method(Method::POST)
        .uri("/pkg.EchoService/Echo")
        .header("content-type", "application/grpc+proto")
        .body(())
        .unwrap();
    assert!(is_grpc_request(&req_proto));
    assert_eq!(
        GrpcWireFormat::from_headers(req_proto.headers()),
        GrpcWireFormat::Proto
    );

    let req_get = Request::builder()
        .method(Method::GET)
        .uri("/pkg.EchoService/Echo")
        .header("content-type", "application/grpc")
        .body(())
        .unwrap();
    assert!(!is_grpc_request(&req_get));
}

#[test]
fn test_encode_and_decode_grpc_frames_across_partial_buffers() {
    let mut wire = BytesMut::new();
    encode_grpc_frame(b"first-message", false, &mut wire);
    encode_grpc_frame(b"second-message-compressed", true, &mut wire);

    let split_point = GRPC_HEADER_LEN + 5;
    let mut part1 = wire.split_to(split_point);

    let frames1 = decode_grpc_frames(&mut part1, 4 * 1024 * 1024).unwrap();
    assert!(frames1.is_empty());
    assert_eq!(part1.len(), split_point);

    part1.unsplit(wire);
    let frames2 = decode_grpc_frames(&mut part1, 4 * 1024 * 1024).unwrap();
    assert_eq!(frames2.len(), 2);
    assert_eq!(
        frames2[0],
        GrpcFrame {
            compressed: false,
            payload: Bytes::from_static(b"first-message"),
        }
    );
    assert_eq!(
        frames2[1],
        GrpcFrame {
            compressed: true,
            payload: Bytes::from_static(b"second-message-compressed"),
        }
    );
    assert!(part1.is_empty());
}

#[test]
fn test_decode_grpc_frames_rejects_oversize_payload() {
    let mut wire = BytesMut::new();
    encode_grpc_frame(&[0u8; 128], false, &mut wire);
    let err = decode_grpc_frames(&mut wire, 64).unwrap_err();
    assert_eq!(err.status(), GrpcStatus::ResourceExhausted);
}

#[test]
fn test_percent_encode_and_build_grpc_trailers() {
    let raw = "invalid argument: field \"email\" contains spaces & symbols % 100%\nline2";
    let encoded = percent_encode_grpc_message(raw);
    assert!(!encoded.contains(' '));
    assert!(!encoded.contains('\n'));
    assert_eq!(percent_decode_grpc_message(&encoded), raw);

    let trailers: HeaderMap = build_grpc_trailers(GrpcStatus::InvalidArgument, Some(raw));
    assert_eq!(trailers.get("grpc-status").unwrap(), "3");
    assert_eq!(
        trailers.get("grpc-message").unwrap().to_str().unwrap(),
        encoded
    );
}

#[test]
fn test_parse_grpc_timeout_all_units() {
    assert_eq!(
        parse_grpc_timeout("2H").unwrap(),
        Duration::from_secs(2 * 3600)
    );
    assert_eq!(
        parse_grpc_timeout("15M").unwrap(),
        Duration::from_secs(15 * 60)
    );
    assert_eq!(parse_grpc_timeout("30S").unwrap(), Duration::from_secs(30));
    assert_eq!(
        parse_grpc_timeout("250m").unwrap(),
        Duration::from_millis(250)
    );
    assert_eq!(
        parse_grpc_timeout("500u").unwrap(),
        Duration::from_micros(500)
    );
    assert_eq!(
        parse_grpc_timeout("1000n").unwrap(),
        Duration::from_nanos(1000)
    );
    assert!(parse_grpc_timeout("").is_err());
    assert!(parse_grpc_timeout("10X").is_err());
}

#[tokio::test]
async fn test_flag_trailers_http2_trailer_emission() {
    assert_eq!(FLAG_TRAILERS, 1 << 5);

    let frames = vec![
        GrpcFrame {
            compressed: false,
            payload: Bytes::from_static(b"msg-1"),
        },
        GrpcFrame {
            compressed: false,
            payload: Bytes::from_static(b"msg-2"),
        },
    ];
    let resp = build_grpc_response_with_trailers(
        GrpcWireFormat::Msgpack,
        frames,
        GrpcStatus::Ok,
        None,
        FLAG_TRAILERS,
    );
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/grpc+msgpack"
    );

    let collected = resp.into_body().collect().await.unwrap();
    let trailers = collected
        .trailers()
        .expect("HTTP/2 trailers frame expected");
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    let body_bytes = collected.to_bytes();
    assert_eq!(body_bytes.len(), (GRPC_HEADER_LEN + 5) * 2);

    let err_resp = build_grpc_trailers_only_response(
        GrpcWireFormat::Proto,
        GrpcStatus::DeadlineExceeded,
        Some("deadline exceeded after 250ms"),
    );
    assert_eq!(err_resp.status(), StatusCode::OK);
    assert_eq!(err_resp.headers().get("grpc-status").unwrap(), "4");
    assert!(err_resp.headers().get("grpc-message").is_some());
}
