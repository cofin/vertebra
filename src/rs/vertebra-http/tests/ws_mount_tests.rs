use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{HeaderMap, HeaderValue, Method, StatusCode, Version, header};
use http_body_util::BodyExt;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::protocol::Message;
use vertebra_http::hmr::{HmrEvent, HmrEventKind};
use vertebra_http::ws_mount::{
    WebSocketHandshakeDecision, WsConfig, WsConnectionRegistry, WsIncoming, WsOutgoing,
    build_handshake_response, is_websocket_upgrade_parts, negotiate_subprotocol, run_ws_connection,
};

#[test]
fn detects_valid_rfc6455_upgrade_on_any_application_path() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONNECTION,
        HeaderValue::from_static("keep-alive, Upgrade"),
    );
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(
        header::SEC_WEBSOCKET_VERSION,
        HeaderValue::from_static("13"),
    );
    headers.insert(
        header::SEC_WEBSOCKET_KEY,
        HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ=="),
    );

    assert!(is_websocket_upgrade_parts(
        &Method::GET,
        Version::HTTP_11,
        &headers
    ));
    assert!(!is_websocket_upgrade_parts(
        &Method::POST,
        Version::HTTP_11,
        &headers
    ));
}

#[test]
fn negotiates_first_matching_subprotocol() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("chat.v2, json.v1, Binary"),
    );
    let supported = ["json.v1", "chat.v1"];
    assert_eq!(
        negotiate_subprotocol(&headers, &supported),
        Some("json.v1".to_string())
    );
}

#[tokio::test]
async fn pre_upgrade_reject_decision_returns_custom_http_response_without_101() {
    let decision = WebSocketHandshakeDecision::Reject {
        status: 403,
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        body: Bytes::from_static(br#"{"detail":"forbidden"}"#),
    };

    let resp = build_handshake_response("dGhlIHNhbXBsZSBub25jZQ==", false, decision);
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert!(resp.headers().get(header::SEC_WEBSOCKET_ACCEPT).is_none());
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, Bytes::from_static(br#"{"detail":"forbidden"}"#));
}

#[tokio::test]
async fn ws_connection_registry_and_watchdog_manage_full_lifecycle() {
    let registry = Arc::new(WsConnectionRegistry::new());
    let config = WsConfig {
        max_frame_size: 4096,
        max_message_size: 8192,
        ping_interval: Duration::from_millis(40),
        idle_timeout: Duration::from_millis(150),
        enable_permessage_deflate: true,
        fast_connect: false,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let reg_clone = Arc::clone(&registry);
    let server_task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let ws_stream = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let (in_tx, mut in_rx) = tokio::sync::mpsc::channel::<WsIncoming>(32);
        let (out_tx, out_rx) = tokio::sync::mpsc::channel::<WsOutgoing>(32);

        let echo_task = tokio::spawn(async move {
            while let Some(msg) = in_rx.recv().await {
                match msg {
                    WsIncoming::Text(t) => {
                        let _ = out_tx.send(WsOutgoing::Text(format!("echo:{t}"))).await;
                    }
                    WsIncoming::Binary(b) => {
                        let _ = out_tx.send(WsOutgoing::Binary(b)).await;
                    }
                    WsIncoming::Close(_, _) => break,
                }
            }
        });

        run_ws_connection(
            ws_stream,
            "/ws/chat".to_string(),
            Some("json.v1".to_string()),
            config,
            reg_clone,
            in_tx,
            out_rx,
        )
        .await;
        echo_task.abort();
    });

    let (mut client_ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/chat"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(registry.active_count(), 1);

    client_ws.send(Message::Text("hello".into())).await.unwrap();
    let reply = client_ws.next().await.unwrap().unwrap();
    assert_eq!(reply, Message::Text("echo:hello".into()));

    registry.close_all(1001, "server shutdown").await;
    let _ = server_task.await;
    assert_eq!(registry.active_count(), 0);
}

#[test]
fn hmr_event_serialize_includes_type_and_event_fields() {
    let ev = HmrEvent::new(HmrEventKind::CssUpdate, "/static/app.css", 7);
    let json: serde_json::Value = serde_json::from_str(&ev.serialize()).unwrap();
    assert_eq!(json["type"], "css-update");
    assert_eq!(json["event"], "css-update");
    assert_eq!(json["path"], "/static/app.css");
    assert_eq!(json["generation"], 7);
}
