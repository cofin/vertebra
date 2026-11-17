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

    client_ws.send(Message::Text("hello".into())).await.unwrap();
    let reply = client_ws.next().await.unwrap().unwrap();
    assert_eq!(reply, Message::Text("echo:hello".into()));
    assert_eq!(registry.active_count(), 1);

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

#[tokio::test]
async fn ws_mount_upgrades_forwards_pubsub_and_closes_on_server_shutdown() {
    use tokio::sync::oneshot;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    use vertebra_http::batcher::{BatchLimit, RequestBatcher};
    use vertebra_http::config::ServerConfig;
    use vertebra_http::dispatcher::Dispatcher;
    use vertebra_http::routing::{RouteTable, Router};
    use vertebra_http::server::{self, ServerState, WsMount};

    for enable_h2c_prior_knowledge in [false, true] {
        let router = Arc::new(Router::from_entries(RouteTable::new().into_entries()).unwrap());
        let (dispatcher, _direct_rx) = Dispatcher::new_direct(8);
        let dispatcher = Arc::new(dispatcher);
        let (_collector, collector_tx) =
            RequestBatcher::new(Arc::clone(&dispatcher), BatchLimit::default(), 16);

        let listener = server::bind_listener("127.0.0.1:0".parse().unwrap(), false).unwrap();
        let addr = listener.local_addr().unwrap();
        let config = Arc::new(ServerConfig {
            bind: addr,
            enable_h2c_prior_knowledge,
            drain_timeout: Duration::from_millis(200),
            ..ServerConfig::default()
        });

        let mut custom_mount = WsMount::new("/ws");
        custom_mount.allowed_origins = vec!["https://app.example.com".to_string()];
        custom_mount.supported_subprotocols = vec!["json.v1".to_string()];

        let state = ServerState::new(router, dispatcher, config, vec![collector_tx])
            .with_ws_allowed_origins(vec!["https://app.example.com".to_string()])
            .with_ws_supported_subprotocols(vec!["json.v1".to_string(), "chat.v1".to_string()])
            .with_ws_mount(custom_mount);

        let pubsub = Arc::clone(&state.pubsub_broker);
        let ws_registry = Arc::clone(&state.ws_registry);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server_handle = tokio::spawn(async move {
            server::run_with_listener_state(listener, state, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
        });

        let mut bad_origin_req = format!("ws://{addr}/__vertebra/ws/alerts")
            .into_client_request()
            .unwrap();
        bad_origin_req
            .headers_mut()
            .insert("Origin", "https://evil.example.com".parse().unwrap());
        let bad_err = tokio_tungstenite::connect_async(bad_origin_req)
            .await
            .unwrap_err();
        match bad_err {
            tokio_tungstenite::tungstenite::Error::Http(resp) => {
                assert_eq!(resp.status(), StatusCode::FORBIDDEN);
            }
            other => panic!("expected HTTP 403 rejection, got: {other:?}"),
        }

        let mut req = format!("ws://{addr}/__vertebra/ws/alerts")
            .into_client_request()
            .unwrap();
        req.headers_mut()
            .insert("Origin", "https://app.example.com".parse().unwrap());
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "chat.v2, json.v1".parse().unwrap(),
        );

        let (mut client_ws, upgrade_resp) = tokio_tungstenite::connect_async(req).await.unwrap();
        assert_eq!(upgrade_resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            upgrade_resp
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok()),
            Some("json.v1")
        );

        let mut custom_req = format!("ws://{addr}/ws/alerts")
            .into_client_request()
            .unwrap();
        custom_req
            .headers_mut()
            .insert("Origin", "https://app.example.com".parse().unwrap());
        let (mut custom_ws, custom_resp) =
            tokio_tungstenite::connect_async(custom_req).await.unwrap();
        assert_eq!(custom_resp.status(), StatusCode::SWITCHING_PROTOCOLS);

        let event_id = pubsub
            .pubsub_publish("alerts", Bytes::from_static(br#"{"ok":true}"#), None)
            .expect("published event id");
        assert_eq!(event_id, 1);

        let msg1 = tokio::time::timeout(Duration::from_secs(2), client_ws.next())
            .await
            .expect("timed out waiting for ws pubsub message")
            .unwrap()
            .unwrap();
        assert_eq!(msg1, Message::Text(r#"{"ok":true}"#.into()));

        let msg2 = tokio::time::timeout(Duration::from_secs(2), custom_ws.next())
            .await
            .expect("timed out waiting for custom ws mount message")
            .unwrap()
            .unwrap();
        assert_eq!(msg2, Message::Text(r#"{"ok":true}"#.into()));

        tokio::time::timeout(Duration::from_secs(2), async {
            while ws_registry.active_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both upgraded connections registered in ws_registry");

        let _ = shutdown_tx.send(());
        server_handle.await.unwrap();

        let close1 = tokio::time::timeout(Duration::from_secs(2), client_ws.next())
            .await
            .expect("timed out waiting for close frame")
            .unwrap()
            .unwrap();
        match close1 {
            Message::Close(Some(frame)) => {
                assert_eq!(frame.code, CloseCode::Away);
                assert_eq!(frame.reason, "Server shutting down");
            }
            other => panic!("expected Close(1001) frame, got: {other:?}"),
        }

        let close2 = tokio::time::timeout(Duration::from_secs(2), custom_ws.next())
            .await
            .expect("timed out waiting for custom mount close frame")
            .unwrap()
            .unwrap();
        assert!(matches!(close2, Message::Close(Some(_))));

        tokio::time::timeout(Duration::from_secs(2), async {
            while ws_registry.active_count() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ws_registry drained to zero on shutdown");
    }
}
