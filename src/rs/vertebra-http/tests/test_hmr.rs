//! Integration tests for the HMR control endpoints.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

use vertebra_http::{config::ServerConfig, dispatcher::Dispatcher, router::NativeRouter, server};

fn http_client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

async fn spawn_hmr_server(dev_mode: bool) -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let router = Arc::new(NativeRouter::from_entries(Vec::new()).unwrap());
    let dispatcher = Arc::new(Dispatcher::new());
    let (collector_tx, _collector_rx) = tokio::sync::mpsc::channel(4);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config = Arc::new(ServerConfig {
        bind: addr,
        dev_mode,
        ..ServerConfig::default()
    });

    tokio::spawn(async move {
        server::run_with_listener(
            listener,
            config,
            router,
            dispatcher,
            vec![collector_tx],
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    (addr, shutdown_tx)
}

#[tokio::test]
async fn test_hmr_status_endpoint_reports_state() {
    let (addr, _shutdown) = spawn_hmr_server(true).await;
    let client = http_client();

    let response = client
        .get(format!("http://{addr}/__vertebra/hmr"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["event"], "vertebra:types-changed");
    assert_eq!(body["generation"], 0);
    assert_eq!(body["connected_clients"], 0);
    assert_eq!(body["websocket"], true);
}

#[tokio::test]
async fn test_hmr_signal_endpoint_advances_generation() {
    let (addr, _shutdown) = spawn_hmr_server(true).await;
    let client = http_client();

    let response = client
        .post(format!("http://{addr}/__vertebra/hmr/signal"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let status: Value = client
        .get(format!("http://{addr}/__vertebra/hmr"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["generation"], 1);
}

#[tokio::test]
async fn test_hmr_websocket_receives_signal_events() {
    let (addr, _shutdown) = spawn_hmr_server(true).await;
    let (mut websocket, _) = connect_async(format!("ws://{addr}/__vertebra/hmr"))
        .await
        .unwrap();

    let client = http_client();
    let response = client
        .post(format!("http://{addr}/__vertebra/hmr/signal"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let next = timeout(Duration::from_secs(2), websocket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let payload: Value = serde_json::from_str(next.to_text().unwrap()).unwrap();
    assert_eq!(payload["event"], "vertebra:types-changed");
    assert_eq!(payload["generation"], 1);
}

#[tokio::test]
async fn test_hmr_endpoints_are_disabled_outside_dev_mode() {
    let (addr, _shutdown) = spawn_hmr_server(false).await;
    let client = http_client();

    let response = client
        .get(format!("http://{addr}/__vertebra/hmr"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}
