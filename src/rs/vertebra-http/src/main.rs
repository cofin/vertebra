//! `vertebra-server` binary entrypoint.
//!
//! In Chapter 5 the route table is hardcoded to `/hello` and `/echo` for
//! bootstrap testing. The real route registration from Python's `VertebraApp`
//! is wired in Chapter 6 (FFI Bridge).

use std::sync::Arc;

use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

use vertebra_http::{
    collector::{BatchCollector, PendingResponse},
    config::ServerConfig,
    dispatcher::Dispatcher,
    policy::DrainPolicy,
    route_table::RouteTable,
    router::NativeRouter,
    server,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = Arc::new(ServerConfig::default());

    // --- Route table (hardcoded for Chapter 5 bootstrap) ---
    let mut table = RouteTable::new();
    let hello_id = table.insert("GET", "/hello");
    let echo_id = table.insert("POST", "/echo");
    info!(hello_id, echo_id, "route table built");

    let router = Arc::new(NativeRouter::from_entries(table.into_entries())?);

    // --- Collector + Dispatcher wiring ---
    let (dispatcher, mut ring_rx) = Dispatcher::new(256);
    let dispatcher = Arc::new(dispatcher);

    let (collector, collector_tx) = BatchCollector::new(
        Arc::clone(&dispatcher),
        DrainPolicy::default_fixed(),
        1024,
    );

    // Spawn the collector loop.
    tokio::spawn(collector.run());

    // Spawn the stub ring-reader (Chapter 6 replaces this with real Python FFI).
    let dispatcher_clone = Arc::clone(&dispatcher);
    tokio::spawn(async move {
        while let Some(batch) = ring_rx.recv().await {
            for id in batch.correlation_ids {
                dispatcher_clone.complete(
                    id,
                    PendingResponse {
                        status: 200,
                        headers_mp: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"ok"),
                    },
                );
            }
        }
    });

    // --- Start server ---
    let shutdown = async {
        signal::ctrl_c().await.expect("failed to listen for ctrl-c");
        info!("ctrl-c received");
    };

    server::run_with_collector(config, router, dispatcher, collector_tx, shutdown).await?;

    Ok(())
}
