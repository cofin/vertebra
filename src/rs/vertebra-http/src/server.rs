//! Hyper/Tokio accept loop.
//!
//! Supports HTTP/1.1 (with pipeline_flush for throughput) and HTTP/2 via
//! `hyper_util::server::conn::auto::Builder`. When TLS is configured, ALPN
//! determines the protocol; without TLS, only HTTP/1.1.

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::collector::PendingRequest;
use crate::config::ServerConfig;
use crate::dispatcher::Dispatcher;
use crate::hmr::HmrState;
use crate::response::handle;
use crate::router::NativeRouter;

#[cfg(feature = "tls")]
use crate::tls::TlsAcceptor;

/// Shared state threaded through every request service call.
pub struct ServerState {
    pub router: Arc<NativeRouter>,
    pub dispatcher: Arc<Dispatcher>,
    pub config: Arc<ServerConfig>,
    pub collector_tx: tokio::sync::mpsc::Sender<PendingRequest>,
    pub hmr: Arc<HmrState>,
}

/// Run the HTTP server with a collector channel until the shutdown signal resolves.
pub async fn run_with_collector(
    config: Arc<ServerConfig>,
    router: Arc<NativeRouter>,
    dispatcher: Arc<Dispatcher>,
    collector_tx: tokio::sync::mpsc::Sender<PendingRequest>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(config.bind).await?;
    info!(addr = %config.bind, "vertebra-http listening");

    let state = Arc::new(ServerState {
        router: Arc::clone(&router),
        dispatcher: Arc::clone(&dispatcher),
        config: Arc::clone(&config),
        collector_tx,
        hmr: Arc::new(HmrState::default()),
    });

    #[cfg(feature = "tls")]
    let tls_acceptor: Option<TlsAcceptor> = config
        .tls
        .as_ref()
        .map(|tc| TlsAcceptor::from_config(tc))
        .transpose()?;

    #[cfg(not(feature = "tls"))]
    let tls_acceptor: Option<()> = None;

    let _ = &tls_acceptor; // suppress unused warning in non-tls builds

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let state = Arc::clone(&state);
                        #[cfg(feature = "tls")]
                        if let Some(ref tls_acc) = tls_acceptor {
                            let tls_acc = tls_acc.clone();
                            tokio::spawn(async move {
                                if let Err(e) = serve_tls(stream, peer, state, tls_acc).await {
                                    debug!(peer = %peer, error = %e, "TLS connection error");
                                }
                            });
                            continue;
                        }
                        tokio::spawn(async move {
                            if let Err(e) = serve_plain(stream, peer, state).await {
                                debug!(peer = %peer, error = %e, "HTTP connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "accept failed");
                    }
                }
            }
            _ = &mut shutdown => {
                info!("shutdown signal received, stopping accept loop");
                break;
            }
        }
    }

    Ok(())
}

async fn serve_plain(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!(peer = %peer, "plain TCP connection");
    let io = TokioIo::new(stream);
    let state = Arc::clone(&state);

    // Use HTTP/1 builder with pipeline_flush for improved throughput.
    // pipeline_flush flushes response bytes eagerly, reducing per-response
    // latency and improving keep-alive connection throughput.
    let mut builder = AutoBuilder::new(TokioExecutor::new());
    builder.http1().pipeline_flush(true);

    builder
        .serve_connection_with_upgrades(
            io,
            service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle(req, state).await }
            }),
        )
        .await?;
    Ok(())
}

#[cfg(feature = "tls")]
async fn serve_tls(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
    acceptor: TlsAcceptor,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!(peer = %peer, "TLS connection");
    let tls_stream = acceptor.accept(stream).await?;
    let io = TokioIo::new(tls_stream);
    let state = Arc::clone(&state);

    AutoBuilder::new(TokioExecutor::new())
        .serve_connection_with_upgrades(
            io,
            service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle(req, state).await }
            }),
        )
        .await?;
    Ok(())
}
