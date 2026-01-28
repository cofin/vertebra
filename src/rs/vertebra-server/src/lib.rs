use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

use vertebra_broker::pool::PoolManager;
use vertebra_broker::scheduler::FairScheduler;

pub mod assets;
pub mod bridge;
pub mod dispatcher;
pub mod introspection;
pub mod multipart;
pub mod response_router;
pub mod router;
pub mod worker;
use assets::AssetManager;
use bridge::dispatch;
use dispatcher::{Dispatcher, LeastPendingDispatcher};
use introspection::handle_status;
use multipart::handle_multipart;
use response_router::spawn_response_router;
use router::Router;
use worker::WorkerManager;

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    state: Arc<ServerState>,
) -> Result<Response<http_body_util::combinators::BoxBody<Bytes, Infallible>>, Infallible> {
    let path = req.uri().path();

    // 1. Native Router
    if let Ok(matched) = state.router.lookup(path) {
        let body = Full::new(Bytes::from(format!(
            "Matched native route: {}",
            matched.value
        )))
        .map_err(|_: Infallible| unreachable!())
        .boxed();
        return Ok(Response::new(body));
    }

    // 2. Introspection
    if path == "/internal/status" {
        match handle_status(req, state.clone()).await {
            Ok(r) => return Ok(r.map(|b| b.map_err(|_| unreachable!()).boxed())),
            Err(_) => {
                let body = Full::new(Bytes::from("Internal Server Error"))
                    .map_err(|_: Infallible| unreachable!())
                    .boxed();
                return Ok(Response::builder().status(500).body(body).unwrap());
            }
        }
    }

    // 3. Multipart Upload
    if let Some(ct) = req.headers().get("content-type") {
        if ct.as_bytes().starts_with(b"multipart/form-data") {
            match handle_multipart(req, state.clone()).await {
                Ok(r) => return Ok(r.map(|b| b.map_err(|_| unreachable!()).boxed())),
                Err(e) => {
                    let body = Full::new(Bytes::from(format!("Upload Error: {}", e)))
                        .map_err(|_: Infallible| unreachable!())
                        .boxed();
                    return Ok(Response::builder().status(500).body(body).unwrap());
                }
            }
        }
    }

    // 4. Assets (including index.html)
    if path == "/" || path == "/index.html" || path.starts_with("/assets/") {
        let resp = state.asset_manager.serve(req).await;
        match resp {
            Ok(r) => return Ok(r.map(|b| b.map_err(|_| unreachable!()).boxed())),
            Err(_) => {
                let body = Full::new(Bytes::from("Internal Server Error"))
                    .map_err(|_: Infallible| unreachable!())
                    .boxed();
                return Ok(Response::builder().status(500).body(body).unwrap());
            }
        }
    }

    // 5. IPC Dispatch (Default)
    if let Some(worker) = state.dispatcher.get_worker() {
        match dispatch(req, worker).await {
            Ok(r) => return Ok(r.map(|b| b.map_err(|_| unreachable!()).boxed())),
            Err(e) => {
                let body = Full::new(Bytes::from(format!("IPC Error: {}", e)))
                    .map_err(|_: Infallible| unreachable!())
                    .boxed();
                return Ok(Response::builder().status(502).body(body).unwrap());
            }
        }
    } else {
        let body = Full::new(Bytes::from("No workers available"))
            .map_err(|_: Infallible| unreachable!())
            .boxed();
        return Ok(Response::builder().status(503).body(body).unwrap());
    }
}

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub app_path: String,
    pub python_path: String,
    pub workers: usize,
}

pub struct ServerState {
    pub pool_manager: Arc<PoolManager>,
    pub scheduler: Arc<FairScheduler<()>>,
    pub asset_manager: AssetManager,
    pub router: Router,
    pub dispatcher: Arc<dyn Dispatcher>,
}

pub async fn serve(config: ServerConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr_str = format!("{}:{}", config.host, config.port);
    let addr: SocketAddr = addr_str.parse()?;

    let mut worker_manager =
        WorkerManager::new(config.workers, &config.app_path, &config.python_path);
    let pool_manager = Arc::new(PoolManager::new());
    let scheduler = Arc::new(FairScheduler::new(10.0));
    let asset_manager = AssetManager::new(".", None);

    let mut router = Router::new();
    router.insert("/api/native", "native_handler".to_string())?;

    if let Err(e) = worker_manager.start() {
        eprintln!("WARNING: Failed to start workers: {}", e);
    }

    let dispatcher = Arc::new(LeastPendingDispatcher::new(worker_manager.into_workers()));

    // Spawn response router tasks for each worker
    for worker in dispatcher.workers() {
        spawn_response_router(worker.clone());
    }
    println!(
        "Spawned {} response router tasks",
        dispatcher.worker_count()
    );

    let state = Arc::new(ServerState {
        pool_manager,
        scheduler,
        asset_manager,
        router,
        dispatcher,
    });

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);

    loop {
        let (stream, _) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Accept error: {}", e);
                        continue;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nShutdown signal received.");
                break;
            }
        };

        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::task::spawn(async move {
            let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());

            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                let state = state.clone();
                handle_request(req, state)
            });

            if let Err(err) = builder.serve_connection(io, service).await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }

    Ok(())
}
