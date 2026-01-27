use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use futures_util::StreamExt;
use tokio_stream::wrappers::IntervalStream;
use http_body_util::{StreamBody, BodyExt};
use hyper::body::Frame;

use vertebra_broker::pool::PoolManager;
use vertebra_broker::scheduler::FairScheduler;

pub mod worker;
pub mod assets;
use worker::WorkerManager;
use assets::AssetManager;

async fn hello(req: Request<hyper::body::Incoming>) -> Result<Response<http_body_util::combinators::BoxBody<Bytes, Infallible>>, Infallible> {
    // gRPC Check
    if let Some(content_type) = req.headers().get("content-type") {
        if content_type.as_bytes().starts_with(b"application/grpc") {
             let body = Full::new(Bytes::from("")).map_err(|_: Infallible| unreachable!()).boxed();
             return Ok(Response::builder()
                .header("content-type", "application/grpc")
                .header("grpc-status", "12") // Unimplemented
                .body(body)
                .unwrap());
        }
    }

    // SSE Check
    if let Some(accept) = req.headers().get("accept") {
        let accept_str = std::str::from_utf8(accept.as_bytes()).unwrap_or("");
        if accept_str.contains("text/event-stream") {
            let interval = tokio::time::interval(std::time::Duration::from_secs(1));
            let stream = IntervalStream::new(interval)
                .map(|_| {
                    let event = format!("data: Ping\n\n");
                    Ok::<_, Infallible>(Frame::data(Bytes::from(event)))
                });
            
            let body = BodyExt::boxed(StreamBody::new(stream));
            
            return Ok(Response::builder()
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .body(body)
                .unwrap());
        }
    }

    let body = Full::new(Bytes::from("Hello from Vertebra Rust (HTTP/2 Enabled)"))
        .map_err(|_: Infallible| unreachable!())
        .boxed();
        
    Ok(Response::new(body))
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
    pub worker_manager: WorkerManager,
    pub asset_manager: AssetManager,
}

pub async fn serve(config: ServerConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr_str = format!("{}:{}", config.host, config.port);
    let addr: SocketAddr = addr_str.parse()?;

    let mut worker_manager = WorkerManager::new(config.workers, &config.app_path, &config.python_path);
    let pool_manager = Arc::new(PoolManager::new());
    let scheduler = Arc::new(FairScheduler::new(10.0));
    let asset_manager = AssetManager::new(".", None); // Serve current dir for now

    if let Err(e) = worker_manager.start() {
        eprintln!("WARNING: Failed to start workers: {}", e);
        // Continue serving even if workers fail (for testing/dev)
    }

    let state = Arc::new(ServerState {
        pool_manager,
        scheduler,
        worker_manager,
        asset_manager,
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
                async move {
                    // Try assets first (includes index.html)
                    let path = req.uri().path();
                    if path == "/" || path == "/index.html" || path.starts_with("/assets/") {
                         // Adapt response body type
                         let resp = state.asset_manager.serve(req).await;
                         match resp {
                             Ok(r) => Ok::<_, Infallible>(r.map(|b| b.map_err(|_| unreachable!()).boxed())),
                             Err(_) => {
                                 // Return 500 if asset serving failed internally
                                 let body = Full::new(Bytes::from("Internal Server Error"))
                                     .map_err(|_: Infallible| unreachable!()).boxed();
                                 Ok(Response::builder().status(500).body(body).unwrap())
                             }
                         }
                    } else {
                        hello(req).await
                    }
                }
            });

            if let Err(err) = builder.serve_connection(io, service).await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }

    Ok(())
}
