use crate::ServerState;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response};
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
struct StatusResponse {
    status: String,
    workers: usize,
    version: String,
}

pub async fn handle_status(_req: Request<hyper::body::Incoming>, state: Arc<ServerState>) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let status = StatusResponse {
        status: "ready".to_string(),
        workers: state.dispatcher.worker_count(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    
    let json = serde_json::to_string(&status)?;
    Ok(Response::builder()
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(json)))?)
}
