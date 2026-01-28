use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request, Response};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use vertebra_core::protocol::{flags, RequestHeader, ResponseHeader};

use crate::worker::WorkerConnection;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AsgiRequest {
    id: String,
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
    scheme: String,
    /// HTTP version (e.g., "1.1", "2")
    http_version: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AsgiResponse {
    id: String,
    status: u16,
    headers: Vec<(String, String)>,
    #[serde(with = "serde_bytes", default)]
    body: Vec<u8>,
    path: Option<String>,
    offset: Option<u64>,
    count: Option<u64>,
}

/// Request timeout in seconds
const REQUEST_TIMEOUT_SECS: u64 = 30;

/// Dispatch a request to a Python worker using correlation-based routing.
///
/// Multiple requests can be dispatched to the same worker concurrently.
/// Each request gets a unique correlation ID, and responses are routed
/// back via the response router task.
///
/// The SpscSender is protected by a tokio Mutex to enforce the SPSC
/// single-producer invariant. The critical section is minimal (just memcpy).
pub async fn dispatch(
    req: Request<hyper::body::Incoming>,
    worker: Arc<WorkerConnection>,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let (parts, body) = req.into_parts();
    let body_bytes = body.collect().await?.to_bytes();

    let uuid = uuid::Uuid::new_v4().to_string();

    let http_version = match parts.version {
        hyper::Version::HTTP_09 => "0.9",
        hyper::Version::HTTP_10 => "1.0",
        hyper::Version::HTTP_11 => "1.1",
        hyper::Version::HTTP_2 => "2",
        hyper::Version::HTTP_3 => "3",
        _ => "1.1",
    }
    .to_string();

    let asgi_req = AsgiRequest {
        id: uuid.clone(),
        method: parts.method.as_str().to_string(),
        path: parts.uri.path().to_string(),
        query: parts.uri.query().unwrap_or("").to_string(),
        headers: parts
            .headers
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    v.to_str().unwrap_or("").to_string(),
                )
            })
            .collect(),
        body: body_bytes.to_vec(),
        scheme: "http".to_string(),
        http_version,
    };

    let payload = rmp_serde::to_vec_named(&asgi_req)?;

    // Generate unique correlation ID for this request
    let request_id = worker.next_request_id();

    let header = RequestHeader::new(request_id, flags::NONE, payload.len() as u32);

    let mut message = Vec::with_capacity(RequestHeader::SIZE + payload.len());
    message.extend_from_slice(&header.to_bytes());
    message.extend_from_slice(&payload);

    // Increment pending count BEFORE send (for accurate load balancing)
    worker.increment_pending();

    // Create oneshot channel for response
    let (tx, rx) = oneshot::channel();

    // Register in pending map BEFORE sending (prevents race condition)
    worker.pending.insert(request_id, tx);

    // Send request to Python worker (mutex-protected for SPSC safety)
    if let Err(e) = worker.send_request(&message, 0).await {
        // Cleanup on send failure
        worker.pending.remove(&request_id);
        worker.decrement_pending();
        return Err(format!("Failed to send request: {}", e).into());
    }

    // Notify Python that a request is available
    if let Err(e) = worker.req_notifier.notify() {
        worker.pending.remove(&request_id);
        worker.decrement_pending();
        return Err(format!("Failed to notify worker: {}", e).into());
    }

    // Wait for response via oneshot channel (with timeout)
    let timeout = Duration::from_secs(REQUEST_TIMEOUT_SECS);
    let response_data = match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(data)) => {
            // Success - decrement pending
            worker.decrement_pending();
            data
        }
        Ok(Err(_)) => {
            // Oneshot sender was dropped (worker crashed or router error)
            worker.decrement_pending();
            return Err("Worker connection lost".into());
        }
        Err(_) => {
            // Timeout - cleanup
            worker.pending.remove(&request_id);
            worker.decrement_pending();
            return Err(format!(
                "Timeout waiting for response ({}s)",
                REQUEST_TIMEOUT_SECS
            )
            .into());
        }
    };

    // Parse response
    if response_data.len() < ResponseHeader::SIZE {
        return Ok(Response::builder()
            .status(500)
            .body(Full::new(Bytes::from("Invalid response length from worker")))?);
    }

    let payload = &response_data[ResponseHeader::SIZE..];
    let asgi_resp: AsgiResponse = rmp_serde::from_slice(payload)?;

    let mut builder = Response::builder().status(asgi_resp.status);
    for (k, v) in asgi_resp.headers {
        builder = builder.header(k, v);
    }

    Ok(builder.body(Full::new(Bytes::from(asgi_resp.body)))?)
}
