use hyper::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use vertebra_core::protocol::{RequestHeader, flags};
use crate::worker::Worker;

#[derive(Serialize)]
struct AsgiRequest {
    id: String,
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
    scheme: String,
}

#[derive(Deserialize)]
struct AsgiResponse {
    id: String,
    status: u16,
    headers: Vec<(String, String)>,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
}

pub async fn dispatch(
    req: Request<hyper::body::Incoming>,
    worker: Arc<Mutex<Worker>>
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let (parts, body) = req.into_parts();
    let body_bytes = body.collect().await?.to_bytes();
    
    let id = uuid::Uuid::new_v4().to_string();
    
    let asgi_req = AsgiRequest {
        id: id.clone(),
        method: parts.method.as_str().to_string(),
        path: parts.uri.path().to_string(),
        query: parts.uri.query().unwrap_or("").to_string(),
        headers: parts.headers.iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect(),
        body: body_bytes.to_vec(),
        scheme: "http".to_string(),
    };
    
    let payload = rmp_serde::to_vec(&asgi_req)?;
    
    let header = RequestHeader::new(
        0, 
        flags::NONE,
        payload.len() as u32
    );
    
    let mut message = Vec::with_capacity(RequestHeader::SIZE + payload.len());
    message.extend_from_slice(&header.to_bytes());
    message.extend_from_slice(&payload);
    
    let mut worker = worker.lock().await;
    
use vertebra_core::channel::ChannelError;

// ...

    // Send
    worker.req_tx.send(&message, 0)?;
    worker.notifier.notify()?;
    
    // Receive (Manual spin with debug)
    let mut resp_buffer = vec![0u8; 65536];
    let len = tokio::task::block_in_place(|| -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut retries = 0;
        loop {
            match worker.resp_rx.try_recv(&mut resp_buffer) {
                Ok(n) => return Ok(n),
                Err(ChannelError::Empty) => {
                    retries += 1;
                    if retries % 10000000 == 0 {
                        println!("DEBUG: Waiting for response... {}", retries);
                    }
                    std::thread::yield_now();
                    if retries > 200000000 { // ~few seconds
                        return Err("Timeout waiting for Python response".into());
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    })?;
    
    if len < RequestHeader::SIZE {
        return Ok(Response::builder().status(500).body(Full::new(Bytes::from("Invalid response length from worker")))?);
    }
    
    let payload = &resp_buffer[RequestHeader::SIZE..len];
    
    let asgi_resp: AsgiResponse = rmp_serde::from_slice(payload)?;
    
    let mut builder = Response::builder().status(asgi_resp.status);
    for (k, v) in asgi_resp.headers {
        builder = builder.header(k, v);
    }
    
    Ok(builder.body(Full::new(Bytes::from(asgi_resp.body)))?)
}
