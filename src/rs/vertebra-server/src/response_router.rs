//! Response router for correlation-based request/response matching.
//!
//! This module spawns a background task per worker that reads responses from
//! the IPC ring buffer and routes them to the correct waiter via correlation IDs.

use std::sync::Arc;
use vertebra_core::channel::ChannelError;
use vertebra_core::protocol::ResponseHeader;

use crate::worker::WorkerConnection;

/// Spawn a response router task for a worker.
///
/// The router continuously reads responses from the worker's response ring
/// and dispatches them to waiters via the correlation map.
pub fn spawn_response_router(worker: Arc<WorkerConnection>) {
    tokio::spawn(async move {
        if let Err(e) = response_router_loop(&worker).await {
            eprintln!(
                "Response router for worker {} exited with error: {}",
                worker.pid, e
            );
        }
    });
}

/// Main response router loop.
///
/// Reads responses from the worker and routes them to the correct oneshot sender
/// based on the correlation ID in the response header.
async fn response_router_loop(
    worker: &WorkerConnection,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = vec![0u8; 65536];

    loop {
        // Wait for Python to signal response is ready
        worker.resp_notifier.wait_readable().await?;
        worker.resp_notifier.clear()?;

        // Drain all available responses
        loop {
            match worker.resp_rx.try_recv(&mut buffer) {
                Ok(len) => {
                    if len < ResponseHeader::SIZE {
                        eprintln!("Response too short: {} bytes", len);
                        continue;
                    }

                    // Extract correlation ID from header
                    let header = match ResponseHeader::from_bytes(&buffer[..len]) {
                        Some(h) => h,
                        None => {
                            eprintln!("Failed to parse response header");
                            continue;
                        }
                    };

                    let request_id = header.id;

                    // Route to waiter
                    if let Some((_, tx)) = worker.pending.remove(&request_id) {
                        // Copy response data for the waiter
                        let response_data = buffer[..len].to_vec();
                        let _ = tx.send(response_data);
                    } else {
                        eprintln!(
                            "No waiter for response with id={} (orphaned response)",
                            request_id
                        );
                    }
                }
                Err(ChannelError::Empty) => {
                    // No more responses available, wait for next notification
                    break;
                }
                Err(e) => {
                    return Err(format!("Response receive error: {}", e).into());
                }
            }
        }
    }
}
