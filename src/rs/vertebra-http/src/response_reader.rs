//! Async response reader for SharedMemory workers.
//!
//! Registers each worker's response notification eventfd/pipe with Tokio's reactor
//! via `AsyncFd`, achieving zero-poll wakeup — the kernel notifies Tokio directly
//! when a worker writes a response and signals the eventfd.
//!
//! On each wakeup, drains all available responses from the tiered response rings
//! and completes the corresponding correlations in the dispatcher.

use std::sync::Arc;

#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

#[cfg(unix)]
use tokio::io::unix::AsyncFd;

use vertebra_core::transport::ring_pair::RingPair;

use crate::collector::PendingResponse;
use crate::dispatcher::Dispatcher;

/// Wrapper around a raw FD for `AsyncFd` registration.
///
/// Does NOT own the FD — the `RingPair` owns it. This is just a handle
/// for Tokio's reactor to poll on.
#[cfg(unix)]
struct NotifierFd {
    fd: RawFd,
}

#[cfg(unix)]
impl AsRawFd for NotifierFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

/// Run the response reader loop with AsyncFd-based wakeup.
///
/// Each worker's response notification FD is registered with Tokio's reactor.
/// When a worker signals (via eventfd write), the reactor wakes this task,
/// which then drains all responses from that worker's tiered response rings.
#[cfg(unix)]
pub async fn run_response_reader(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) {
    if workers.is_empty() {
        return;
    }

    debug!(
        workers = workers.len(),
        "response reader started (AsyncFd mode)"
    );

    // Register each worker's response notification FD with Tokio
    let mut async_fds: Vec<(usize, AsyncFd<NotifierFd>)> = Vec::with_capacity(workers.len());
    for (i, ring_pair) in workers.iter().enumerate() {
        let fd = ring_pair.response_notifier.as_raw_fd();
        match AsyncFd::new(NotifierFd { fd }) {
            Ok(afd) => async_fds.push((i, afd)),
            Err(e) => {
                error!(worker_id = i, error = %e, "failed to register response notifier with Tokio");
            }
        }
    }

    if async_fds.is_empty() {
        error!("no response notifiers registered, falling back to polling");
        run_response_reader_polling(workers, dispatcher, shutdown).await;
        return;
    }

    // Main event loop: wait for any worker's eventfd to become readable
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // Use tokio::select! to wait on all AsyncFds simultaneously
        // For simplicity and to avoid macro explosion with dynamic worker counts,
        // we poll in a tight loop with yield points.
        let mut any_received = false;

        for (worker_id, async_fd) in &async_fds {
            // Non-blocking check: is this FD readable?
            match async_fd.readable().await {
                Ok(mut guard) => {
                    // Clear the notification
                    let _ = workers[*worker_id].response_notifier.clear();
                    guard.clear_ready();

                    // Drain all responses from this worker
                    loop {
                        match workers[*worker_id].response_rings.try_recv_response() {
                            Ok(Some(resp)) => {
                                any_received = true;
                                dispatcher.complete(
                                    resp.correlation_id,
                                    PendingResponse {
                                        status: resp.status,
                                        headers_mp: bytes::Bytes::from(resp.headers_raw),
                                        body: bytes::Bytes::from(resp.body),
                                    },
                                );
                            }
                            Ok(None) => break,
                            Err(e) => {
                                warn!(worker_id, error = %e, "error reading response ring");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(worker_id, error = %e, "AsyncFd readable error");
                }
            }

            // Check shutdown between workers
            if shutdown.is_cancelled() {
                break;
            }
        }

        if !any_received {
            // Yield to prevent busy-spinning when using sequential AsyncFd polling
            tokio::task::yield_now().await;
        }
    }

    // Final drain
    drain_all_responses(&workers, &dispatcher);
    debug!("response reader stopped");
}

/// Fallback polling-based response reader (used when AsyncFd fails).
#[cfg(unix)]
async fn run_response_reader_polling(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) {
    use std::time::Duration;

    debug!("response reader running in polling mode");
    let poll_interval = Duration::from_micros(50);

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let mut any_received = false;
        for (worker_id, ring_pair) in workers.iter().enumerate() {
            loop {
                match ring_pair.response_rings.try_recv_response() {
                    Ok(Some(resp)) => {
                        any_received = true;
                        let _ = ring_pair.response_notifier.clear();
                        dispatcher.complete(
                            resp.correlation_id,
                            PendingResponse {
                                status: resp.status,
                                headers_mp: bytes::Bytes::from(resp.headers_raw),
                                body: bytes::Bytes::from(resp.body),
                            },
                        );
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(worker_id, error = %e, "error reading response ring");
                        break;
                    }
                }
            }
        }

        if !any_received {
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {}
                _ = shutdown.cancelled() => break,
            }
        }
    }

    drain_all_responses(&workers, &dispatcher);
}

/// Non-Unix fallback: polling-only mode.
#[cfg(not(unix))]
pub async fn run_response_reader(
    workers: Vec<Arc<RingPair>>,
    dispatcher: Arc<Dispatcher>,
    shutdown: CancellationToken,
) {
    use std::time::Duration;

    if workers.is_empty() {
        return;
    }

    debug!(
        workers = workers.len(),
        "response reader started (polling mode)"
    );
    let poll_interval = Duration::from_micros(100);

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let mut any_received = false;
        for ring_pair in &workers {
            while let Ok(Some(resp)) = ring_pair.response_rings.try_recv_response() {
                any_received = true;
                dispatcher.complete(
                    resp.correlation_id,
                    PendingResponse {
                        status: resp.status,
                        headers_mp: bytes::Bytes::from(resp.headers_raw),
                        body: bytes::Bytes::from(resp.body),
                    },
                );
            }
        }

        if !any_received {
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {}
                _ = shutdown.cancelled() => break,
            }
        }
    }

    drain_all_responses(&workers, &dispatcher);
}

/// Drain any remaining responses from all workers.
fn drain_all_responses(workers: &[Arc<RingPair>], dispatcher: &Dispatcher) {
    for ring_pair in workers {
        while let Ok(Some(resp)) = ring_pair.response_rings.try_recv_response() {
            dispatcher.complete(
                resp.correlation_id,
                PendingResponse {
                    status: resp.status,
                    headers_mp: bytes::Bytes::from(resp.headers_raw),
                    body: bytes::Bytes::from(resp.body),
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    #[tokio::test]
    async fn response_reader_completes_correlation() {
        let pair = Arc::new(RingPair::create(RingPairConfig::default()).unwrap());
        let dispatcher = Arc::new(Dispatcher::new_shm(vec![pair.clone()], 1024));

        // Dispatch a request
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let batch = crate::collector::Batch {
            requests: vec![crate::collector::PendingRequest {
                handler_id: 1,
                method_tag: 0,
                http_version_tag: 1,
                flags: 0,
                path: "/test".to_owned(),
                query: None,
                headers_mp: bytes::Bytes::new(),
                body: bytes::Bytes::new(),
                reply: reply_tx,
            }],
        };
        dispatcher.dispatch(batch);

        // Read request from ring, get correlation ID
        let mut buf = vec![0u8; pair.request_ring.payload_capacity()];
        let len = pair.request_ring.try_recv(&mut buf).expect("recv");
        let snap = vertebra_core::transport::BatchSnapshot::deserialize_from(&buf[..len]).unwrap();
        let cid = snap.node(0).correlation_id;

        // Simulate worker response
        pair.response_rings
            .send_response(cid, 200, b"", b"hello from worker")
            .expect("send response");
        pair.response_notifier.notify().expect("notify");

        // Start response reader
        let shutdown = CancellationToken::new();
        let reader = tokio::spawn(run_response_reader(
            vec![pair.clone()],
            Arc::clone(&dispatcher),
            shutdown.clone(),
        ));

        // Wait for response
        let resp = tokio::time::timeout(Duration::from_secs(2), reply_rx)
            .await
            .expect("timeout")
            .expect("should receive response");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_ref(), b"hello from worker");

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), reader).await;
    }

    #[tokio::test]
    async fn response_reader_handles_multiple_workers() {
        let pairs: Vec<Arc<RingPair>> = (0..3)
            .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
            .collect();

        let dispatcher = Arc::new(Dispatcher::new_shm(pairs.clone(), 1024));

        // Dispatch 3 requests
        let mut reply_rxs = Vec::new();
        for i in 0..3u64 {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            dispatcher.dispatch(crate::collector::Batch {
                requests: vec![crate::collector::PendingRequest {
                    handler_id: i,
                    method_tag: 0,
                    http_version_tag: 1,
                    flags: 0,
                    path: format!("/w/{i}"),
                    query: None,
                    headers_mp: bytes::Bytes::new(),
                    body: bytes::Bytes::new(),
                    reply: reply_tx,
                }],
            });
            reply_rxs.push(reply_rx);
        }

        // Simulate workers responding
        for pair in &pairs {
            let mut buf = vec![0u8; pair.request_ring.payload_capacity()];
            if let Ok(len) = pair.request_ring.try_recv(&mut buf) {
                let snap =
                    vertebra_core::transport::BatchSnapshot::deserialize_from(&buf[..len]).unwrap();
                let cid = snap.node(0).correlation_id;
                pair.response_rings
                    .send_response(cid, 200, b"", b"ok")
                    .expect("send");
                pair.response_notifier.notify().expect("notify");
            }
        }

        // Start reader
        let shutdown = CancellationToken::new();
        let reader = tokio::spawn(run_response_reader(
            pairs.clone(),
            Arc::clone(&dispatcher),
            shutdown.clone(),
        ));

        // All 3 responses should arrive
        for rx in reply_rxs {
            let resp = tokio::time::timeout(Duration::from_secs(2), rx)
                .await
                .expect("timeout")
                .expect("recv");
            assert_eq!(resp.status, 200);
        }

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), reader).await;
    }
}
