//! Adaptive direct dispatch worker.
//!
//! Receives batches from the direct dispatch channel and processes them with
//! an adaptive strategy that balances latency and throughput based on load:
//!
//! - **Low load**: process immediately (minimize latency)
//! - **High load**: accumulate more requests via `try_recv` before processing
//!   (amortize per-batch overhead, maximize throughput)
//!
//! The adaptation uses an EWMA (exponentially weighted moving average) of
//! recent batch sizes to detect load levels, then adjusts the accumulation
//! window accordingly.

use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::collector::PendingResponse;
use crate::dispatcher::DirectBatch;

/// Configuration for the adaptive direct worker.
#[derive(Debug, Clone)]
pub struct AdaptiveWorkerConfig {
    /// Minimum requests before processing (low-load floor).
    pub min_batch: usize,
    /// Maximum requests to accumulate (high-load ceiling).
    pub max_batch: usize,
    /// Maximum time to wait for more requests before flushing.
    pub max_linger: Duration,
    /// EWMA smoothing factor (0.0–1.0). Higher = more responsive to load changes.
    pub ewma_alpha: f64,
    /// Load threshold: when EWMA batch size exceeds this, switch to accumulation mode.
    pub load_threshold: f64,
}

impl Default for AdaptiveWorkerConfig {
    fn default() -> Self {
        Self {
            min_batch: 1,
            max_batch: 128,
            max_linger: Duration::from_micros(100),
            ewma_alpha: 0.3,
            load_threshold: 4.0,
        }
    }
}

/// Handler function signature for processing a batch of direct requests.
///
/// Called with accumulated requests. Each request carries its own oneshot
/// reply channel — the handler must send a `PendingResponse` through it.
pub type BatchHandler = Box<dyn Fn(Vec<crate::dispatcher::DirectRequest>) + Send + 'static>;

/// Run the adaptive direct dispatch worker loop.
///
/// This is the main entry point. It receives `DirectBatch` values from the
/// dispatcher channel and feeds them through the adaptive accumulation
/// strategy before calling the handler.
pub async fn run_adaptive_worker(
    mut rx: mpsc::Receiver<DirectBatch>,
    config: AdaptiveWorkerConfig,
    handler: BatchHandler,
) {
    let mut ewma_batch_size: f64 = 1.0;
    let mut accumulated: Vec<crate::dispatcher::DirectRequest> =
        Vec::with_capacity(config.max_batch);

    loop {
        // Wait for the first batch (blocking — no busy loop when idle).
        let first = match rx.recv().await {
            Some(batch) => batch,
            None => break, // Channel closed, shut down.
        };

        accumulated.extend(first.requests);

        // Phase 1: Eager drain — always try_recv to capture already-buffered requests.
        // This is cheap (no syscall) and catches bursts regardless of EWMA state.
        while accumulated.len() < config.max_batch {
            match rx.try_recv() {
                Ok(batch) => accumulated.extend(batch.requests),
                Err(_) => break,
            }
        }

        // Phase 2: Adaptive linger — under high load, wait briefly for more arrivals.
        // Skip linger if any request has the PRIORITY flag (low-latency path).
        let has_priority = accumulated
            .iter()
            .any(|r| r.flags & vertebra_core::protocol::flags::PRIORITY != 0);
        let target = compute_target_batch_size(&config, ewma_batch_size);
        if !has_priority && accumulated.len() < target && ewma_batch_size >= config.load_threshold {
            let deadline = Instant::now() + config.max_linger;
            while accumulated.len() < target && Instant::now() < deadline {
                tokio::task::yield_now().await;
                match rx.try_recv() {
                    Ok(batch) => accumulated.extend(batch.requests),
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }
        }

        // Update EWMA with observed batch size.
        let batch_size = accumulated.len();
        ewma_batch_size =
            config.ewma_alpha * batch_size as f64 + (1.0 - config.ewma_alpha) * ewma_batch_size;

        // Process the accumulated batch.
        let batch = std::mem::replace(&mut accumulated, Vec::with_capacity(config.max_batch));
        handler(batch);
    }
}

/// Compute the target batch size based on current load.
///
/// This is the core adaptive decision:
/// - Below `load_threshold`: target = min_batch (process immediately)
/// - Above `load_threshold`: target scales linearly up to max_batch
///
/// The scaling factor uses the ratio of current EWMA to the threshold,
/// mapping it onto the [min_batch, max_batch] range.
fn compute_target_batch_size(config: &AdaptiveWorkerConfig, ewma: f64) -> usize {
    if ewma < config.load_threshold {
        // Low load: process immediately.
        config.min_batch
    } else {
        // High load: scale target linearly.
        // ratio goes from 1.0 (at threshold) to higher values under load.
        let ratio = (ewma / config.load_threshold).min(config.max_batch as f64);
        let scaled = config.min_batch as f64
            + (config.max_batch - config.min_batch) as f64
                * ((ratio - 1.0) / (config.max_batch as f64 / config.load_threshold - 1.0))
                    .min(1.0);
        (scaled as usize).clamp(config.min_batch, config.max_batch)
    }
}

/// Convenience: spawn an adaptive worker that responds to each request with
/// a simple handler function.
///
/// The `handler_fn` receives a single `DirectRequest` and returns a `PendingResponse`.
/// This is the simplest way to use the adaptive worker.
pub async fn run_adaptive_worker_simple<F>(
    rx: mpsc::Receiver<DirectBatch>,
    config: AdaptiveWorkerConfig,
    handler_fn: F,
) where
    F: Fn(&crate::dispatcher::DirectRequest) -> PendingResponse + Send + 'static,
{
    run_adaptive_worker(
        rx,
        config,
        Box::new(move |requests| {
            for req in requests {
                let response = handler_fn(&req);
                // F&F requests have no reply channel — just drop the response.
                if let Some(reply) = req.reply {
                    let _ = reply.send(response);
                }
            }
        }),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::DirectRequest;
    use bytes::Bytes;

    #[test]
    fn compute_target_low_load() {
        let config = AdaptiveWorkerConfig::default();
        // EWMA below threshold → min_batch
        assert_eq!(compute_target_batch_size(&config, 1.0), config.min_batch);
        assert_eq!(compute_target_batch_size(&config, 3.9), config.min_batch);
    }

    #[test]
    fn compute_target_high_load() {
        let config = AdaptiveWorkerConfig {
            min_batch: 1,
            max_batch: 64,
            load_threshold: 4.0,
            ..Default::default()
        };
        // EWMA well above threshold → approaches max_batch
        let target = compute_target_batch_size(&config, 100.0);
        assert!(target > config.min_batch);
        assert!(target <= config.max_batch);
    }

    #[test]
    fn compute_target_at_threshold() {
        let config = AdaptiveWorkerConfig {
            min_batch: 1,
            max_batch: 64,
            load_threshold: 4.0,
            ..Default::default()
        };
        // At exactly the threshold → should be close to min_batch
        let target = compute_target_batch_size(&config, 4.0);
        assert_eq!(target, config.min_batch);
    }

    #[tokio::test]
    async fn adaptive_worker_processes_requests() {
        let (tx, rx) = mpsc::channel(64);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        let config = AdaptiveWorkerConfig {
            min_batch: 1,
            max_batch: 16,
            max_linger: Duration::from_micros(50),
            ..Default::default()
        };

        // Spawn worker
        tokio::spawn(run_adaptive_worker_simple(rx, config, |_req| {
            PendingResponse {
                status: 200,
                headers_mp: Bytes::new(),
                body: Bytes::from_static(b"ok"),
            }
        }));

        // Send a request
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        tx.send(DirectBatch {
            requests: vec![DirectRequest {
                handler_id: 1,
                method_tag: 0,
                http_version_tag: 1,
                path: "/test".to_owned(),
                query: None,
                headers_mp: Bytes::new(),
                body: Bytes::new(),
                flags: 0,
                reply: Some(reply_tx),
            }],
        })
        .await
        .unwrap();

        let resp = tokio::time::timeout(Duration::from_millis(100), reply_rx)
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_ref(), b"ok");

        drop(tx);
        drop(done_tx);
        let _ = done_rx.await;
    }

    #[tokio::test]
    async fn adaptive_worker_accumulates_under_load() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let (tx, rx) = mpsc::channel(256);
        let batch_sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let batch_sizes_clone = Arc::clone(&batch_sizes);
        let processed = Arc::new(AtomicUsize::new(0));
        let processed_clone = Arc::clone(&processed);

        let config = AdaptiveWorkerConfig {
            min_batch: 1,
            max_batch: 64,
            max_linger: Duration::from_millis(5),
            ewma_alpha: 0.5,
            load_threshold: 2.0,
        };

        // Pre-fill channel with 100 requests BEFORE starting the worker.
        // This ensures the channel has buffered messages for try_recv to find.
        let mut reply_rxs = Vec::new();
        for i in 0..100u64 {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            tx.send(DirectBatch {
                requests: vec![DirectRequest {
                    handler_id: i,
                    method_tag: 0,
                    http_version_tag: 1,
                    path: "/flood".to_owned(),
                    query: None,
                    headers_mp: Bytes::new(),
                    body: Bytes::new(),
                    flags: 0,
                reply: Some(reply_tx),
                }],
            })
            .await
            .unwrap();
            reply_rxs.push(reply_rx);
        }

        // Now start the worker — it will find all 100 messages already buffered.
        tokio::spawn(run_adaptive_worker(
            rx,
            config,
            Box::new(move |requests| {
                let count = requests.len();
                batch_sizes_clone.lock().unwrap().push(count);
                for req in requests {
                    if let Some(reply) = req.reply {
                        let _ = reply.send(PendingResponse {
                            status: 200,
                            headers_mp: Bytes::new(),
                            body: Bytes::from_static(b"ok"),
                        });
                    }
                    processed_clone.fetch_add(1, Ordering::Relaxed);
                }
            }),
        ));

        // Wait for all responses
        for rx in reply_rxs {
            let _ = tokio::time::timeout(Duration::from_secs(1), rx).await;
        }

        assert_eq!(processed.load(Ordering::Relaxed), 100);

        // With 100 pre-buffered messages, the worker should accumulate batches
        // (i.e., process fewer than 100 handler calls for 100 requests).
        let sizes = batch_sizes.lock().unwrap();
        let total_batches = sizes.len();
        assert!(
            total_batches < 100,
            "adaptive worker should accumulate under load: {} batches for 100 requests",
            total_batches
        );
    }
}
