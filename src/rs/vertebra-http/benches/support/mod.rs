//! Adaptive direct dispatch worker for benchmarks and integration testing.

use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use vertebra_http::batcher::{Batch, PendingRequest};

/// Configuration for the adaptive direct worker.
#[derive(Debug, Clone)]
pub struct AdaptiveWorkerConfig {
    pub min_batch: usize,
    pub max_batch: usize,
    pub max_linger: Duration,
    pub ewma_alpha: f64,
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
pub type BatchHandler = Box<dyn Fn(Vec<PendingRequest>) + Send + 'static>;

/// Run the adaptive direct dispatch worker loop.
pub async fn run_adaptive_worker(
    mut rx: mpsc::Receiver<Batch>,
    config: AdaptiveWorkerConfig,
    handler: BatchHandler,
) {
    let mut ewma_batch_size: f64 = 1.0;
    let mut accumulated: Vec<PendingRequest> = Vec::with_capacity(config.max_batch);

    loop {
        let first = match rx.recv().await {
            Some(b) => b,
            None => break,
        };

        accumulated.extend(first.requests);

        let target_batch = compute_target_batch_size(&config, ewma_batch_size);

        if accumulated.len() < target_batch {
            let deadline = Instant::now() + config.max_linger;

            while accumulated.len() < config.max_batch {
                match rx.try_recv() {
                    Ok(batch) => {
                        accumulated.extend(batch.requests);
                        if accumulated.len() >= target_batch {
                            break;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        let now = Instant::now();
                        if now >= deadline {
                            break;
                        }
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                        while let Ok(b) = rx.try_recv() {
                            accumulated.extend(b.requests);
                            if accumulated.len() >= config.max_batch {
                                break;
                            }
                        }
                        break;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }
        }

        let actual_size = accumulated.len();
        ewma_batch_size =
            config.ewma_alpha * (actual_size as f64) + (1.0 - config.ewma_alpha) * ewma_batch_size;

        let batch_to_process = std::mem::replace(
            &mut accumulated,
            Vec::with_capacity(config.max_batch),
        );
        handler(batch_to_process);
    }

    if !accumulated.is_empty() {
        handler(accumulated);
    }
}

fn compute_target_batch_size(config: &AdaptiveWorkerConfig, ewma: f64) -> usize {
    if ewma < config.load_threshold {
        config.min_batch
    } else {
        let factor = ((ewma - config.load_threshold) / config.load_threshold).min(1.0);
        let range = (config.max_batch - config.min_batch) as f64;
        config.min_batch + (range * factor) as usize
    }
}

/// Sharded pool of adaptive direct dispatch workers.
pub struct DirectWorkerPool {
    shard_senders: Vec<mpsc::Sender<Batch>>,
}

impl DirectWorkerPool {
    pub fn spawn<F>(
        mut ingress_rx: mpsc::Receiver<Batch>,
        worker_count: usize,
        config: AdaptiveWorkerConfig,
        mut handler_factory: F,
    ) -> Self
    where
        F: FnMut(usize) -> BatchHandler + Send + 'static,
    {
        let count = worker_count.max(1);
        let mut shard_senders = Vec::with_capacity(count);

        for worker_idx in 0..count {
            let (tx, rx) = mpsc::channel(config.max_batch.max(64));
            let worker_cfg = config.clone();
            let handler = handler_factory(worker_idx);
            tokio::spawn(run_adaptive_worker(rx, worker_cfg, handler));
            shard_senders.push(tx);
        }

        let fanout_senders = shard_senders.clone();
        tokio::spawn(async move {
            let mut next_idx = 0usize;
            while let Some(batch) = ingress_rx.recv().await {
                let target = next_idx % fanout_senders.len();
                next_idx = next_idx.wrapping_add(1);
                if fanout_senders[target].send(batch).await.is_err() {
                    break;
                }
            }
        });

        Self { shard_senders }
    }

    pub fn shard_count(&self) -> usize {
        self.shard_senders.len()
    }
}
