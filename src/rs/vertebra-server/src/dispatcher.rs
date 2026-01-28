use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::worker::WorkerConnection;

pub trait Dispatcher: Send + Sync {
    fn get_worker(&self) -> Option<Arc<WorkerConnection>>;
    fn worker_count(&self) -> usize;
    fn workers(&self) -> &[Arc<WorkerConnection>];
}

pub struct RoundRobinDispatcher {
    workers: Vec<Arc<WorkerConnection>>,
    next: AtomicUsize,
}

impl RoundRobinDispatcher {
    pub fn new(workers: Vec<WorkerConnection>) -> Self {
        Self {
            workers: workers.into_iter().map(Arc::new).collect(),
            next: AtomicUsize::new(0),
        }
    }
}

impl Dispatcher for RoundRobinDispatcher {
    fn get_worker(&self) -> Option<Arc<WorkerConnection>> {
        if self.workers.is_empty() {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        Some(self.workers[idx].clone())
    }

    fn worker_count(&self) -> usize {
        self.workers.len()
    }

    fn workers(&self) -> &[Arc<WorkerConnection>] {
        &self.workers
    }
}

/// Dispatcher that selects the worker with the fewest pending requests.
/// This provides better load distribution under varying request durations.
pub struct LeastPendingDispatcher {
    workers: Vec<Arc<WorkerConnection>>,
}

impl LeastPendingDispatcher {
    pub fn new(workers: Vec<WorkerConnection>) -> Self {
        Self {
            workers: workers.into_iter().map(Arc::new).collect(),
        }
    }
}

impl Dispatcher for LeastPendingDispatcher {
    fn get_worker(&self) -> Option<Arc<WorkerConnection>> {
        if self.workers.is_empty() {
            return None;
        }
        // Select worker with fewest pending requests (fast atomic reads)
        self.workers
            .iter()
            .min_by_key(|w| w.pending_count())
            .map(Arc::clone)
    }

    fn worker_count(&self) -> usize {
        self.workers.len()
    }

    fn workers(&self) -> &[Arc<WorkerConnection>] {
        &self.workers
    }
}
