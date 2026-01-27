use std::sync::Arc;
use tokio::sync::Mutex;
use crate::worker::Worker;
use std::sync::atomic::{AtomicUsize, Ordering};

pub trait Dispatcher: Send + Sync {
    fn get_worker(&self) -> Option<Arc<Mutex<Worker>>>;
    fn worker_count(&self) -> usize;
}

pub struct RoundRobinDispatcher {
    workers: Vec<Arc<Mutex<Worker>>>,
    next: AtomicUsize,
}

impl RoundRobinDispatcher {
    pub fn new(workers: Vec<Worker>) -> Self {
        Self {
            workers: workers.into_iter().map(|w| Arc::new(Mutex::new(w))).collect(),
            next: AtomicUsize::new(0),
        }
    }
}

impl Dispatcher for RoundRobinDispatcher {
    fn get_worker(&self) -> Option<Arc<Mutex<Worker>>> {
        if self.workers.is_empty() {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        Some(self.workers[idx].clone())
    }

    fn worker_count(&self) -> usize {
        self.workers.len()
    }
}
