use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct WorkerManager {
    workers: usize,
}

impl WorkerManager {
    pub fn new(workers: usize) -> Self {
        Self { workers }
    }

    pub fn start(&self) {
        println!("Starting {} workers...", self.workers);
    }
}
