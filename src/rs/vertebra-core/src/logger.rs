//! Non-blocking GIL-free buffered log queue and background consumer (`logger.rs`).

use std::collections::VecDeque;
use std::io::{self, BufWriter, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Overflow policy when the bounded log queue reaches capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    DropOldest,
    DropNewest,
    BlockTimeoutMs(u64),
}

/// Atomic telemetry metrics for the non-blocking logger.
#[derive(Debug, Default)]
pub struct LoggerMetrics {
    pub enqueued: AtomicU64,
    pub flushed: AtomicU64,
    pub dropped: AtomicU64,
}

struct QueueState {
    buf: VecDeque<Vec<u8>>,
    closed: bool,
}

/// Bounded multi-producer single-consumer byte queue for non-blocking log records.
pub struct BoundedLogQueue {
    capacity: usize,
    policy: OverflowPolicy,
    state: Mutex<QueueState>,
    not_empty: Condvar,
    not_full: Condvar,
    pub metrics: LoggerMetrics,
}

impl BoundedLogQueue {
    pub fn new(capacity: usize, policy: OverflowPolicy) -> Self {
        Self {
            capacity: capacity.max(1),
            policy,
            state: Mutex::new(QueueState {
                buf: VecDeque::with_capacity(capacity.clamp(1, 4096)),
                closed: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            metrics: LoggerMetrics::default(),
        }
    }

    pub fn push(&self, payload: Vec<u8>) -> bool {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.closed {
            return false;
        }
        if guard.buf.len() >= self.capacity {
            match self.policy {
                OverflowPolicy::DropNewest => {
                    self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
                OverflowPolicy::DropOldest => {
                    guard.buf.pop_front();
                    self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                }
                OverflowPolicy::BlockTimeoutMs(ms) => {
                    let timeout = Duration::from_millis(ms);
                    let (new_guard, result) =
                        match self.not_full.wait_timeout_while(guard, timeout, |s| {
                            s.buf.len() >= self.capacity && !s.closed
                        }) {
                            Ok(pair) => pair,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                    guard = new_guard;
                    if result.timed_out() || guard.closed {
                        self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                        return false;
                    }
                }
            }
        }
        guard.buf.push_back(payload);
        self.metrics.enqueued.fetch_add(1, Ordering::Relaxed);
        self.not_empty.notify_one();
        true
    }

    pub fn drain_batch(&self, max_items: usize, wait_timeout: Duration) -> Vec<Vec<u8>> {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.buf.is_empty() && !guard.closed {
            guard = match self.not_empty.wait_timeout(guard, wait_timeout) {
                Ok((g, _)) => g,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
        let count = guard.buf.len().min(max_items.max(1));
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(item) = guard.buf.pop_front() {
                out.push(item);
            }
        }
        if !out.is_empty() {
            self.not_full.notify_all();
        }
        out
    }

    pub fn close(&self) {
        if let Ok(mut guard) = self.state.lock() {
            guard.closed = true;
            self.not_empty.notify_all();
            self.not_full.notify_all();
        }
    }
}

/// Dedicated background consumer that drains `BoundedLogQueue` into a `BufWriter`.
pub struct AsyncLogCore {
    queue: Arc<BoundedLogQueue>,
    running: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl AsyncLogCore {
    pub fn spawn<W: Write + Send + 'static>(
        capacity: usize,
        policy: OverflowPolicy,
        flush_interval: Duration,
        sink: W,
    ) -> Self {
        let queue = Arc::new(BoundedLogQueue::new(capacity, policy));
        let running = Arc::new(AtomicBool::new(true));
        let q_clone = Arc::clone(&queue);
        let r_clone = Arc::clone(&running);

        let worker = thread::spawn(move || {
            let mut writer = BufWriter::with_capacity(64 * 1024, sink);
            while r_clone.load(Ordering::Acquire) {
                let batch = q_clone.drain_batch(256, flush_interval);
                if !batch.is_empty() {
                    for mut line in batch {
                        if !line.ends_with(b"\n") {
                            line.push(b'\n');
                        }
                        let _ = writer.write_all(&line);
                        q_clone.metrics.flushed.fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = writer.flush();
                }
            }
            loop {
                let remaining = q_clone.drain_batch(512, Duration::ZERO);
                if remaining.is_empty() {
                    break;
                }
                for mut line in remaining {
                    if !line.ends_with(b"\n") {
                        line.push(b'\n');
                    }
                    let _ = writer.write_all(&line);
                    q_clone.metrics.flushed.fetch_add(1, Ordering::Relaxed);
                }
            }
            let _ = writer.flush();
        });

        Self {
            queue,
            running,
            worker: Some(worker),
        }
    }

    pub fn spawn_stdout(capacity: usize, policy: OverflowPolicy) -> Self {
        Self::spawn(capacity, policy, Duration::from_millis(25), io::stdout())
    }

    pub fn write_bytes(&self, payload: &[u8]) -> bool {
        self.queue.push(payload.to_vec())
    }

    pub fn queue(&self) -> &Arc<BoundedLogQueue> {
        &self.queue
    }

    pub fn shutdown(&mut self) {
        self.running.store(false, Ordering::Release);
        self.queue.close();
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for AsyncLogCore {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_blocking_queue_drop_oldest_and_flush() {
        let sink_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        struct SharedVecWriter(Arc<Mutex<Vec<u8>>>);
        impl Write for SharedVecWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut core = AsyncLogCore::spawn(
            4,
            OverflowPolicy::DropOldest,
            Duration::from_millis(5),
            SharedVecWriter(Arc::clone(&sink_buf)),
        );
        for idx in 0..6 {
            assert!(core.write_bytes(format!("{{\"seq\":{idx}}}").as_bytes()));
        }
        core.shutdown();
        let output = String::from_utf8(sink_buf.lock().unwrap().clone()).unwrap();
        assert!(output.contains("\"seq\":5"));
        assert!(core.queue().metrics.flushed.load(Ordering::Relaxed) >= 1);
    }
}
