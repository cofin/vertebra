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
    write_seq: u64,
    evicted_seq: u64,
    flushed_seq: u64,
    worker_busy: bool,
}

/// Bounded multi-producer single-consumer byte queue for non-blocking log records.
pub struct BoundedLogQueue {
    capacity: usize,
    policy: OverflowPolicy,
    state: Mutex<QueueState>,
    not_empty: Condvar,
    not_full: Condvar,
    flushed_cv: Condvar,
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
                write_seq: 0,
                evicted_seq: 0,
                flushed_seq: 0,
                worker_busy: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            flushed_cv: Condvar::new(),
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
                    self.metrics.dropped.fetch_add(1, Ordering::AcqRel);
                    return false;
                }
                OverflowPolicy::DropOldest => {
                    if guard.buf.pop_front().is_some() {
                        guard.evicted_seq += 1;
                        self.metrics.dropped.fetch_add(1, Ordering::AcqRel);
                        self.flushed_cv.notify_all();
                    }
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
                        self.metrics.dropped.fetch_add(1, Ordering::AcqRel);
                        return false;
                    }
                }
            }
        }
        guard.buf.push_back(payload);
        guard.write_seq += 1;
        self.metrics.enqueued.fetch_add(1, Ordering::AcqRel);
        self.not_empty.notify_one();
        true
    }

    pub fn drain_batch(&self, max_items: usize, wait_timeout: Duration) -> Vec<Vec<u8>> {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.buf.is_empty() && !guard.closed && !wait_timeout.is_zero() {
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
        guard.worker_busy = !out.is_empty();
        if !out.is_empty() {
            self.not_full.notify_all();
        }
        out
    }

    /// Record that `count` drained records have been written and flushed to the underlying sink.
    pub fn mark_flushed(&self, count: usize) {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.flushed_seq += count as u64;
        guard.worker_busy = false;
        self.metrics
            .flushed
            .fetch_add(count as u64, Ordering::Release);
        self.flushed_cv.notify_all();
    }

    /// Block until all records enqueued up to this call have been flushed (or evicted) to the sink.
    pub fn wait_flushed(&self, timeout: Duration) -> bool {
        let guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let target_seq = guard.write_seq;
        if guard.flushed_seq + guard.evicted_seq >= target_seq {
            return true;
        }
        self.not_empty.notify_all();
        let (final_guard, _) = match self.flushed_cv.wait_timeout_while(guard, timeout, |s| {
            s.flushed_seq + s.evicted_seq < target_seq
                && !(s.closed && s.buf.is_empty() && !s.worker_busy)
        }) {
            Ok(pair) => pair,
            Err(poisoned) => poisoned.into_inner(),
        };
        final_guard.flushed_seq + final_guard.evicted_seq >= target_seq
    }

    /// Return the exact count of accepted records not yet flushed to the underlying sink.
    pub fn pending_len(&self) -> usize {
        let guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .write_seq
            .saturating_sub(guard.flushed_seq + guard.evicted_seq) as usize
    }

    fn mark_worker_stopped(&self) {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.worker_busy = false;
        self.flushed_cv.notify_all();
    }

    pub fn close(&self) {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.closed = true;
        self.not_empty.notify_all();
        self.not_full.notify_all();
        self.flushed_cv.notify_all();
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
                    let batch_len = batch.len();
                    for mut line in batch {
                        if !line.ends_with(b"\n") {
                            line.push(b'\n');
                        }
                        let _ = writer.write_all(&line);
                    }
                    let _ = writer.flush();
                    q_clone.mark_flushed(batch_len);
                }
            }
            loop {
                let remaining = q_clone.drain_batch(512, Duration::ZERO);
                if remaining.is_empty() {
                    break;
                }
                let batch_len = remaining.len();
                for mut line in remaining {
                    if !line.ends_with(b"\n") {
                        line.push(b'\n');
                    }
                    let _ = writer.write_all(&line);
                }
                let _ = writer.flush();
                q_clone.mark_flushed(batch_len);
            }
            let _ = writer.flush();
            q_clone.mark_worker_stopped();
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

    pub fn flush(&self, timeout: Duration) -> bool {
        self.queue.wait_flushed(timeout)
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
        assert!(core.queue().metrics.flushed.load(Ordering::Acquire) >= 1);
    }

    #[test]
    fn flush_waits_for_underlying_sink_write_after_bufwriter_flush() {
        #[derive(Default)]
        struct GateState {
            writes: usize,
            flushes: usize,
            allow_writes: usize,
            allow_flushes: usize,
            gate_enabled: bool,
        }

        let sink_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new((
            Mutex::new(GateState {
                gate_enabled: true,
                ..GateState::default()
            }),
            Condvar::new(),
        ));

        struct GatedFlushWriter {
            buf: Arc<Mutex<Vec<u8>>>,
            gate: Arc<(Mutex<GateState>, Condvar)>,
        }

        impl Write for GatedFlushWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                let (lock, cv) = &*self.gate;
                let mut state = lock.lock().unwrap();
                state.writes += 1;
                let target = state.writes;
                cv.notify_all();
                while state.gate_enabled && state.allow_writes < target {
                    state = cv.wait(state).unwrap();
                }
                drop(state);
                self.buf.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                let (lock, cv) = &*self.gate;
                let mut state = lock.lock().unwrap();
                state.flushes += 1;
                let target = state.flushes;
                cv.notify_all();
                while state.gate_enabled && state.allow_flushes < target {
                    state = cv.wait(state).unwrap();
                }
                Ok(())
            }
        }

        let core = AsyncLogCore::spawn(
            8,
            OverflowPolicy::DropNewest,
            Duration::from_millis(100),
            GatedFlushWriter {
                buf: Arc::clone(&sink_buf),
                gate: Arc::clone(&gate),
            },
        );
        let (lock, cv) = &*gate;
        for idx in 0..10 {
            let payload = format!("{{\"event\":\"item-{idx}\"}}");
            assert!(core.write_bytes(payload.as_bytes()));

            let mut state = lock.lock().unwrap();
            while state.writes < idx + 1 {
                state = cv.wait(state).unwrap();
            }
            drop(state);
            assert!(!core.flush(Duration::ZERO));

            {
                let mut state = lock.lock().unwrap();
                state.allow_writes = idx + 1;
                cv.notify_all();
                while state.flushes < idx + 1 {
                    state = cv.wait(state).unwrap();
                }
            }
            assert!(!core.flush(Duration::ZERO));

            {
                let mut state = lock.lock().unwrap();
                state.allow_flushes = idx + 1;
                cv.notify_all();
            }

            assert!(core.flush(Duration::from_secs(2)));
            let snapshot = String::from_utf8(sink_buf.lock().unwrap().clone()).unwrap();
            assert!(
                snapshot.contains(&payload),
                "sink missing {payload} after flush(): {snapshot:?}"
            );
            assert_eq!(core.queue().pending_len(), 0);
        }

        let mut state = lock.lock().unwrap();
        state.gate_enabled = false;
        cv.notify_all();
    }

    #[test]
    fn drop_newest_does_not_skew_pending_len_or_flush_barrier() {
        let q = BoundedLogQueue::new(2, OverflowPolicy::DropNewest);
        assert!(q.push(b"a".to_vec()));
        assert!(q.push(b"b".to_vec()));
        assert!(!q.push(b"c".to_vec()));
        assert_eq!(q.pending_len(), 2);
        let batch = q.drain_batch(1, Duration::ZERO);
        assert_eq!(batch.len(), 1);
        q.mark_flushed(1);
        assert_eq!(q.pending_len(), 1);
        assert!(!q.wait_flushed(Duration::from_millis(2)));
        let batch2 = q.drain_batch(1, Duration::ZERO);
        assert_eq!(batch2.len(), 1);
        q.mark_flushed(1);
        assert_eq!(q.pending_len(), 0);
        assert!(q.wait_flushed(Duration::from_millis(2)));
    }
}
