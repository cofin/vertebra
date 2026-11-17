//! Admission control and stream-aware backpressure for the dispatcher ingress path.
//!
//! Isolates long-lived SSE and WebSocket stream leases (`active_streams` / `max_active_streams`)
//! from short-lived unary request permits (`in_flight` / `max_in_flight`), provides
//! `high_watermark` / `low_watermark` atomic pause/resume notifications (`Notify`),
//! allows configured `priority_routes` (`/healthz`, `/readyz`, `/__vertebra/health`) to
//! bypass concurrency shedding, and enforces strict lock-free probe countdown in `HalfOpen`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use vertebra_core::buffer::CacheLine;

/// Tunable thresholds for the admission and stream flow controller.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum concurrent unary in-flight permits. Must be > 0.
    pub max_in_flight: usize,
    /// Maximum number of unary waiters blocked on `try_acquire` before shedding with 503.
    pub max_queue_depth: usize,
    /// Maximum time a unary waiter may block before shedding with 503.
    pub max_queue_time: Duration,
    /// Maximum concurrent long-lived SSE/WebSocket streams.
    pub max_active_streams: usize,
    /// Buffered stream chunk count at which producers are paused.
    pub high_watermark: usize,
    /// Buffered stream chunk count at which paused producers are resumed.
    pub low_watermark: usize,
    /// Priority route paths or prefixes (e.g. `/healthz`, `/readyz`) that bypass unary shedding.
    pub priority_routes: Vec<String>,
    /// Circuit-breaker thresholds.
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 4096,
            max_queue_depth: 4096,
            max_queue_time: Duration::from_millis(5000),
            max_active_streams: 65_536,
            high_watermark: 192,
            low_watermark: 64,
            priority_routes: vec![
                "/healthz".to_string(),
                "/readyz".to_string(),
                "/__vertebra/health".to_string(),
            ],
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

/// Sliding-window failure-rate circuit breaker thresholds.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_rate_threshold: f64,
    pub min_samples: u64,
    pub window: Duration,
    pub half_open_probe_count: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_rate_threshold: 0.5,
            min_samples: 20,
            window: Duration::from_secs(10),
            half_open_probe_count: 5,
        }
    }
}

/// Reason a request or stream was rejected by the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    /// Unary in-flight permits exhausted and queue depth was exceeded.
    Saturated,
    /// Waiter sat in the queue longer than `max_queue_time`.
    QueueTimeout,
    /// Circuit breaker was open (or half-open probe quota exhausted).
    CircuitOpen,
    /// Long-lived stream count reached `max_active_streams`.
    StreamLimitExceeded,
}

impl RejectionReason {
    /// HTTP status code to return for this rejection reason.
    pub fn status(&self) -> u16 {
        match self {
            Self::Saturated | Self::QueueTimeout | Self::StreamLimitExceeded => 503,
            Self::CircuitOpen => 429,
        }
    }
}

/// Atomic counters exposed via the metrics snapshot.
#[derive(Debug, Default)]
pub struct BackpressureMetrics {
    pub accepted: AtomicU64,
    pub saturated_rejected: AtomicU64,
    pub queue_rejected: AtomicU64,
    pub circuit_rejected: AtomicU64,
    pub timeout_rejected: AtomicU64,
    pub stream_rejected: AtomicU64,
}

/// Plain snapshot of the controller's counters for the health endpoint.
#[derive(Debug, Clone, Copy)]
pub struct BackpressureMetricsSnapshot {
    pub accepted: u64,
    pub saturated_rejected: u64,
    pub queue_rejected: u64,
    pub circuit_rejected: u64,
    pub timeout_rejected: u64,
    pub in_flight: usize,
    pub active_streams: usize,
    pub queue_depth: usize,
    pub circuit_state: CircuitState,
}

/// Circuit breaker state exposed to the metrics snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

const PHASE_CLOSED: u64 = 0;
const PHASE_OPEN: u64 = 1;
const PHASE_HALF_OPEN: u64 = 2;
const PHASE_SHIFT: u32 = 62;
const OPEN_UNTIL_MASK: u64 = 0x3FFF_FFFF_FFFF_FFFF;
const HALF_OPEN_COUNT_MASK: u64 = 0x7FFF_FFFF;
const HALF_OPEN_REM_SHIFT: u32 = 31;

const BUCKET_SHARDS: usize = 4;
const NUM_BUCKETS: usize = 16;
const BUCKET_EPOCH_SHIFT: u32 = 36;
const BUCKET_TOTAL_SHIFT: u32 = 18;
const BUCKET_SAMPLE_MASK: u64 = 0x0003_FFFF;

static NEXT_THREAD_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static THREAD_BUCKET_SHARD: usize =
        NEXT_THREAD_SHARD.fetch_add(1, Ordering::Relaxed) & (BUCKET_SHARDS - 1);
}

#[inline]
fn current_bucket_shard() -> usize {
    THREAD_BUCKET_SHARD.with(|&s| s)
}

#[inline]
fn encode_open_word(until_ms: u64) -> u64 {
    (PHASE_OPEN << PHASE_SHIFT) | (until_ms & OPEN_UNTIL_MASK)
}

#[inline]
fn encode_half_open_word(remaining_probes: u32, successes: u32) -> u64 {
    (PHASE_HALF_OPEN << PHASE_SHIFT)
        | (((remaining_probes as u64) & HALF_OPEN_COUNT_MASK) << HALF_OPEN_REM_SHIFT)
        | ((successes as u64) & HALF_OPEN_COUNT_MASK)
}

#[derive(Debug)]
struct QueueWakeState {
    queue_depth: CacheLine<AtomicUsize>,
    slot_notify: Notify,
}

/// Lock-free RAII guard for an admitted unary or priority HTTP request.
#[derive(Debug)]
pub struct AdmissionPermit {
    counter: Arc<CacheLine<AtomicUsize>>,
    wake: Option<Arc<QueueWakeState>>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.counter.0.fetch_sub(1, Ordering::AcqRel);
        std::sync::atomic::fence(Ordering::SeqCst);
        if let Some(ref wake) = self.wake
            && wake.queue_depth.0.load(Ordering::Acquire) > 0
        {
            wake.slot_notify.notify_one();
        }
    }
}

/// RAII guard for an active long-lived SSE or WebSocket stream.
#[derive(Debug)]
pub struct StreamGuard {
    active_streams: Arc<AtomicUsize>,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.active_streams.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Lock-free admission and stream flow-control coordinator.
pub struct BackpressureController {
    config: BackpressureConfig,
    max_in_flight: usize,
    max_priority_in_flight: usize,
    in_flight: Arc<CacheLine<AtomicUsize>>,
    priority_in_flight: Arc<CacheLine<AtomicUsize>>,
    wake_state: Arc<QueueWakeState>,
    active_streams: Arc<AtomicUsize>,
    buffered_chunks: AtomicUsize,
    paused: AtomicBool,
    resume_notify: Notify,
    metrics: Arc<BackpressureMetrics>,
    clock_origin: Instant,
    clock_offset_ms: AtomicU64,
    window_ms: u64,
    bucket_ms: u64,
    half_open_probes: u32,
    circuit_word: CacheLine<AtomicU64>,
    buckets: Box<[[CacheLine<AtomicU64>; NUM_BUCKETS]; BUCKET_SHARDS]>,
}

impl BackpressureController {
    /// Create a new controller from `config`.
    pub fn new(config: BackpressureConfig) -> Self {
        let max_in_flight = config.max_in_flight.max(1);
        let max_priority_in_flight = max_in_flight.max(16);
        let window_ms = (config.circuit_breaker.window.as_millis() as u64).max(1);
        let bucket_ms = window_ms.div_ceil(NUM_BUCKETS as u64).max(1);
        let half_open_probes = (config
            .circuit_breaker
            .half_open_probe_count
            .min(HALF_OPEN_COUNT_MASK) as u32)
            .max(1);
        let buckets = Box::new(std::array::from_fn(|_| {
            std::array::from_fn(|_| CacheLine(AtomicU64::new(0)))
        }));

        Self {
            config,
            max_in_flight,
            max_priority_in_flight,
            in_flight: Arc::new(CacheLine(AtomicUsize::new(0))),
            priority_in_flight: Arc::new(CacheLine(AtomicUsize::new(0))),
            wake_state: Arc::new(QueueWakeState {
                queue_depth: CacheLine(AtomicUsize::new(0)),
                slot_notify: Notify::new(),
            }),
            active_streams: Arc::new(AtomicUsize::new(0)),
            buffered_chunks: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
            resume_notify: Notify::new(),
            metrics: Arc::new(BackpressureMetrics::default()),
            clock_origin: Instant::now(),
            clock_offset_ms: AtomicU64::new(0),
            window_ms,
            bucket_ms,
            half_open_probes,
            circuit_word: CacheLine(AtomicU64::new(0)),
            buckets,
        }
    }

    #[inline]
    fn elapsed_ms(&self) -> u64 {
        (self.clock_origin.elapsed().as_millis() as u64)
            .saturating_add(self.clock_offset_ms.load(Ordering::Acquire))
    }

    /// Advance the circuit-breaker clock by `delta` for deterministic testing.
    #[doc(hidden)]
    pub fn advance_circuit_clock_for_test(&self, delta: Duration) {
        self.clock_offset_ms
            .fetch_add(delta.as_millis() as u64, Ordering::Release);
    }

    fn clear_buckets(&self) {
        for shard in self.buckets.iter() {
            for slot in shard.iter() {
                slot.0.store(0, Ordering::Release);
            }
        }
    }

    #[inline]
    fn record_bucket_sample(&self, now_ms: u64, is_failure: bool) {
        let epoch = (now_ms / self.bucket_ms).wrapping_add(1);
        let bucket_idx = (epoch as usize) & (NUM_BUCKETS - 1);
        let shard_idx = current_bucket_shard();
        let slot = &self.buckets[shard_idx][bucket_idx];
        let fail_inc = u64::from(is_failure);

        let mut old = slot.0.load(Ordering::Relaxed);
        loop {
            let next = if (old >> BUCKET_EPOCH_SHIFT) == epoch {
                let total = ((old >> BUCKET_TOTAL_SHIFT) & BUCKET_SAMPLE_MASK)
                    .saturating_add(1)
                    .min(BUCKET_SAMPLE_MASK);
                let fails = (old & BUCKET_SAMPLE_MASK)
                    .saturating_add(fail_inc)
                    .min(BUCKET_SAMPLE_MASK);
                (epoch << BUCKET_EPOCH_SHIFT) | (total << BUCKET_TOTAL_SHIFT) | fails
            } else {
                (epoch << BUCKET_EPOCH_SHIFT) | (1u64 << BUCKET_TOTAL_SHIFT) | fail_inc
            };

            match slot
                .0
                .compare_exchange_weak(old, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => old = actual,
            }
        }
    }

    fn sum_window_buckets(&self, now_ms: u64) -> (u64, u64) {
        let current_epoch = (now_ms / self.bucket_ms).wrapping_add(1);
        let min_epoch = current_epoch
            .saturating_sub((NUM_BUCKETS as u64) - 1)
            .max(1);
        let mut total = 0u64;
        let mut failures = 0u64;

        for shard in self.buckets.iter() {
            for slot in shard.iter() {
                let val = slot.0.load(Ordering::Acquire);
                let epoch = val >> BUCKET_EPOCH_SHIFT;
                if epoch >= min_epoch && epoch <= current_epoch {
                    total += (val >> BUCKET_TOTAL_SHIFT) & BUCKET_SAMPLE_MASK;
                    failures += val & BUCKET_SAMPLE_MASK;
                }
            }
        }

        (total, failures)
    }

    /// Acquire an isolated stream lease (`StreamGuard`) without consuming unary `in_flight` capacity.
    pub fn acquire_stream(&self) -> Result<StreamGuard, RejectionReason> {
        let max_streams = self.config.max_active_streams.max(1);
        let prev = self.active_streams.fetch_add(1, Ordering::AcqRel);
        if prev >= max_streams {
            self.active_streams.fetch_sub(1, Ordering::AcqRel);
            self.metrics.stream_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(RejectionReason::StreamLimitExceeded);
        }
        Ok(StreamGuard {
            active_streams: Arc::clone(&self.active_streams),
        })
    }

    /// Record an enqueued stream chunk; returns `true` if `high_watermark` is reached and flow is paused.
    pub fn on_stream_chunk_enqueued(&self) -> bool {
        let count = self.buffered_chunks.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.config.high_watermark.max(1) {
            self.paused.store(true, Ordering::Release);
            return true;
        }
        self.paused.load(Ordering::Acquire)
    }

    /// Record a dequeued stream chunk; wakes paused producers via `Notify` when count drops to `<= low_watermark`.
    pub fn on_stream_chunk_dequeued(&self) -> bool {
        let prev = self.buffered_chunks.fetch_sub(1, Ordering::AcqRel);
        let count = prev.saturating_sub(1);
        if count <= self.config.low_watermark
            && self
                .paused
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.resume_notify.notify_waiters();
        }
        self.paused.load(Ordering::Acquire)
    }

    /// Return `true` if stream producers are currently paused above `high_watermark`.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// Wait asynchronously until buffered chunks drain to `<= low_watermark`.
    pub async fn wait_until_resumed(&self) {
        while self.paused.load(Ordering::Acquire) {
            self.resume_notify.notified().await;
        }
    }

    /// Return `true` if `path` matches a configured priority route.
    #[inline]
    pub fn is_priority_route(&self, path: &str) -> bool {
        self.config.priority_routes.iter().any(|pattern| {
            if let Some(prefix) = pattern.strip_suffix('*') {
                path.starts_with(prefix)
            } else {
                path == pattern
            }
        })
    }

    fn check_circuit_state(&self) -> CircuitState {
        let mut word = self.circuit_word.0.load(Ordering::Acquire);
        loop {
            match word >> PHASE_SHIFT {
                PHASE_CLOSED => return CircuitState::Closed,
                PHASE_HALF_OPEN => return CircuitState::HalfOpen,
                PHASE_OPEN => {
                    let until_ms = word & OPEN_UNTIL_MASK;
                    if self.elapsed_ms() < until_ms {
                        return CircuitState::Open;
                    }
                    let next = encode_half_open_word(self.half_open_probes, 0);
                    match self.circuit_word.0.compare_exchange_weak(
                        word,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return CircuitState::HalfOpen,
                        Err(actual) => word = actual,
                    }
                }
                _ => return CircuitState::Closed,
            }
        }
    }

    #[inline]
    fn try_admit_circuit(&self) -> bool {
        let mut word = self.circuit_word.0.load(Ordering::Acquire);
        if word == 0 {
            return true;
        }
        loop {
            match word >> PHASE_SHIFT {
                PHASE_CLOSED => return true,
                PHASE_OPEN => {
                    let until_ms = word & OPEN_UNTIL_MASK;
                    if self.elapsed_ms() < until_ms {
                        return false;
                    }
                    let next = encode_half_open_word(self.half_open_probes.saturating_sub(1), 0);
                    match self.circuit_word.0.compare_exchange_weak(
                        word,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return true,
                        Err(actual) => word = actual,
                    }
                }
                PHASE_HALF_OPEN => {
                    let rem = ((word >> HALF_OPEN_REM_SHIFT) & HALF_OPEN_COUNT_MASK) as u32;
                    if rem == 0 {
                        return false;
                    }
                    let succ = (word & HALF_OPEN_COUNT_MASK) as u32;
                    let next = encode_half_open_word(rem - 1, succ);
                    match self.circuit_word.0.compare_exchange_weak(
                        word,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return true,
                        Err(actual) => word = actual,
                    }
                }
                _ => return true,
            }
        }
    }

    #[inline]
    fn try_claim_slot(
        counter: &Arc<CacheLine<AtomicUsize>>,
        max: usize,
        wake: Option<Arc<QueueWakeState>>,
    ) -> Option<AdmissionPermit> {
        let mut cur = counter.0.load(Ordering::Acquire);
        loop {
            if cur >= max {
                return None;
            }
            match counter
                .0
                .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(AdmissionPermit {
                        counter: Arc::clone(counter),
                        wake,
                    });
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Attempt to admit a request for `path`, bypassing unary saturation if `path` is a priority route.
    #[inline]
    pub async fn try_acquire_for_path(
        &self,
        path: &str,
    ) -> Result<AdmissionPermit, RejectionReason> {
        if self.is_priority_route(path)
            && let Some(permit) =
                Self::try_claim_slot(&self.priority_in_flight, self.max_priority_in_flight, None)
        {
            self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
            return Ok(permit);
        }
        self.try_acquire().await
    }

    /// Attempt to admit a unary request without locks on the fastpath.
    #[inline]
    pub async fn try_acquire(&self) -> Result<AdmissionPermit, RejectionReason> {
        if !self.try_admit_circuit() {
            self.metrics
                .circuit_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(RejectionReason::CircuitOpen);
        }

        if let Some(permit) = Self::try_claim_slot(
            &self.in_flight,
            self.max_in_flight,
            Some(Arc::clone(&self.wake_state)),
        ) {
            self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
            return Ok(permit);
        }

        if self.config.max_queue_depth == 0 {
            self.metrics
                .saturated_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(RejectionReason::Saturated);
        }

        let depth = self.wake_state.queue_depth.0.fetch_add(1, Ordering::AcqRel);
        if depth >= self.config.max_queue_depth {
            self.wake_state.queue_depth.0.fetch_sub(1, Ordering::AcqRel);
            self.metrics
                .saturated_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(RejectionReason::Saturated);
        }

        let wait_fut = async {
            loop {
                let notified = self.wake_state.slot_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                std::sync::atomic::fence(Ordering::SeqCst);

                if let Some(permit) = Self::try_claim_slot(
                    &self.in_flight,
                    self.max_in_flight,
                    Some(Arc::clone(&self.wake_state)),
                ) {
                    return permit;
                }
                notified.await;
            }
        };

        let result = tokio::time::timeout(self.config.max_queue_time, wait_fut).await;
        self.wake_state.queue_depth.0.fetch_sub(1, Ordering::AcqRel);

        match result {
            Ok(permit) => {
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(permit)
            }
            Err(_elapsed) => {
                self.metrics
                    .timeout_rejected
                    .fetch_add(1, Ordering::Relaxed);
                Err(RejectionReason::QueueTimeout)
            }
        }
    }

    /// Record a successful request (2xx/3xx/4xx) without locks.
    #[inline]
    pub fn record_success(&self) {
        let now_ms = self.elapsed_ms();
        self.record_bucket_sample(now_ms, false);

        let mut word = self.circuit_word.0.load(Ordering::Acquire);
        if word == 0 {
            return;
        }
        loop {
            match word >> PHASE_SHIFT {
                PHASE_OPEN => {
                    let until_ms = word & OPEN_UNTIL_MASK;
                    if now_ms < until_ms {
                        return;
                    }
                    let ho = encode_half_open_word(self.half_open_probes, 0);
                    match self.circuit_word.0.compare_exchange_weak(
                        word,
                        ho,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => word = ho,
                        Err(actual) => word = actual,
                    }
                }
                PHASE_HALF_OPEN => {
                    let rem = ((word >> HALF_OPEN_REM_SHIFT) & HALF_OPEN_COUNT_MASK) as u32;
                    let succ = ((word & HALF_OPEN_COUNT_MASK) as u32).saturating_add(1);
                    if (succ as u64) >= self.config.circuit_breaker.half_open_probe_count.max(1) {
                        self.clear_buckets();
                        match self.circuit_word.0.compare_exchange_weak(
                            word,
                            0,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => return,
                            Err(actual) => word = actual,
                        }
                    } else {
                        let next = encode_half_open_word(rem, succ);
                        match self.circuit_word.0.compare_exchange_weak(
                            word,
                            next,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => return,
                            Err(actual) => word = actual,
                        }
                    }
                }
                _ => return,
            }
        }
    }

    /// Record a failed request (5xx or internal error) without locks.
    pub fn record_failure(&self) {
        let now_ms = self.elapsed_ms();
        self.record_bucket_sample(now_ms, true);

        let mut word = self.circuit_word.0.load(Ordering::Acquire);
        loop {
            match word >> PHASE_SHIFT {
                PHASE_HALF_OPEN => {
                    let open_word = encode_open_word(now_ms.saturating_add(self.window_ms));
                    match self.circuit_word.0.compare_exchange_weak(
                        word,
                        open_word,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return,
                        Err(actual) => word = actual,
                    }
                }
                PHASE_OPEN => {
                    let until_ms = word & OPEN_UNTIL_MASK;
                    if now_ms < until_ms {
                        return;
                    }
                    let open_word = encode_open_word(now_ms.saturating_add(self.window_ms));
                    match self.circuit_word.0.compare_exchange_weak(
                        word,
                        open_word,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return,
                        Err(actual) => word = actual,
                    }
                }
                _ => {
                    let (total, failures) = self.sum_window_buckets(now_ms);
                    if total < self.config.circuit_breaker.min_samples {
                        return;
                    }
                    let rate = (failures as f64) / (total as f64);
                    if rate < self.config.circuit_breaker.failure_rate_threshold {
                        return;
                    }
                    let open_word = encode_open_word(now_ms.saturating_add(self.window_ms));
                    match self.circuit_word.0.compare_exchange_weak(
                        word,
                        open_word,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return,
                        Err(actual) => word = actual,
                    }
                }
            }
        }
    }

    /// Return a point-in-time snapshot of the controller's counters.
    pub fn snapshot(&self) -> BackpressureMetricsSnapshot {
        BackpressureMetricsSnapshot {
            accepted: self.metrics.accepted.load(Ordering::Relaxed),
            saturated_rejected: self.metrics.saturated_rejected.load(Ordering::Relaxed),
            queue_rejected: self.metrics.queue_rejected.load(Ordering::Relaxed),
            circuit_rejected: self.metrics.circuit_rejected.load(Ordering::Relaxed),
            timeout_rejected: self.metrics.timeout_rejected.load(Ordering::Relaxed),
            in_flight: self.in_flight.0.load(Ordering::Acquire),
            active_streams: self.active_streams.load(Ordering::Acquire),
            queue_depth: self.wake_state.queue_depth.0.load(Ordering::Acquire),
            circuit_state: self.check_circuit_state(),
        }
    }

    /// Shared reference to the metrics counters.
    pub fn metrics(&self) -> Arc<BackpressureMetrics> {
        Arc::clone(&self.metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(max_in_flight: usize, max_queue_depth: usize) -> BackpressureConfig {
        BackpressureConfig {
            max_in_flight,
            max_queue_depth,
            max_queue_time: Duration::from_millis(50),
            circuit_breaker: CircuitBreakerConfig {
                failure_rate_threshold: 0.5,
                min_samples: 4,
                window: Duration::from_millis(200),
                half_open_probe_count: 2,
            },
            ..BackpressureConfig::default()
        }
    }

    #[tokio::test]
    async fn accepts_up_to_limit() {
        let c = BackpressureController::new(cfg(2, 0));
        let _p1 = c.try_acquire().await.expect("first permit");
        let _p2 = c.try_acquire().await.expect("second permit");
        let snap = c.snapshot();
        assert_eq!(snap.accepted, 2);
        assert_eq!(snap.in_flight, 2);
    }

    #[tokio::test]
    async fn rejects_when_saturated_and_queue_full() {
        let c = BackpressureController::new(cfg(1, 0));
        let _p = c.try_acquire().await.expect("first permit");
        let err = c.try_acquire().await.expect_err("should reject");
        assert_eq!(err, RejectionReason::Saturated);
        assert_eq!(c.snapshot().saturated_rejected, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_in_queue() {
        let c = BackpressureController::new(cfg(1, 4));
        let _p = c.try_acquire().await.expect("first permit");
        let err = c.try_acquire().await.expect_err("should time out");
        assert_eq!(err, RejectionReason::QueueTimeout);
        assert_eq!(c.snapshot().timeout_rejected, 1);
    }

    #[tokio::test]
    async fn circuit_opens_after_failure_rate() {
        let c = BackpressureController::new(cfg(16, 16));
        for _ in 0..4 {
            c.record_failure();
        }
        assert_eq!(c.snapshot().circuit_state, CircuitState::Open);
        let err = c.try_acquire().await.expect_err("circuit open");
        assert_eq!(err, RejectionReason::CircuitOpen);
        assert_eq!(c.snapshot().circuit_rejected, 1);
    }

    #[tokio::test]
    async fn circuit_half_opens_then_closes_on_probes() {
        let c = BackpressureController::new(cfg(16, 16));
        for _ in 0..4 {
            c.record_failure();
        }
        c.advance_circuit_clock_for_test(Duration::from_millis(220));
        assert_eq!(c.snapshot().circuit_state, CircuitState::HalfOpen);
        c.record_success();
        c.record_success();
        assert_eq!(c.snapshot().circuit_state, CircuitState::Closed);
    }

    #[tokio::test]
    async fn half_open_reopens_on_failure() {
        let c = BackpressureController::new(cfg(16, 16));
        for _ in 0..4 {
            c.record_failure();
        }
        c.advance_circuit_clock_for_test(Duration::from_millis(220));
        assert_eq!(c.snapshot().circuit_state, CircuitState::HalfOpen);
        c.record_failure();
        assert_eq!(c.snapshot().circuit_state, CircuitState::Open);
    }

    #[test]
    fn rejection_status_codes() {
        assert_eq!(RejectionReason::Saturated.status(), 503);
        assert_eq!(RejectionReason::QueueTimeout.status(), 503);
        assert_eq!(RejectionReason::StreamLimitExceeded.status(), 503);
        assert_eq!(RejectionReason::CircuitOpen.status(), 429);
    }
}
