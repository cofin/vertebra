//! Admission control and stream-aware backpressure for the dispatcher ingress path.
//!
//! Isolates long-lived SSE and WebSocket stream leases (`active_streams` / `max_active_streams`)
//! from short-lived unary request permits (`in_flight` / `max_in_flight`), provides
//! `high_watermark` / `low_watermark` atomic pause/resume notifications (`Notify`),
//! allows configured `priority_routes` (`/healthz`, `/readyz`, `/__vertebra/health`) to
//! bypass concurrency shedding, and enforces strict probe countdown in `CircuitPhase::HalfOpen`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

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
            max_in_flight: 1024,
            max_queue_depth: 1024,
            max_queue_time: Duration::from_millis(100),
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

/// Internal phase tracking exact remaining half-open probe permits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitPhase {
    Closed,
    Open { opened_at: Instant },
    HalfOpen { remaining_probes: u32 },
}

#[derive(Debug)]
struct CircuitBreakerInner {
    phase: CircuitPhase,
    samples: VecDeque<(Instant, bool)>,
    failure_count: usize,
    half_open_successes: u64,
}

impl CircuitBreakerInner {
    fn new() -> Self {
        Self {
            phase: CircuitPhase::Closed,
            samples: VecDeque::new(),
            failure_count: 0,
            half_open_successes: 0,
        }
    }

    fn state(&self) -> CircuitState {
        match self.phase {
            CircuitPhase::Closed => CircuitState::Closed,
            CircuitPhase::Open { .. } => CircuitState::Open,
            CircuitPhase::HalfOpen { .. } => CircuitState::HalfOpen,
        }
    }

    fn prune(&mut self, now: Instant, window: Duration) {
        while let Some(&(ts, _)) = self.samples.front() {
            if now.duration_since(ts) > window {
                if let Some((_, is_failure)) = self.samples.pop_front()
                    && is_failure
                {
                    self.failure_count = self.failure_count.saturating_sub(1);
                }
            } else {
                break;
            }
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

/// Admission and stream flow-control coordinator.
pub struct BackpressureController {
    config: BackpressureConfig,
    semaphore: Arc<Semaphore>,
    priority_semaphore: Arc<Semaphore>,
    active_streams: Arc<AtomicUsize>,
    buffered_chunks: AtomicUsize,
    paused: AtomicBool,
    resume_notify: Notify,
    queue_depth: AtomicUsize,
    metrics: Arc<BackpressureMetrics>,
    circuit_state: AtomicU8,
    circuit: Mutex<CircuitBreakerInner>,
}

impl BackpressureController {
    /// Create a new controller from `config`.
    pub fn new(config: BackpressureConfig) -> Self {
        let max = config.max_in_flight.max(1);
        Self {
            config,
            semaphore: Arc::new(Semaphore::new(max)),
            priority_semaphore: Arc::new(Semaphore::new(max.max(16))),
            active_streams: Arc::new(AtomicUsize::new(0)),
            buffered_chunks: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
            resume_notify: Notify::new(),
            queue_depth: AtomicUsize::new(0),
            metrics: Arc::new(BackpressureMetrics::default()),
            circuit_state: AtomicU8::new(0),
            circuit: Mutex::new(CircuitBreakerInner::new()),
        }
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
        if self.circuit_state.load(Ordering::Acquire) == 0 {
            return CircuitState::Closed;
        }
        let mut inner = self.circuit.lock().expect("circuit mutex");
        if let CircuitPhase::Open { opened_at } = inner.phase
            && opened_at.elapsed() >= self.config.circuit_breaker.window
        {
            let probes = (self.config.circuit_breaker.half_open_probe_count as u32).max(1);
            inner.phase = CircuitPhase::HalfOpen {
                remaining_probes: probes,
            };
            inner.half_open_successes = 0;
            self.circuit_state.store(2, Ordering::Release);
        }
        inner.state()
    }

    fn try_admit_circuit(&self) -> bool {
        if self.circuit_state.load(Ordering::Acquire) == 0 {
            return true;
        }
        let mut inner = self.circuit.lock().expect("circuit mutex");
        if let CircuitPhase::Open { opened_at } = inner.phase
            && opened_at.elapsed() >= self.config.circuit_breaker.window
        {
            let probes = (self.config.circuit_breaker.half_open_probe_count as u32).max(1);
            inner.phase = CircuitPhase::HalfOpen {
                remaining_probes: probes,
            };
            inner.half_open_successes = 0;
            self.circuit_state.store(2, Ordering::Release);
        }

        match &mut inner.phase {
            CircuitPhase::Closed => true,
            CircuitPhase::Open { .. } => false,
            CircuitPhase::HalfOpen { remaining_probes } => {
                if *remaining_probes == 0 {
                    false
                } else {
                    *remaining_probes -= 1;
                    true
                }
            }
        }
    }

    /// Attempt to admit a request for `path`, bypassing unary saturation if `path` is a priority route.
    pub async fn try_acquire_for_path(
        &self,
        path: &str,
    ) -> Result<OwnedSemaphorePermit, RejectionReason> {
        if self.is_priority_route(path)
            && let Ok(permit) = Arc::clone(&self.priority_semaphore).try_acquire_owned()
        {
            self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
            return Ok(permit);
        }
        self.try_acquire().await
    }

    /// Attempt to admit a unary request.
    pub async fn try_acquire(&self) -> Result<OwnedSemaphorePermit, RejectionReason> {
        if !self.try_admit_circuit() {
            self.metrics
                .circuit_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(RejectionReason::CircuitOpen);
        }

        if let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() {
            self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
            return Ok(permit);
        }

        let depth = self.queue_depth.fetch_add(1, Ordering::AcqRel);
        if depth >= self.config.max_queue_depth {
            self.queue_depth.fetch_sub(1, Ordering::AcqRel);
            self.metrics
                .saturated_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(RejectionReason::Saturated);
        }

        let acquire = Arc::clone(&self.semaphore).acquire_owned();
        let result = tokio::time::timeout(self.config.max_queue_time, acquire).await;
        self.queue_depth.fetch_sub(1, Ordering::AcqRel);

        match result {
            Ok(Ok(permit)) => {
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(permit)
            }
            Ok(Err(_closed)) => {
                self.metrics.queue_rejected.fetch_add(1, Ordering::Relaxed);
                Err(RejectionReason::Saturated)
            }
            Err(_elapsed) => {
                self.metrics
                    .timeout_rejected
                    .fetch_add(1, Ordering::Relaxed);
                Err(RejectionReason::QueueTimeout)
            }
        }
    }

    /// Record a successful request (2xx/3xx/4xx).
    pub fn record_success(&self) {
        let mut inner = self.circuit.lock().expect("circuit mutex");
        let now = Instant::now();
        inner.prune(now, self.config.circuit_breaker.window);
        inner.samples.push_back((now, false));

        if matches!(inner.phase, CircuitPhase::HalfOpen { .. }) {
            inner.half_open_successes += 1;
            if inner.half_open_successes >= self.config.circuit_breaker.half_open_probe_count {
                inner.phase = CircuitPhase::Closed;
                inner.samples.clear();
                inner.failure_count = 0;
                self.circuit_state.store(0, Ordering::Release);
            }
        }
    }

    /// Record a failed request (5xx or internal error).
    pub fn record_failure(&self) {
        let mut inner = self.circuit.lock().expect("circuit mutex");
        let now = Instant::now();
        inner.prune(now, self.config.circuit_breaker.window);
        inner.samples.push_back((now, true));
        inner.failure_count += 1;

        if matches!(inner.phase, CircuitPhase::HalfOpen { .. }) {
            inner.phase = CircuitPhase::Open { opened_at: now };
            inner.half_open_successes = 0;
            self.circuit_state.store(1, Ordering::Release);
            return;
        }

        if inner.phase == CircuitPhase::Closed
            && inner.samples.len() as u64 >= self.config.circuit_breaker.min_samples
        {
            let failures = inner.failure_count as f64;
            let total = inner.samples.len() as f64;
            if failures / total >= self.config.circuit_breaker.failure_rate_threshold {
                inner.phase = CircuitPhase::Open { opened_at: now };
                self.circuit_state.store(1, Ordering::Release);
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
            in_flight: self.config.max_in_flight - self.semaphore.available_permits(),
            active_streams: self.active_streams.load(Ordering::Acquire),
            queue_depth: self.queue_depth.load(Ordering::Acquire),
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

    #[tokio::test]
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
        tokio::time::sleep(Duration::from_millis(220)).await;
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
        tokio::time::sleep(Duration::from_millis(220)).await;
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
