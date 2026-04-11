//! Admission control for the dispatcher ingress path.
//!
//! Sits in front of every dispatch mode as an explicit accept/shed decision.
//! The three rejection reasons are:
//!
//! 1. **Saturated** — the semaphore is out of permits and the waiter queue is
//!    already at `max_queue_depth`. Returns 503.
//! 2. **Queue timeout** — the waiter waited longer than `max_queue_time` for
//!    a permit. Returns 503.
//! 3. **Circuit open** — the circuit breaker tripped after enough recent
//!    failures. Returns 429 (rate-limit-style rejection so proxies know to
//!    back off rather than retry immediately).
//!
//! Success / failure are recorded per-response via
//! `record_success` / `record_failure`; the circuit breaker looks at the
//! failure rate over a sliding window.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Tunable thresholds for the admission controller.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum concurrent in-flight permits. Must be > 0.
    pub max_in_flight: usize,
    /// Maximum number of waiters blocked on `try_acquire` before we start
    /// shedding with 503.
    pub max_queue_depth: usize,
    /// Maximum time a waiter may block before we shed with 503.
    pub max_queue_time: Duration,
    /// Circuit-breaker thresholds.
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 1024,
            max_queue_depth: 1024,
            max_queue_time: Duration::from_millis(100),
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

/// Sliding-window failure-rate circuit breaker thresholds.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Open the circuit when the recent failure rate crosses this fraction
    /// (0.0..=1.0). 0.5 means "half or more of recent responses failed".
    pub failure_rate_threshold: f64,
    /// Minimum number of samples in the window before we even consider
    /// opening. Avoids opening on a single failed request during warm-up.
    pub min_samples: u64,
    /// Sliding window for sample decay.
    pub window: Duration,
    /// How many successful probes we require in the half-open state before
    /// fully closing the circuit again.
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

/// Reason a request was rejected by the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    /// In-flight permits exhausted and queue depth was exceeded.
    Saturated,
    /// Waiter sat in the queue longer than `max_queue_time`.
    QueueTimeout,
    /// Circuit breaker was open when the request arrived.
    CircuitOpen,
}

impl RejectionReason {
    /// HTTP status code to return for this rejection reason.
    pub fn status(&self) -> u16 {
        match self {
            Self::Saturated | Self::QueueTimeout => 503,
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

#[derive(Debug)]
struct CircuitBreakerInner {
    state: CircuitState,
    /// Timestamps of recent samples (success = false, failure = true).
    samples: VecDeque<(Instant, bool)>,
    /// Count of successful probes while half-open.
    half_open_successes: u64,
    /// Instant the circuit opened (for computing when to transition to
    /// half-open).
    opened_at: Option<Instant>,
}

impl CircuitBreakerInner {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            samples: VecDeque::new(),
            half_open_successes: 0,
            opened_at: None,
        }
    }

    fn prune(&mut self, now: Instant, window: Duration) {
        while let Some(&(ts, _)) = self.samples.front() {
            if now.duration_since(ts) > window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }
}

/// Admission controller. Single instance per dispatcher; cheap to clone
/// via the inner `Arc`.
pub struct BackpressureController {
    config: BackpressureConfig,
    semaphore: Arc<Semaphore>,
    queue_depth: AtomicUsize,
    metrics: Arc<BackpressureMetrics>,
    circuit: Mutex<CircuitBreakerInner>,
}

impl BackpressureController {
    /// Create a new controller. `max_in_flight` must be > 0; if 0 is given,
    /// the controller is disabled and all `try_acquire` calls succeed
    /// without accounting.
    pub fn new(config: BackpressureConfig) -> Self {
        let max = config.max_in_flight.max(1);
        Self {
            config,
            semaphore: Arc::new(Semaphore::new(max)),
            queue_depth: AtomicUsize::new(0),
            metrics: Arc::new(BackpressureMetrics::default()),
            circuit: Mutex::new(CircuitBreakerInner::new()),
        }
    }

    /// Check the circuit breaker; may transition open → half-open based on
    /// the elapsed time since opening.
    fn check_circuit(&self) -> CircuitState {
        let mut inner = self.circuit.lock().expect("circuit mutex");
        if inner.state == CircuitState::Open {
            if let Some(opened) = inner.opened_at {
                if opened.elapsed() >= self.config.circuit_breaker.window {
                    inner.state = CircuitState::HalfOpen;
                    inner.half_open_successes = 0;
                }
            }
        }
        inner.state
    }

    /// Attempt to admit a request. Returns a held permit on success, or the
    /// rejection reason on shed.
    pub async fn try_acquire(&self) -> Result<OwnedSemaphorePermit, RejectionReason> {
        // Circuit breaker: reject immediately if open.
        if matches!(self.check_circuit(), CircuitState::Open) {
            self.metrics.circuit_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(RejectionReason::CircuitOpen);
        }

        // Fast path: try acquire without blocking.
        if let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() {
            self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
            return Ok(permit);
        }

        // Slow path: queue for a permit. Reject if the queue is full.
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
                self.metrics
                    .queue_rejected
                    .fetch_add(1, Ordering::Relaxed);
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

        if inner.state == CircuitState::HalfOpen {
            inner.half_open_successes += 1;
            if inner.half_open_successes >= self.config.circuit_breaker.half_open_probe_count {
                inner.state = CircuitState::Closed;
                inner.opened_at = None;
                inner.samples.clear();
            }
        }
    }

    /// Record a failed request (5xx or internal error).
    pub fn record_failure(&self) {
        let mut inner = self.circuit.lock().expect("circuit mutex");
        let now = Instant::now();
        inner.prune(now, self.config.circuit_breaker.window);
        inner.samples.push_back((now, true));

        // Any failure in half-open slams us back to open.
        if inner.state == CircuitState::HalfOpen {
            inner.state = CircuitState::Open;
            inner.opened_at = Some(now);
            inner.half_open_successes = 0;
            return;
        }

        if inner.state == CircuitState::Closed
            && inner.samples.len() as u64 >= self.config.circuit_breaker.min_samples
        {
            let failures = inner.samples.iter().filter(|(_, f)| *f).count() as f64;
            let total = inner.samples.len() as f64;
            if failures / total >= self.config.circuit_breaker.failure_rate_threshold {
                inner.state = CircuitState::Open;
                inner.opened_at = Some(now);
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
            queue_depth: self.queue_depth.load(Ordering::Acquire),
            circuit_state: self.check_circuit(),
        }
    }

    /// Shared reference to the metrics counters, for tests or external
    /// observers that want live values.
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
        // 4 failures, 0 successes = 100% failure rate, min_samples=4
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
        // Wait out the window so check_circuit transitions to half-open.
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
        assert_eq!(RejectionReason::CircuitOpen.status(), 429);
    }
}
