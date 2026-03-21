//! Drain policy for the batch collector.
//!
//! Two modes:
//! - **Fixed**: drain at `size` requests or `timeout`, whichever fires first.
//! - **Adaptive**: preferred batch sizes with EMA arrival-rate tracking
//!   and a hard max queue delay ceiling.

use std::time::Duration;

/// Policy determining when a batch should be drained.
#[derive(Debug, Clone)]
pub enum DrainPolicy {
    /// Drain when `size` requests accumulate OR `timeout` elapses, whichever is first.
    Fixed { size: usize, timeout: Duration },

    /// Adaptive: drain at preferred batch sizes, with EMA-based arrival rate
    /// tracking and a hard max queue delay.
    Adaptive {
        /// Preferred batch sizes to drain at (sorted ascending). When the
        /// pending count matches any of these, drain immediately.
        preferred_sizes: Vec<usize>,
        /// Hard ceiling: max time a request waits in the collector.
        max_queue_delay: Duration,
        /// Exponential moving average of arrival rate (requests/sec).
        ema_arrival_rate: f64,
        /// EMA smoothing factor (0.0–1.0). Higher = more responsive.
        ema_alpha: f64,
        /// Minimum batch size (never drain smaller than this, except on timeout).
        min_size: usize,
        /// Maximum batch size cap.
        max_size: usize,
    },
}

impl DrainPolicy {
    /// Create the default fixed policy (64 requests, 1ms timeout).
    pub fn default_fixed() -> Self {
        Self::Fixed {
            size: 64,
            timeout: Duration::from_millis(1),
        }
    }

    /// Create the default adaptive policy.
    pub fn default_adaptive() -> Self {
        Self::Adaptive {
            preferred_sizes: vec![8, 16, 32, 64],
            max_queue_delay: Duration::from_micros(200),
            ema_arrival_rate: 0.0,
            ema_alpha: 0.25,
            min_size: 1,
            max_size: 64,
        }
    }

    /// Return the effective batch size cap for the current policy state.
    pub fn batch_size(&self) -> usize {
        match self {
            Self::Fixed { size, .. } => *size,
            Self::Adaptive { max_size, .. } => *max_size,
        }
    }

    /// Return the drain timeout for the current policy state.
    pub fn timeout(&self) -> Duration {
        match self {
            Self::Fixed { timeout, .. } => *timeout,
            Self::Adaptive {
                max_queue_delay, ..
            } => *max_queue_delay,
        }
    }

    /// Returns true if the pending count matches a preferred batch size
    /// (adaptive mode only). The collector should drain immediately when
    /// this returns true.
    pub fn matches_preferred(&self, pending: usize) -> bool {
        match self {
            Self::Adaptive {
                preferred_sizes, ..
            } => preferred_sizes.contains(&pending),
            _ => false,
        }
    }

    /// Update EMA of arrival rate after draining a batch.
    pub fn adapt(&mut self, batch_count: usize) {
        if let Self::Adaptive {
            ema_arrival_rate,
            ema_alpha,
            ..
        } = self
        {
            // Simple adaptation: use batch count as a proxy for arrival rate.
            // A full EMA over time requires tracking elapsed time between drains;
            // for now, smooth the batch sizes themselves.
            let rate = batch_count as f64;
            *ema_arrival_rate = *ema_alpha * rate + (1.0 - *ema_alpha) * *ema_arrival_rate;
        }
    }
}

impl Default for DrainPolicy {
    fn default() -> Self {
        Self::default_fixed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_policy_defaults() {
        let p = DrainPolicy::default_fixed();
        assert_eq!(p.batch_size(), 64);
        assert_eq!(p.timeout(), Duration::from_millis(1));
        assert!(!p.matches_preferred(8));
    }

    #[test]
    fn adaptive_preferred_sizes() {
        let p = DrainPolicy::default_adaptive();
        assert!(p.matches_preferred(8));
        assert!(p.matches_preferred(16));
        assert!(p.matches_preferred(32));
        assert!(p.matches_preferred(64));
        assert!(!p.matches_preferred(10));
        assert!(!p.matches_preferred(0));
    }

    #[test]
    fn adaptive_timeout_is_max_queue_delay() {
        let p = DrainPolicy::default_adaptive();
        assert_eq!(p.timeout(), Duration::from_micros(200));
    }

    #[test]
    fn adaptive_ema_updates() {
        let mut p = DrainPolicy::default_adaptive();
        p.adapt(32);
        if let DrainPolicy::Adaptive {
            ema_arrival_rate, ..
        } = &p
        {
            assert!(*ema_arrival_rate > 0.0);
        } else {
            panic!("expected adaptive");
        }

        // Multiple adaptations should converge toward the batch count
        for _ in 0..20 {
            p.adapt(16);
        }
        if let DrainPolicy::Adaptive {
            ema_arrival_rate, ..
        } = &p
        {
            assert!((*ema_arrival_rate - 16.0).abs() < 1.0);
        }
    }

    #[test]
    fn fixed_adapt_is_noop() {
        let mut p = DrainPolicy::default_fixed();
        p.adapt(100); // Should not panic or change anything
        assert_eq!(p.batch_size(), 64);
    }
}
