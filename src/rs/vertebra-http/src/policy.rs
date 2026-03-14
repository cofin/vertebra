//! Drain policy for the batch collector.
//!
//! The initial implementation uses a fixed policy (64 requests or 1ms timeout).
//! The adaptive policy is scaffolded here but activates only after benchmarks
//! confirm baseline behaviour.

use std::time::Duration;

/// Policy determining when a batch should be drained.
#[derive(Debug, Clone)]
pub enum DrainPolicy {
    /// Drain when `size` requests accumulate OR `timeout` elapses, whichever is first.
    Fixed {
        size: usize,
        timeout: Duration,
    },
    /// Adaptive: tighten the size limit under low load, relax it under high load.
    Adaptive {
        min_size: usize,
        max_size: usize,
        base_timeout: Duration,
        /// Current effective size (updated by the collector loop).
        current_size: usize,
    },
}

impl DrainPolicy {
    /// Create the default fixed policy.
    pub fn default_fixed() -> Self {
        Self::Fixed {
            size: 64,
            timeout: Duration::from_millis(1),
        }
    }

    /// Return the effective batch size cap for the current policy state.
    pub fn batch_size(&self) -> usize {
        match self {
            Self::Fixed { size, .. } => *size,
            Self::Adaptive { current_size, .. } => *current_size,
        }
    }

    /// Return the drain timeout for the current policy state.
    pub fn timeout(&self) -> Duration {
        match self {
            Self::Fixed { timeout, .. } => *timeout,
            Self::Adaptive { base_timeout, .. } => *base_timeout,
        }
    }

    /// Adjust the adaptive policy based on the number of requests seen in the
    /// last window. No-op for the fixed policy.
    pub fn adapt(&mut self, requests_in_window: usize) {
        if let Self::Adaptive {
            min_size,
            max_size,
            current_size,
            ..
        } = self
        {
            let target = (requests_in_window * 4 / 3).clamp(*min_size, *max_size);
            *current_size = target;
        }
    }
}

impl Default for DrainPolicy {
    fn default() -> Self {
        Self::default_fixed()
    }
}
