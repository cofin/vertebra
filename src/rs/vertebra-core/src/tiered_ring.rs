//! Tiered response ring configuration for bridge-mode multi-process transport.
//!
//! Four tiers allow responses of different sizes to use appropriately sized
//! ring slots, avoiding waste for small responses and fragmentation for large ones.

/// Configuration for a four-tier response ring system.
///
/// Each tier targets a different response size class:
/// - **Small**: headers-only or tiny JSON (4KB slots)
/// - **Medium**: typical API responses (64KB slots)
/// - **Large**: bulk data, moderate payloads (512KB slots)
/// - **XLarge**: file uploads, large serialized objects (2MB slots)
#[derive(Debug, Clone)]
pub struct TieredRingConfig {
    pub small: RingTier,
    pub medium: RingTier,
    pub large: RingTier,
    pub xlarge: RingTier,
}

/// Configuration for a single ring tier.
#[derive(Debug, Clone)]
pub struct RingTier {
    /// Size of each slot in bytes.
    pub slot_size: usize,
    /// Number of slots in the ring.
    pub slot_count: usize,
}

impl RingTier {
    /// Total memory footprint of this tier in bytes.
    pub fn total_bytes(&self) -> usize {
        self.slot_size * self.slot_count
    }
}

impl Default for TieredRingConfig {
    /// Default configuration: ~7.25MB total.
    ///
    /// - Small:  4KB   × 64 slots = 256KB
    /// - Medium: 64KB  × 16 slots = 1MB
    /// - Large:  512KB × 4 slots  = 2MB
    /// - XLarge: 2MB   × 2 slots  = 4MB
    fn default() -> Self {
        Self {
            small: RingTier {
                slot_size: 4 * 1024,
                slot_count: 64,
            },
            medium: RingTier {
                slot_size: 64 * 1024,
                slot_count: 16,
            },
            large: RingTier {
                slot_size: 512 * 1024,
                slot_count: 4,
            },
            xlarge: RingTier {
                slot_size: 2 * 1024 * 1024,
                slot_count: 2,
            },
        }
    }
}

impl TieredRingConfig {
    /// Total memory footprint across all tiers.
    pub fn total_bytes(&self) -> usize {
        self.small.total_bytes()
            + self.medium.total_bytes()
            + self.large.total_bytes()
            + self.xlarge.total_bytes()
    }

    /// Select the appropriate tier for a response of the given size.
    ///
    /// Returns `None` if the response exceeds even the xlarge tier's slot size.
    pub fn select_tier(&self, response_size: usize) -> Option<&RingTier> {
        if response_size <= self.small.slot_size {
            Some(&self.small)
        } else if response_size <= self.medium.slot_size {
            Some(&self.medium)
        } else if response_size <= self.large.slot_size {
            Some(&self.large)
        } else if response_size <= self.xlarge.slot_size {
            Some(&self.xlarge)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_total_bytes() {
        let config = TieredRingConfig::default();
        // 256KB + 1MB + 2MB + 4MB = 7.25MB = 7602176 bytes
        assert_eq!(
            config.total_bytes(),
            256 * 1024 + 1024 * 1024 + 2048 * 1024 + 4096 * 1024
        );
    }

    #[test]
    fn tier_selection() {
        let config = TieredRingConfig::default();
        assert_eq!(config.select_tier(100).unwrap().slot_size, 4 * 1024);
        assert_eq!(config.select_tier(4096).unwrap().slot_size, 4 * 1024);
        assert_eq!(config.select_tier(4097).unwrap().slot_size, 64 * 1024);
        assert_eq!(config.select_tier(65536).unwrap().slot_size, 64 * 1024);
        assert_eq!(config.select_tier(65537).unwrap().slot_size, 512 * 1024);
        assert_eq!(
            config.select_tier(512 * 1024 + 1).unwrap().slot_size,
            2 * 1024 * 1024
        );
        assert_eq!(
            config.select_tier(2 * 1024 * 1024).unwrap().slot_size,
            2 * 1024 * 1024
        );
        assert!(config.select_tier(2 * 1024 * 1024 + 1).is_none());
    }
}
