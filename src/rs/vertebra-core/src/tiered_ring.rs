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

    /// Usable payload capacity per slot, accounting for the slot header.
    pub fn payload_capacity(&self) -> usize {
        self.slot_size
            .saturating_sub(core::mem::size_of::<crate::buffer::SlotHeader>())
    }
}

impl Default for TieredRingConfig {
    /// Default configuration: 30 MiB total across tiers.
    ///
    /// - Small:  4 KiB   × 512 slots = 2 MiB (absorbs `c=256+` bursts without overflow)
    /// - Medium: 32 KiB  × 128 slots = 4 MiB
    /// - Large:  256 KiB × 32 slots  = 8 MiB
    /// - XLarge: 2 MiB   × 8 slots   = 16 MiB
    fn default() -> Self {
        Self {
            small: RingTier {
                slot_size: 4 * 1024,
                slot_count: 512,
            },
            medium: RingTier {
                slot_size: 32 * 1024,
                slot_count: 128,
            },
            large: RingTier {
                slot_size: 256 * 1024,
                slot_count: 32,
            },
            xlarge: RingTier {
                slot_size: 2 * 1024 * 1024,
                slot_count: 8,
            },
        }
    }
}

impl TieredRingConfig {
    /// Compact container configuration: 8 MiB total across tiers.
    ///
    /// - Small:  4 KiB   × 256 slots = 1 MiB
    /// - Medium: 32 KiB  × 32 slots  = 1 MiB
    /// - Large:  256 KiB × 8 slots   = 2 MiB
    /// - XLarge: 2 MiB   × 2 slots   = 4 MiB
    pub fn compact() -> Self {
        Self {
            small: RingTier {
                slot_size: 4 * 1024,
                slot_count: 256,
            },
            medium: RingTier {
                slot_size: 32 * 1024,
                slot_count: 32,
            },
            large: RingTier {
                slot_size: 256 * 1024,
                slot_count: 8,
            },
            xlarge: RingTier {
                slot_size: 2 * 1024 * 1024,
                slot_count: 2,
            },
        }
    }

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
        if response_size <= self.small.payload_capacity() {
            Some(&self.small)
        } else if response_size <= self.medium.payload_capacity() {
            Some(&self.medium)
        } else if response_size <= self.large.payload_capacity() {
            Some(&self.large)
        } else if response_size <= self.xlarge.payload_capacity() {
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
        // 2 MiB + 4 MiB + 8 MiB + 16 MiB = 30 MiB
        assert_eq!(
            config.total_bytes(),
            2 * 1024 * 1024 + 4 * 1024 * 1024 + 8 * 1024 * 1024 + 16 * 1024 * 1024
        );
    }

    #[test]
    fn tier_selection() {
        let config = TieredRingConfig::default();
        let small_cap = config.small.payload_capacity();
        let medium_cap = config.medium.payload_capacity();
        let large_cap = config.large.payload_capacity();
        let xlarge_cap = config.xlarge.payload_capacity();

        assert_eq!(config.select_tier(100).unwrap().slot_size, 4 * 1024);
        assert_eq!(config.select_tier(small_cap).unwrap().slot_size, 4 * 1024);
        assert_eq!(
            config.select_tier(small_cap + 1).unwrap().slot_size,
            32 * 1024
        );
        assert_eq!(config.select_tier(medium_cap).unwrap().slot_size, 32 * 1024);
        assert_eq!(
            config.select_tier(medium_cap + 1).unwrap().slot_size,
            256 * 1024
        );
        assert_eq!(config.select_tier(large_cap).unwrap().slot_size, 256 * 1024);
        assert_eq!(
            config.select_tier(large_cap + 1).unwrap().slot_size,
            2 * 1024 * 1024
        );
        assert_eq!(
            config.select_tier(xlarge_cap).unwrap().slot_size,
            2 * 1024 * 1024
        );
        assert!(config.select_tier(xlarge_cap + 1).is_none());
    }
}
