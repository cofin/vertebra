//! Shared memory layout and ring buffer metadata.

use core::sync::atomic::{AtomicU32, AtomicU64};

/// Cache line size used to isolate hot atomics.
///
/// Keep this in sync with `CacheLine<T>` alignment below.
#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub const CACHELINE_BYTES: usize = 128;
#[cfg(not(all(target_arch = "aarch64", target_os = "macos")))]
pub const CACHELINE_BYTES: usize = 64;

/// ABI version for shared memory layouts.
pub const ABI_VERSION: u32 = 3;

/// Magic marker for validating mapped regions ("VERTEBRA").
pub const MAGIC: u64 = u64::from_le_bytes(*b"VERTEBRA");

/// Slot alignment for payloads (bytes).
pub const SLOT_ALIGN: usize = 64;

/// Shared memory region state.
#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RegionState {
    Ready = 1,
}

/// Cache-line aligned wrapper to avoid false sharing.
#[cfg_attr(all(target_arch = "aarch64", target_os = "macos"), repr(align(128)))]
#[cfg_attr(
    not(all(target_arch = "aarch64", target_os = "macos")),
    repr(align(64))
)]
#[repr(C)]
#[derive(Debug, Default)]
pub struct CacheLine<T>(pub T);

/// Fixed header layout for all shared-memory regions.
///
/// The header is placed at the start of the mapping followed by the ring buffer.
#[repr(C)]
pub struct ShmHeader {
    pub magic: u64,
    pub abi_version: u32,
    pub flags: u32,
    pub total_size: u64,
    pub ring_offset: u64,
    pub ring_size: u64,
    pub generation: AtomicU32,
    pub state: AtomicU32,
    pub creator_pid: u32,
    pub reserved0: u32,
    pub creator_epoch: u64,
    pub reserved1: [u8; 32],
    pub read_head: CacheLine<AtomicU64>,
    pub write_head: CacheLine<AtomicU64>,
    pub wait_read: CacheLine<AtomicU32>,
    pub wait_write: CacheLine<AtomicU32>,
    /// Number of threads blocked waiting to send (ring full).
    /// Used for conditional wake optimization - only wake if waiters > 0.
    pub send_waiters: CacheLine<AtomicU32>,
    /// Number of threads blocked waiting to recv (ring empty).
    /// Used for conditional wake optimization - only wake if waiters > 0.
    pub recv_waiters: CacheLine<AtomicU32>,
    /// Dekker consumer sleep flag (`0` = awake/polling, `1` = sleeping on notifier).
    pub consumer_sleeping: CacheLine<AtomicU32>,
    /// Active tier bitmask (`bit 0..3` on `TieredRingSet.small`) and auxiliary ring flags.
    pub active_flags: CacheLine<AtomicU32>,
}

/// Channel configuration for the shared-memory ring.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ChannelMode {
    Spsc,
    Mpmc,
}

/// Slot header prefix (fixed-size).
#[repr(C)]
pub struct SlotHeader {
    pub len: u32,
    pub flags: u32,
}

/// Describes a ring layout derived from slot size + slot count.
#[derive(Debug, Copy, Clone)]
pub struct RingLayout {
    pub mode: ChannelMode,
    pub slot_size: usize,
    pub slots: usize,
    pub seq_size: usize,
    pub ring_size: usize,
    pub header_size: usize,
    pub total_size: usize,
}

impl RingLayout {
    pub fn new(slot_size: usize, slots: usize, mode: ChannelMode) -> Self {
        let seq_size = match mode {
            ChannelMode::Spsc => 0,
            ChannelMode::Mpmc => slots * core::mem::size_of::<CacheLine<AtomicU64>>(),
        };
        let ring_size = slot_size * slots;
        let header_size = align_up(
            core::mem::size_of::<ShmHeader>(),
            CACHELINE_BYTES.max(SLOT_ALIGN),
        );
        let total_size = header_size + seq_size + ring_size;
        Self {
            mode,
            slot_size,
            slots,
            seq_size,
            ring_size,
            header_size,
            total_size,
        }
    }

    pub fn is_power_of_two(&self) -> bool {
        self.slots.is_power_of_two()
    }

    pub fn seq_offset(&self) -> usize {
        self.header_size
    }

    pub fn ring_offset(&self) -> usize {
        self.header_size + self.seq_size
    }
}

pub fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_alignment_matches_cacheline() {
        assert_eq!(core::mem::align_of::<CacheLine<u8>>(), CACHELINE_BYTES);
    }

    #[test]
    fn ring_layout_mpmc_includes_sequences() {
        let slots = 64;
        let slot_size = 256;
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Mpmc);
        let expected_seq_size = slots * core::mem::size_of::<CacheLine<AtomicU64>>();
        assert_eq!(layout.seq_size, expected_seq_size);
        assert_eq!(
            layout.total_size,
            layout.header_size + expected_seq_size + (slot_size * slots)
        );
    }
}
