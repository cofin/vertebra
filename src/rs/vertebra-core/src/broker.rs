//! Shared-memory atomic lease broker and prepared statement cache.
//!
//! Provides cross-process database connection admission control, crash lease
//! reclamation, and optimistic seqlock-guarded statement plan caching.

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::layout::CacheLine;
use crate::platform::WaitWord;
use crate::shm::ShmRegion;
#[cfg(unix)]
use crate::shm::{NamedAttach, ShmError};

pub const BROKER_MAGIC: u64 = u64::from_le_bytes(*b"VTBBRKR1");
pub const BROKER_ABI_VERSION: u32 = 2;
pub const MAX_BROKER_SLOTS: u32 = 4096;
pub const MAX_STATEMENT_CAPACITY: u32 = 1 << 20;
pub const MAX_POOL_PARTITIONS: usize = 64;
const NO_PARTITION: u32 = u32::MAX;
const PARTITION_EMPTY: u32 = 0;
const PARTITION_INIT: u32 = 1;
const PARTITION_READY: u32 = 2;

pub const SLOT_STATE_FREE: u32 = 0;
pub const SLOT_STATE_PRIMARY: u32 = 1;
pub const SLOT_STATE_OVERFLOW: u32 = 2;
const SLOT_STATE_CLAIMING: u32 = 3;
pub const SLOT_STATE_QUARANTINED: u32 = 4;
const SLOT_STATE_RECLAIMING: u32 = 5;

/// Public lifecycle state of a broker lease slot.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    Free = 0,
    Leased = 1,
    Quarantined = 2,
}

/// Quarantined lease metadata recovered when reclaiming a dead worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimedLease {
    pub slot_index: u32,
    pub generation: u64,
    pub pool_id: Option<u16>,
}

/// Per-partition lease counters stored atomically inside shared memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PartitionStats {
    pub pool_id: u16,
    pub pool_size: u32,
    pub max_overflow: u32,
    pub active_leases: u32,
    pub quarantined: u32,
    pub total_acquired: u64,
    pub total_released: u64,
    pub total_reclaimed: u64,
    pub total_quarantined: u64,
    pub total_disposed: u64,
}

pub const STMT_SLOT_PAYLOAD_BYTES: usize = 224;
pub const CLOCK_REF_BIT: u32 = 1 << 8;

pub const DEFAULT_CHANNEL_CAPACITY: u32 = 64;
pub const CHAN_SLOT_FREE: u32 = 0;
pub const CHAN_SLOT_WRITING: u32 = 1;
pub const CHAN_SLOT_READY: u32 = 2;
pub const CHAN_MAX_NAME_BYTES: usize = 64;
pub const CHAN_MAX_LEASE_ID_BYTES: usize = 32;
pub const CHAN_MAX_PAYLOAD_BYTES: usize = 256;

struct WaitGuard<'a>(&'a AtomicU32);

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleaseKind {
    Release,
    Reclaim,
    DisposeQuarantined,
    Expire,
}

pub const DIALECT_TAG_POSTGRES: u16 = 1;
pub const DIALECT_TAG_MYSQL: u16 = 2;
pub const DIALECT_TAG_SQLITE: u16 = 3;
pub const DIALECT_TAG_ORACLE: u16 = 4;
pub const DIALECT_TAG_MSSQL: u16 = 5;
pub const DIALECT_TAG_DUCKDB: u16 = 6;
pub const DIALECT_TAG_BIGQUERY: u16 = 7;
pub const DIALECT_TAG_SPANNER: u16 = 8;
pub const DIALECT_TAG_SPANGRES: u16 = 9;

/// Compute a deterministic unseeded XXH3-64 digest across processes.
#[inline]
pub fn stable_hash64(bytes: &[u8]) -> u64 {
    twox_hash::XxHash3_64::oneshot(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BrokerError {
    #[error("invalid broker capacity specified")]
    InvalidCapacity,
    #[error("shared memory region is too small: expected {expected} bytes, got {actual} bytes")]
    RegionTooSmall { expected: usize, actual: usize },
    #[error(
        "misaligned shared-memory broker section at offset {offset} (requires {align}-byte alignment)"
    )]
    MisalignedRegion { offset: usize, align: usize },
    #[error("invalid broker magic header: {0:#x}")]
    InvalidMagic(u64),
    #[error("unsupported broker ABI version: {0}")]
    UnsupportedAbi(u32),
    #[error("connection pool is exhausted and timeout expired")]
    PoolExhausted,
    #[error("connection lease not found")]
    LeaseNotFound,
    #[error("lease generation mismatch during release or reap")]
    GenerationMismatch,
    #[error("stale lease generation")]
    StaleGeneration,
    #[error("invalid lease state for operation")]
    InvalidState,
    #[error("partition registry capacity exhausted")]
    PartitionsExhausted,
    #[error("invalid pool partition id: {0}")]
    InvalidPartition(u16),
    #[error("shared memory segment in use by owner pid {owner_pid}")]
    SegmentInUse { owner_pid: u32 },
    #[error("shared memory segment not found")]
    NotFound,
    #[error("shared memory permission denied")]
    PermissionDenied,
    #[error("timed out waiting for shared memory initialization")]
    InitTimeout,
    #[error("shared memory system error: errno {0}")]
    SystemError(i32),
}

/// Partition admission limits registered inside [`ShmLeaseTable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolPartitionSpec {
    pub pool_key_hash: u64,
    pub pool_size: u32,
    pub max_overflow: u32,
}

#[repr(C, align(64))]
pub struct ShmPartitionEntry {
    pub pool_key_hash: AtomicU64,
    pub state: AtomicU32,
    pub pool_size: AtomicU32,
    pub max_overflow: AtomicU32,
    pub active_leases: AtomicU32,
    pub quarantined: AtomicU32,
    pub total_acquired: AtomicU32,
    pub total_released: AtomicU32,
    pub total_reclaimed: AtomicU32,
    pub total_quarantined: AtomicU32,
    pub total_disposed: AtomicU32,
    pub _pad: [u32; 4],
}

const _: () = assert!(std::mem::size_of::<ShmPartitionEntry>() == 64);
const _: () = assert!(std::mem::align_of::<ShmPartitionEntry>() == 64);

#[cfg(unix)]
fn map_shm_error(err: ShmError, name: &str) -> BrokerError {
    match err {
        ShmError::InUse { owner_pid } => BrokerError::SegmentInUse { owner_pid },
        ShmError::InitTimeout => BrokerError::InitTimeout,
        ShmError::InvalidName | ShmError::InvalidSize | ShmError::Name(_) | ShmError::MapFailed => {
            #[cfg(unix)]
            if let Some(pid) = crate::shm::probe_owner_pid(name) {
                return BrokerError::SegmentInUse { owner_pid: pid };
            }
            let _ = name;
            BrokerError::InvalidCapacity
        }
        ShmError::Sys(io_err) => match io_err.kind() {
            std::io::ErrorKind::NotFound => BrokerError::NotFound,
            std::io::ErrorKind::PermissionDenied => BrokerError::PermissionDenied,
            std::io::ErrorKind::AlreadyExists => {
                #[cfg(unix)]
                if let Some(pid) = crate::shm::probe_owner_pid(name) {
                    return BrokerError::SegmentInUse { owner_pid: pid };
                }
                BrokerError::SystemError(libc_errno(&io_err))
            }
            _ => BrokerError::SystemError(libc_errno(&io_err)),
        },
    }
}

#[cfg(unix)]
fn libc_errno(err: &std::io::Error) -> i32 {
    err.raw_os_error().unwrap_or(5)
}

const MAX_STUCK_SPINS: u32 = 2048;

fn checked_aligned_section<T>(
    base: *const u8,
    offset: usize,
    section_bytes: usize,
    region_len: usize,
) -> Result<*mut T, BrokerError> {
    let min_bytes = section_bytes.max(std::mem::size_of::<T>());
    let end = offset
        .checked_add(min_bytes)
        .ok_or(BrokerError::RegionTooSmall {
            expected: usize::MAX,
            actual: region_len,
        })?;
    let align = std::mem::align_of::<T>();
    let Some(addr) = (base as usize).checked_add(offset) else {
        return Err(BrokerError::MisalignedRegion { offset, align });
    };
    if !offset.is_multiple_of(align) || !addr.is_multiple_of(align) {
        return Err(BrokerError::MisalignedRegion { offset, align });
    }
    if end > region_len {
        return Err(BrokerError::RegionTooSmall {
            expected: end,
            actual: region_len,
        });
    }
    // SAFETY: `offset + section_bytes <= region_len`, `base + offset` does not overflow,
    // and both `offset` and `base + offset` satisfy `align_of::<T>()`.
    Ok(unsafe { base.add(offset) as *mut T })
}

#[inline]
pub fn pack_state_pid(state: u32, pid: u32) -> u64 {
    ((state as u64) << 32) | (pid as u64)
}

#[inline]
pub fn unpack_state_pid(raw: u64) -> (u32, u32) {
    ((raw >> 32) as u32, raw as u32)
}

#[inline]
pub fn monotonic_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[repr(C)]
#[cfg_attr(target_arch = "aarch64", repr(align(128)))]
#[cfg_attr(not(target_arch = "aarch64"), repr(align(64)))]
pub struct ShmLeaseHeader {
    // Cache line 0 (64B): Immutable configuration
    pub magic: u64,
    pub abi_version: u32,
    pub pool_size: u32,
    pub max_overflow: u32,
    pub stmt_capacity: u32,
    pub stmt_offset: u64,
    pub total_size: u64,
    pub channel_capacity: u32,
    pub _pad0_reserved: u32,
    pub channel_offset: u64,
    pub _pad0: [u8; 8],

    // Cache line 1 (64B): Active lease counter
    pub active_leases: CacheLine<AtomicU32>,

    // Cache line 2 (64B): Waiters counter
    pub waiters: CacheLine<AtomicU32>,

    // Cache line 3 (64B): Futex wake sequence
    pub wake_seq: CacheLine<WaitWord>,

    // Cache line 4 (64B): Round-robin acquisition cursor
    pub rr_cursor: CacheLine<AtomicU32>,

    // Cache line 5 (64B): Cumulative telemetry counters
    pub total_acquired: AtomicU64,
    pub total_released: AtomicU64,
    pub total_expired: AtomicU64,
    pub total_timeouts: AtomicU64,
    pub total_reclaimed: AtomicU64,
    pub total_quarantined: AtomicU64,
    pub total_disposed: AtomicU64,
    pub _pad1: [u8; 8],

    // Cache line 6 (64B): Partition directory count
    pub partition_count: CacheLine<AtomicU32>,

    // Cache lines 7..70 (4096B): Partition directory entries
    pub partitions: [ShmPartitionEntry; MAX_POOL_PARTITIONS],
}

#[cfg(target_arch = "aarch64")]
const _: () = assert!(std::mem::size_of::<ShmLeaseHeader>() == 4992);
#[cfg(not(target_arch = "aarch64"))]
const _: () = assert!(std::mem::size_of::<ShmLeaseHeader>() == 4544);
const _: () = assert!(std::mem::align_of::<ShmLeaseHeader>() == crate::layout::CACHELINE_BYTES);

#[repr(C, align(64))]
pub struct LeaseSlot {
    pub state_and_pid: AtomicU64,
    pub generation: AtomicU64,
    pub acquired_ns: AtomicU64,
    pub expiry_ns: AtomicU64,
    pub bind_key_hash: AtomicU64,
    pub last_heartbeat_ns: AtomicU64,
    pub ttl_ns: AtomicU64,
    pub partition_id: AtomicU32,
    pub _pad: u32,
}

const _: () = assert!(std::mem::size_of::<LeaseSlot>() == 64);
const _: () = assert!(std::mem::align_of::<LeaseSlot>() == 64);

#[repr(C, align(64))]
pub struct ShmChannelHeader {
    pub capacity: u32,
    pub _pad0: u32,
    pub write_seq: AtomicU64,
    pub total_published: AtomicU64,
    pub total_drained: AtomicU64,
    pub total_dropped: AtomicU64,
    pub _pad1: [u8; 24],
}

const _: () = assert!(std::mem::size_of::<ShmChannelHeader>() == 64);
const _: () = assert!(std::mem::align_of::<ShmChannelHeader>() == 64);

#[repr(C, align(64))]
pub struct ShmChannelSlot {
    pub state: AtomicU32,
    pub channel_len: AtomicU16,
    pub payload_len: AtomicU16,
    pub lease_id_len: AtomicU16,
    pub _pad: [u8; 6],
    pub seq: AtomicU64,
    pub timestamp_ns: AtomicU64,
    pub channel_words: [AtomicU64; 8],
    pub lease_id_words: [AtomicU64; 4],
    pub payload_words: [AtomicU64; 32],
}

const _: () = assert!(std::mem::size_of::<ShmChannelSlot>() == 384);
const _: () = assert!(std::mem::align_of::<ShmChannelSlot>() == 64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelFrame {
    pub channel: Vec<u8>,
    pub payload: Vec<u8>,
    pub timestamp_ns: u64,
    pub lease_id: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseGrant {
    pub slot_index: u32,
    pub generation: u64,
    pub worker_pid: u32,
    pub is_overflow: bool,
    pub acquired_ns: u64,
    pub expiry_ns: u64,
    pub bind_key_hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LeaseTableStats {
    pub pool_size: u32,
    pub max_overflow: u32,
    pub active_leases: u32,
    pub primary_in_use: u32,
    pub overflow_in_use: u32,
    pub waiting_threads: u32,
    pub total_acquired: u64,
    pub total_released: u64,
    pub total_expired: u64,
    pub total_timeouts: u64,
    pub total_reclaimed: u64,
    pub quarantined: u32,
    pub total_quarantined: u64,
    pub total_disposed: u64,
}

pub struct ShmLeaseTable {
    region: Arc<ShmRegion>,
    header: *mut ShmLeaseHeader,
    slots: *mut LeaseSlot,
}

// SAFETY: All fields accessed via ShmLeaseHeader and LeaseSlot are atomic or immutable.
unsafe impl Send for ShmLeaseTable {}
// SAFETY: Shared references only perform atomic operations or read immutable header metadata.
unsafe impl Sync for ShmLeaseTable {}

impl ShmLeaseTable {
    pub fn validate_capacities(
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
        require_stmts: bool,
    ) -> Result<u32, BrokerError> {
        if pool_size == 0
            || pool_size
                .checked_add(max_overflow)
                .is_none_or(|total| total > MAX_BROKER_SLOTS)
        {
            return Err(BrokerError::InvalidCapacity);
        }
        if stmt_capacity == 0 && !require_stmts {
            return Ok(0);
        }
        let base = stmt_capacity.max(4);
        if base > MAX_STATEMENT_CAPACITY {
            return Err(BrokerError::InvalidCapacity);
        }
        base.checked_next_power_of_two()
            .ok_or(BrokerError::InvalidCapacity)
    }

    pub const fn lease_section_size(pool_size: u32, max_overflow: u32) -> usize {
        let total = (pool_size as usize).saturating_add(max_overflow as usize);
        let raw = std::mem::size_of::<ShmLeaseHeader>()
            .saturating_add(total.saturating_mul(std::mem::size_of::<LeaseSlot>()));
        raw.saturating_add(63) & !63
    }

    pub const fn channel_section_size(capacity: u32) -> usize {
        let raw = std::mem::size_of::<ShmChannelHeader>().saturating_add(
            (capacity as usize).saturating_mul(std::mem::size_of::<ShmChannelSlot>()),
        );
        raw.saturating_add(63) & !63
    }

    pub const fn required_size(pool_size: u32, max_overflow: u32) -> usize {
        Self::lease_section_size(pool_size, max_overflow)
            .saturating_add(Self::channel_section_size(DEFAULT_CHANNEL_CAPACITY))
    }

    pub const fn required_size_with_statements(
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> usize {
        let base = Self::required_size(pool_size, max_overflow);
        if stmt_capacity == 0 {
            base
        } else {
            base.saturating_add(ShmStatementCache::required_size(stmt_capacity))
        }
    }

    pub fn init_in_region(
        region: ShmRegion,
        pool_size: u32,
        max_overflow: u32,
    ) -> Result<Self, BrokerError> {
        Self::init_in_region_with_statements(region, pool_size, max_overflow, 0)
    }

    pub fn init_in_region_with_statements(
        mut region: ShmRegion,
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> Result<Self, BrokerError> {
        Self::init_region_slice_with_statements(
            &mut region,
            pool_size,
            max_overflow,
            stmt_capacity,
        )?;
        Self::attach_region(region)
    }

    fn init_region_slice_with_statements(
        region: &mut ShmRegion,
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> Result<(), BrokerError> {
        let eff_stmt_cap_checked =
            Self::validate_capacities(pool_size, max_overflow, stmt_capacity, false)?;
        let req_size =
            Self::required_size_with_statements(pool_size, max_overflow, eff_stmt_cap_checked);
        let region_len = region.len();
        if region_len < req_size {
            return Err(BrokerError::RegionTooSmall {
                expected: req_size,
                actual: region_len,
            });
        }

        let base = region.as_ptr() as *const u8;
        let lease_bytes = Self::lease_section_size(pool_size, max_overflow);
        let header_ptr =
            checked_aligned_section::<ShmLeaseHeader>(base, 0, lease_bytes, region_len)?;
        let total_slots = (pool_size as usize)
            .checked_add(max_overflow as usize)
            .ok_or(BrokerError::InvalidCapacity)?;
        let slots_offset = std::mem::size_of::<ShmLeaseHeader>();
        let slots_bytes = total_slots
            .checked_mul(std::mem::size_of::<LeaseSlot>())
            .ok_or(BrokerError::InvalidCapacity)?;
        let slots_ptr =
            checked_aligned_section::<LeaseSlot>(base, slots_offset, slots_bytes, region_len)?;

        let (eff_stmt_cap, stmt_offset_usize, stmt_bytes, stmt_hdr_ptr) =
            if eff_stmt_cap_checked > 0 {
                let cap = eff_stmt_cap_checked;
                let offset = lease_bytes;
                let bytes = ShmStatementCache::required_size(cap);
                let ptr =
                    checked_aligned_section::<ShmStatementHeader>(base, offset, bytes, region_len)?;
                (cap, offset, bytes, Some(ptr))
            } else {
                (0, 0, 0, None)
            };

        let channel_offset_usize = lease_bytes
            .checked_add(stmt_bytes)
            .ok_or(BrokerError::InvalidCapacity)?;
        let channel_bytes = Self::channel_section_size(DEFAULT_CHANNEL_CAPACITY);
        let chan_hdr_ptr = checked_aligned_section::<ShmChannelHeader>(
            base,
            channel_offset_usize,
            channel_bytes,
            region_len,
        )?;

        // SAFETY: `region` has exclusive access during initialization and spans at least `req_size` bytes.
        unsafe {
            let slice = region.as_mut_slice();
            slice[..req_size].fill(0);
        }

        if let Some(stmt_hdr_ptr) = stmt_hdr_ptr {
            // SAFETY: `stmt_hdr_ptr` was validated by `checked_aligned_section` and zero-initialized above.
            unsafe {
                let shdr = &mut *stmt_hdr_ptr;
                shdr.capacity = eff_stmt_cap;
                shdr.set_mask = (eff_stmt_cap / 4) - 1;
            }
        }

        // SAFETY: `chan_hdr_ptr` was validated by `checked_aligned_section` and zero-initialized above.
        unsafe {
            let chdr = &mut *chan_hdr_ptr;
            chdr.capacity = DEFAULT_CHANNEL_CAPACITY;
            chdr.write_seq.store(0, Ordering::Release);
            chdr.total_published.store(0, Ordering::Release);
            chdr.total_drained.store(0, Ordering::Release);
            chdr.total_dropped.store(0, Ordering::Release);
        }

        // SAFETY: `header_ptr` was validated by `checked_aligned_section` and zero-initialized above.
        unsafe {
            let hdr = &mut *header_ptr;
            hdr.magic = BROKER_MAGIC;
            hdr.abi_version = BROKER_ABI_VERSION;
            hdr.pool_size = pool_size;
            hdr.max_overflow = max_overflow;
            hdr.stmt_capacity = eff_stmt_cap;
            hdr.stmt_offset = stmt_offset_usize as u64;
            hdr.total_size = region_len as u64;
            hdr.channel_capacity = DEFAULT_CHANNEL_CAPACITY;
            hdr.channel_offset = channel_offset_usize as u64;
            hdr.active_leases.0.store(0, Ordering::Release);
            hdr.waiters.0.store(0, Ordering::Release);
            hdr.wake_seq.0.store(0);
            hdr.rr_cursor.0.store(0, Ordering::Release);
            hdr.partition_count.0.store(0, Ordering::Release);
        }

        for idx in 0..total_slots {
            // SAFETY: `slots_ptr` was validated for `total_slots` `LeaseSlot` entries within `req_size`.
            unsafe {
                (&*slots_ptr.add(idx))
                    .partition_id
                    .store(NO_PARTITION, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    pub fn attach_region(region: ShmRegion) -> Result<Self, BrokerError> {
        let region_len = region.len();
        let base = region.as_ptr() as *const u8;
        let header_ptr = checked_aligned_section::<ShmLeaseHeader>(
            base,
            0,
            std::mem::size_of::<ShmLeaseHeader>(),
            region_len,
        )?;
        let live_owner = region
            .owner_alive()
            .then(|| region.owner_pid().unwrap_or(0));
        // SAFETY: `header_ptr` was validated for bounds and alignment by `checked_aligned_section`.
        let (pool_size, max_overflow, stmt_capacity) = unsafe {
            let hdr = &*header_ptr;
            if hdr.magic != BROKER_MAGIC {
                if let Some(owner_pid) = live_owner {
                    return Err(BrokerError::SegmentInUse { owner_pid });
                }
                return Err(BrokerError::InvalidMagic(hdr.magic));
            }
            if hdr.abi_version != BROKER_ABI_VERSION {
                if let Some(owner_pid) = live_owner {
                    return Err(BrokerError::SegmentInUse { owner_pid });
                }
                return Err(BrokerError::UnsupportedAbi(hdr.abi_version));
            }
            if Self::validate_capacities(hdr.pool_size, hdr.max_overflow, hdr.stmt_capacity, false)
                .is_err()
            {
                if let Some(owner_pid) = live_owner {
                    return Err(BrokerError::SegmentInUse { owner_pid });
                }
                return Err(BrokerError::InvalidCapacity);
            }
            (hdr.pool_size, hdr.max_overflow, hdr.stmt_capacity)
        };

        let req_size = Self::required_size_with_statements(pool_size, max_overflow, stmt_capacity);
        if region_len < req_size {
            if let Some(owner_pid) = live_owner {
                return Err(BrokerError::SegmentInUse { owner_pid });
            }
            return Err(BrokerError::RegionTooSmall {
                expected: req_size,
                actual: region_len,
            });
        }

        let total_slots = (pool_size as usize)
            .checked_add(max_overflow as usize)
            .ok_or(BrokerError::InvalidCapacity)?;
        let slots_offset = std::mem::size_of::<ShmLeaseHeader>();
        let slots_bytes = total_slots
            .checked_mul(std::mem::size_of::<LeaseSlot>())
            .ok_or(BrokerError::InvalidCapacity)?;
        let slots_ptr =
            checked_aligned_section::<LeaseSlot>(base, slots_offset, slots_bytes, region_len)?;

        Ok(Self {
            region: Arc::new(region),
            header: header_ptr,
            slots: slots_ptr,
        })
    }

    #[cfg(unix)]
    pub fn create_with_statements(
        name: &str,
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> Result<(Self, ShmStatementCache), BrokerError> {
        let eff_cap = Self::validate_capacities(pool_size, max_overflow, stmt_capacity, true)?;
        let req_size = Self::required_size_with_statements(pool_size, max_overflow, eff_cap);
        let region = ShmRegion::create_named_with(name, req_size, |region| {
            Self::init_region_slice_with_statements(region, pool_size, max_overflow, eff_cap)
                .map_err(|_| ShmError::InvalidSize)
        })
        .map_err(|e| map_shm_error(e, name))?;
        let table = Self::attach_region(region)?;
        let cache = table
            .statement_cache()
            .ok_or(BrokerError::InvalidCapacity)?;
        Ok((table, cache))
    }

    #[cfg(unix)]
    pub fn open_with_statements(name: &str) -> Result<(Self, ShmStatementCache), BrokerError> {
        Self::open_with_statements_internal(name, (None, None, None))
    }

    #[cfg(unix)]
    pub fn open_with_statements_matching(
        name: &str,
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> Result<(Self, ShmStatementCache), BrokerError> {
        Self::open_with_statements_internal(
            name,
            (Some(pool_size), Some(max_overflow), Some(stmt_capacity)),
        )
    }

    #[cfg(unix)]
    pub fn open_with_statements_matching_opt(
        name: &str,
        pool_size: Option<u32>,
        max_overflow: Option<u32>,
        stmt_capacity: Option<u32>,
    ) -> Result<(Self, ShmStatementCache), BrokerError> {
        Self::open_with_statements_internal(name, (pool_size, max_overflow, stmt_capacity))
    }

    #[cfg(unix)]
    fn open_with_statements_internal(
        name: &str,
        expected: (Option<u32>, Option<u32>, Option<u32>),
    ) -> Result<(Self, ShmStatementCache), BrokerError> {
        let probe = ShmRegion::open_named(name, std::mem::size_of::<ShmLeaseHeader>())
            .map_err(|e| map_shm_error(e, name))?;
        let live_owner = probe.owner_alive().then(|| probe.owner_pid().unwrap_or(0));
        let header_ptr = checked_aligned_section::<ShmLeaseHeader>(
            probe.as_ptr() as *const u8,
            0,
            std::mem::size_of::<ShmLeaseHeader>(),
            probe.len(),
        )?;
        // SAFETY: `header_ptr` was validated for bounds and alignment by `checked_aligned_section`.
        let total_size = unsafe {
            let hdr = &*header_ptr;
            if hdr.magic != BROKER_MAGIC {
                if let Some(owner_pid) = live_owner {
                    return Err(BrokerError::SegmentInUse { owner_pid });
                }
                return Err(BrokerError::InvalidMagic(hdr.magic));
            }
            if hdr.abi_version != BROKER_ABI_VERSION {
                if let Some(owner_pid) = live_owner {
                    return Err(BrokerError::SegmentInUse { owner_pid });
                }
                return Err(BrokerError::UnsupportedAbi(hdr.abi_version));
            }
            if Self::validate_capacities(hdr.pool_size, hdr.max_overflow, hdr.stmt_capacity, false)
                .is_err()
            {
                if let Some(owner_pid) = live_owner {
                    return Err(BrokerError::SegmentInUse { owner_pid });
                }
                return Err(BrokerError::InvalidCapacity);
            }
            let (exp_pool_opt, exp_ovf_opt, exp_stmt_opt) = expected;
            if let Some(exp_pool) = exp_pool_opt {
                if exp_pool == 0 || exp_pool > MAX_BROKER_SLOTS {
                    return Err(BrokerError::InvalidCapacity);
                }
                if hdr.pool_size != exp_pool {
                    if let Some(owner_pid) = live_owner {
                        return Err(BrokerError::SegmentInUse { owner_pid });
                    }
                    return Err(BrokerError::InvalidCapacity);
                }
            }
            if let Some(exp_ovf) = exp_ovf_opt {
                if exp_pool_opt
                    .unwrap_or(hdr.pool_size)
                    .checked_add(exp_ovf)
                    .is_none_or(|t| t > MAX_BROKER_SLOTS)
                {
                    return Err(BrokerError::InvalidCapacity);
                }
                if hdr.max_overflow != exp_ovf {
                    if let Some(owner_pid) = live_owner {
                        return Err(BrokerError::SegmentInUse { owner_pid });
                    }
                    return Err(BrokerError::InvalidCapacity);
                }
            }
            if let Some(exp_stmt) = exp_stmt_opt {
                let eff_stmt = Self::validate_capacities(
                    exp_pool_opt.unwrap_or(hdr.pool_size),
                    exp_ovf_opt.unwrap_or(hdr.max_overflow),
                    exp_stmt,
                    false,
                )?;
                if eff_stmt > 0 && hdr.stmt_capacity != eff_stmt {
                    if let Some(owner_pid) = live_owner {
                        return Err(BrokerError::SegmentInUse { owner_pid });
                    }
                    return Err(BrokerError::InvalidCapacity);
                }
            }
            let min_total = Self::required_size_with_statements(
                hdr.pool_size,
                hdr.max_overflow,
                hdr.stmt_capacity,
            );
            usize::try_from(hdr.total_size)
                .ok()
                .filter(|&sz| sz >= min_total)
                .ok_or(BrokerError::InvalidCapacity)?
        };
        drop(probe);
        let region = ShmRegion::open_named(name, total_size).map_err(|e| map_shm_error(e, name))?;
        let table = Self::attach_region(region)?;
        let cache = table
            .statement_cache()
            .ok_or(BrokerError::InvalidCapacity)?;
        Ok((table, cache))
    }

    #[cfg(unix)]
    pub fn open_or_create_with_statements(
        name: &str,
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> Result<(Self, ShmStatementCache, bool), BrokerError> {
        let eff_cap = Self::validate_capacities(pool_size, max_overflow, stmt_capacity, true)?;
        let req_size = Self::required_size_with_statements(pool_size, max_overflow, eff_cap);
        let attach = ShmRegion::open_or_create_named(name, req_size, |region| {
            Self::init_region_slice_with_statements(region, pool_size, max_overflow, eff_cap)
                .map_err(|_| ShmError::InvalidSize)
        })
        .map_err(|e| map_shm_error(e, name))?;

        let (region, created) = match attach {
            NamedAttach::Created(r) => (r, true),
            NamedAttach::Attached(r) => (r, false),
        };
        let table = Self::attach_region(region)?;
        if !created
            && (table.pool_size() != pool_size
                || table.max_overflow() != max_overflow
                || table.stmt_capacity() != eff_cap)
        {
            return Err(BrokerError::SegmentInUse {
                owner_pid: table.owner_pid().unwrap_or(0),
            });
        }
        let cache = table
            .statement_cache()
            .ok_or(BrokerError::InvalidCapacity)?;
        Ok((table, cache, created))
    }

    pub fn statement_cache(&self) -> Option<ShmStatementCache> {
        let hdr = self.header();
        if hdr.stmt_capacity == 0 || hdr.stmt_offset == 0 {
            return None;
        }
        ShmStatementCache::from_region_at_offset(
            Arc::clone(&self.region),
            hdr.stmt_offset as usize,
            hdr.stmt_capacity,
            false,
        )
        .ok()
    }

    #[inline]
    pub fn stmt_capacity(&self) -> u32 {
        self.header().stmt_capacity
    }

    #[inline]
    pub fn stmt_offset(&self) -> u64 {
        self.header().stmt_offset
    }

    #[inline]
    pub fn region_arc(&self) -> Arc<ShmRegion> {
        Arc::clone(&self.region)
    }

    #[inline]
    fn header(&self) -> &ShmLeaseHeader {
        // SAFETY: self.header points to the valid shared-memory region initialized on creation/attach.
        unsafe { &*self.header }
    }

    #[inline]
    fn slot(&self, index: u32) -> &LeaseSlot {
        // SAFETY: index is bounds-checked against total_capacity.
        unsafe { &*self.slots.add(index as usize) }
    }

    #[inline]
    pub fn pool_size(&self) -> u32 {
        self.header().pool_size
    }

    #[inline]
    pub fn max_overflow(&self) -> u32 {
        self.header().max_overflow
    }

    #[inline]
    pub fn total_capacity(&self) -> u32 {
        self.header().pool_size + self.header().max_overflow
    }

    #[inline]
    pub fn region_size(&self) -> usize {
        self.region.len()
    }

    #[inline]
    pub fn region_handle(&self) -> Option<crate::shm::ShmHandle> {
        self.region.handle()
    }

    #[inline]
    pub fn owner_pid(&self) -> Option<u32> {
        self.region.owner_pid()
    }

    #[inline]
    pub fn owner_alive(&self) -> bool {
        self.region.owner_alive()
    }

    #[cfg(unix)]
    pub fn unlink(&self) -> bool {
        self.region.unlink_now()
    }

    #[cfg(not(unix))]
    pub fn unlink(&self) -> bool {
        false
    }

    /// Idempotently register a pool partition keyed by `spec.pool_key_hash`.
    pub fn register_partition(&self, spec: PoolPartitionSpec) -> Result<u16, BrokerError> {
        if spec.pool_size == 0
            || spec.pool_size.saturating_add(spec.max_overflow) > MAX_BROKER_SLOTS
        {
            return Err(BrokerError::InvalidCapacity);
        }
        let hdr = self.header();
        let deadline = Instant::now() + Duration::from_millis(200);
        'outer: loop {
            let mut saw_init = false;
            for (idx, entry) in hdr.partitions.iter().enumerate() {
                match entry.state.load(Ordering::Acquire) {
                    PARTITION_READY => {
                        if entry.pool_key_hash.load(Ordering::Relaxed) == spec.pool_key_hash {
                            return Ok(idx as u16);
                        }
                    }
                    PARTITION_INIT => {
                        saw_init = true;
                    }
                    _ => {}
                }
            }
            if saw_init {
                if Instant::now() >= deadline {
                    return Err(BrokerError::InitTimeout);
                }
                std::thread::yield_now();
                continue;
            }

            let mut claimed = None;
            for (idx, entry) in hdr.partitions.iter().enumerate() {
                if entry
                    .state
                    .compare_exchange(
                        PARTITION_EMPTY,
                        PARTITION_INIT,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    claimed = Some((idx, entry));
                    break;
                }
            }

            let Some((idx, entry)) = claimed else {
                let pending = hdr
                    .partitions
                    .iter()
                    .any(|e| e.state.load(Ordering::SeqCst) == PARTITION_INIT);
                if pending {
                    if Instant::now() >= deadline {
                        return Err(BrokerError::InitTimeout);
                    }
                    std::thread::yield_now();
                    continue;
                }
                return Err(BrokerError::PartitionsExhausted);
            };

            if (0..idx)
                .any(|other| hdr.partitions[other].state.load(Ordering::SeqCst) == PARTITION_INIT)
            {
                entry.state.store(PARTITION_EMPTY, Ordering::Release);
                if Instant::now() >= deadline {
                    return Err(BrokerError::InitTimeout);
                }
                std::thread::yield_now();
                continue 'outer;
            }

            while ((idx + 1)..MAX_POOL_PARTITIONS)
                .any(|other| hdr.partitions[other].state.load(Ordering::SeqCst) == PARTITION_INIT)
            {
                if Instant::now() >= deadline {
                    entry.state.store(PARTITION_EMPTY, Ordering::Release);
                    return Err(BrokerError::InitTimeout);
                }
                std::hint::spin_loop();
                std::thread::yield_now();
            }

            let duplicate = hdr
                .partitions
                .iter()
                .enumerate()
                .find_map(|(other_idx, other)| {
                    (other_idx != idx
                        && other.state.load(Ordering::Acquire) == PARTITION_READY
                        && other.pool_key_hash.load(Ordering::Relaxed) == spec.pool_key_hash)
                        .then_some(other_idx as u16)
                });
            if let Some(existing_id) = duplicate {
                entry.state.store(PARTITION_EMPTY, Ordering::Release);
                return Ok(existing_id);
            }

            entry
                .pool_key_hash
                .store(spec.pool_key_hash, Ordering::Relaxed);
            entry.pool_size.store(spec.pool_size, Ordering::Relaxed);
            entry
                .max_overflow
                .store(spec.max_overflow, Ordering::Relaxed);
            entry.active_leases.store(0, Ordering::Relaxed);
            entry.quarantined.store(0, Ordering::Relaxed);
            entry.total_acquired.store(0, Ordering::Relaxed);
            entry.total_released.store(0, Ordering::Relaxed);
            entry.total_reclaimed.store(0, Ordering::Relaxed);
            entry.total_quarantined.store(0, Ordering::Relaxed);
            entry.total_disposed.store(0, Ordering::Relaxed);
            entry.state.store(PARTITION_READY, Ordering::Release);
            hdr.partition_count.0.fetch_add(1, Ordering::Relaxed);
            return Ok(idx as u16);
        }
    }

    /// Look up the partition slot matching `pool_key_hash` in the shared header.
    pub fn lookup_partition(&self, pool_key_hash: u64) -> Option<u16> {
        let hdr = self.header();
        for (idx, entry) in hdr.partitions.iter().enumerate() {
            if entry.state.load(Ordering::Acquire) == PARTITION_READY
                && entry.pool_key_hash.load(Ordering::Relaxed) == pool_key_hash
            {
                return Some(idx as u16);
            }
        }
        None
    }

    /// Read atomic cross-process counters for the registered partition matching `pool_key_hash`.
    pub fn partition_stats(&self, pool_key_hash: u64) -> Option<PartitionStats> {
        let pool_id = self.lookup_partition(pool_key_hash)?;
        let entry = &self.header().partitions[usize::from(pool_id)];
        let quarantined = entry.quarantined.load(Ordering::Acquire);
        let active_leases = entry
            .active_leases
            .load(Ordering::Acquire)
            .saturating_sub(quarantined);
        Some(PartitionStats {
            pool_id,
            pool_size: entry.pool_size.load(Ordering::Relaxed),
            max_overflow: entry.max_overflow.load(Ordering::Relaxed),
            active_leases,
            quarantined,
            total_acquired: u64::from(entry.total_acquired.load(Ordering::Relaxed)),
            total_released: u64::from(entry.total_released.load(Ordering::Relaxed)),
            total_reclaimed: u64::from(entry.total_reclaimed.load(Ordering::Relaxed)),
            total_quarantined: u64::from(entry.total_quarantined.load(Ordering::Relaxed)),
            total_disposed: u64::from(entry.total_disposed.load(Ordering::Relaxed)),
        })
    }

    fn try_acquire_slot(
        &self,
        partition: Option<u16>,
        worker_pid: u32,
        ttl_ns: u64,
        bind_key_hash: u64,
    ) -> Result<LeaseGrant, BrokerError> {
        let total = self.total_capacity();
        let hdr = self.header();

        let part_entry = if let Some(part_idx) = partition {
            let entry = hdr
                .partitions
                .get(part_idx as usize)
                .ok_or(BrokerError::InvalidPartition(part_idx))?;
            if entry.state.load(Ordering::Acquire) != PARTITION_READY {
                return Err(BrokerError::InvalidPartition(part_idx));
            }
            Some(entry)
        } else {
            None
        };

        let part_cap = part_entry
            .map(|p| p.pool_size.load(Ordering::Relaxed) + p.max_overflow.load(Ordering::Relaxed));
        if hdr.active_leases.0.load(Ordering::SeqCst) >= total
            || part_entry
                .zip(part_cap)
                .is_some_and(|(p, cap)| p.active_leases.load(Ordering::SeqCst) >= cap)
        {
            self.reap_expired(monotonic_now_ns());
        }

        let part_overflow = if let (Some(entry), Some(cap)) = (part_entry, part_cap) {
            let p_size = entry.pool_size.load(Ordering::Relaxed);
            let mut cur_part = entry.active_leases.load(Ordering::SeqCst);
            loop {
                if cur_part >= cap {
                    return Err(BrokerError::PoolExhausted);
                }
                match entry.active_leases.compare_exchange_weak(
                    cur_part,
                    cur_part + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break cur_part >= p_size,
                    Err(actual) => cur_part = actual,
                }
            }
        } else {
            false
        };

        let mut cur = hdr.active_leases.0.load(Ordering::SeqCst);
        loop {
            if cur >= total {
                if let Some(entry) = part_entry {
                    entry.active_leases.fetch_sub(1, Ordering::SeqCst);
                }
                return Err(BrokerError::PoolExhausted);
            }
            match hdr.active_leases.0.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }

        let pool_sz = self.pool_size();
        let start_idx = hdr.rr_cursor.0.fetch_add(1, Ordering::Relaxed) % total;

        let mut claim_retries = 0u32;
        loop {
            let mut saw_claiming = false;
            for offset in 0..total {
                let idx = (start_idx + offset) % total;
                let slot = self.slot(idx);
                let state_pid = slot.state_and_pid.load(Ordering::Acquire);
                let (state, _) = unpack_state_pid(state_pid);

                if state == SLOT_STATE_FREE {
                    let reclaiming_raw = pack_state_pid(SLOT_STATE_RECLAIMING, 0);
                    if slot
                        .state_and_pid
                        .compare_exchange(
                            state_pid,
                            reclaiming_raw,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        slot.partition_id
                            .store(partition.map_or(NO_PARTITION, u32::from), Ordering::Release);
                        let target_state = if partition.is_some() {
                            if part_overflow {
                                SLOT_STATE_OVERFLOW
                            } else {
                                SLOT_STATE_PRIMARY
                            }
                        } else if idx < pool_sz {
                            SLOT_STATE_PRIMARY
                        } else {
                            SLOT_STATE_OVERFLOW
                        };
                        let grant_gen = slot.generation.fetch_add(1, Ordering::AcqRel) + 1;
                        let now = monotonic_now_ns();
                        let expiry = if ttl_ns > 0 {
                            now.saturating_add(ttl_ns)
                        } else {
                            0
                        };

                        slot.acquired_ns.store(now, Ordering::Relaxed);
                        slot.expiry_ns.store(expiry, Ordering::Relaxed);
                        slot.ttl_ns.store(ttl_ns, Ordering::Relaxed);
                        slot.last_heartbeat_ns.store(now, Ordering::Relaxed);
                        slot.bind_key_hash.store(bind_key_hash, Ordering::Relaxed);
                        slot.state_and_pid
                            .compare_exchange(
                                reclaiming_raw,
                                pack_state_pid(target_state, worker_pid),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .expect("exclusive SLOT_STATE_RECLAIMING ownership during acquire");

                        hdr.total_acquired.fetch_add(1, Ordering::Relaxed);
                        if let Some(entry) = part_entry {
                            entry.total_acquired.fetch_add(1, Ordering::Relaxed);
                        }

                        return Ok(LeaseGrant {
                            slot_index: idx,
                            generation: grant_gen,
                            worker_pid,
                            is_overflow: target_state == SLOT_STATE_OVERFLOW,
                            acquired_ns: now,
                            expiry_ns: expiry,
                            bind_key_hash,
                        });
                    }
                    saw_claiming = true;
                } else if state == SLOT_STATE_CLAIMING || state == SLOT_STATE_RECLAIMING {
                    saw_claiming = true;
                }
            }

            if saw_claiming && claim_retries < 256 {
                claim_retries += 1;
                if claim_retries > 8 {
                    std::thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
                continue;
            }
            break;
        }

        hdr.active_leases.0.fetch_sub(1, Ordering::SeqCst);
        if let Some(entry) = part_entry {
            entry.active_leases.fetch_sub(1, Ordering::SeqCst);
        }
        Err(BrokerError::PoolExhausted)
    }

    pub fn try_acquire(
        &self,
        worker_pid: u32,
        ttl_ns: u64,
        bind_key_hash: u64,
    ) -> Result<LeaseGrant, BrokerError> {
        self.try_acquire_slot(None, worker_pid, ttl_ns, bind_key_hash)
    }

    pub fn try_acquire_in(
        &self,
        partition: u16,
        worker_pid: u32,
        ttl_ns: u64,
        bind_key_hash: u64,
    ) -> Result<Option<LeaseGrant>, BrokerError> {
        match self.try_acquire_slot(Some(partition), worker_pid, ttl_ns, bind_key_hash) {
            Ok(grant) => Ok(Some(grant)),
            Err(BrokerError::PoolExhausted) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn acquire_internal(
        &self,
        partition: Option<u16>,
        worker_pid: u32,
        ttl_ns: u64,
        timeout: Duration,
        bind_key_hash: u64,
    ) -> Result<LeaseGrant, BrokerError> {
        match self.try_acquire_slot(partition, worker_pid, ttl_ns, bind_key_hash) {
            Ok(grant) => return Ok(grant),
            Err(BrokerError::PoolExhausted) => {}
            Err(err) => return Err(err),
        }

        if timeout.is_zero() {
            self.header().total_timeouts.fetch_add(1, Ordering::Relaxed);
            return Err(BrokerError::PoolExhausted);
        }

        let hdr = self.header();
        let deadline = Instant::now() + timeout;

        hdr.waiters.0.fetch_add(1, Ordering::SeqCst);
        let _guard = WaitGuard(&hdr.waiters.0);

        loop {
            let seq = hdr.wake_seq.0.load_seqcst();

            match self.try_acquire_slot(partition, worker_pid, ttl_ns, bind_key_hash) {
                Ok(grant) => return Ok(grant),
                Err(BrokerError::PoolExhausted) => {}
                Err(err) => return Err(err),
            }

            let now = Instant::now();
            if now >= deadline {
                hdr.total_timeouts.fetch_add(1, Ordering::Relaxed);
                return Err(BrokerError::PoolExhausted);
            }

            let remaining = deadline - now;
            let sleep_dur = remaining.min(Duration::from_millis(25));
            let _ = hdr.wake_seq.0.wait_timeout(seq, sleep_dur);
        }
    }

    pub fn acquire(
        &self,
        worker_pid: u32,
        ttl_ns: u64,
        timeout: Duration,
        bind_key_hash: u64,
    ) -> Result<LeaseGrant, BrokerError> {
        self.acquire_internal(None, worker_pid, ttl_ns, timeout, bind_key_hash)
    }

    pub fn acquire_in(
        &self,
        partition: u16,
        worker_pid: u32,
        ttl_ns: u64,
        timeout: Duration,
        bind_key_hash: u64,
    ) -> Result<LeaseGrant, BrokerError> {
        self.acquire_internal(Some(partition), worker_pid, ttl_ns, timeout, bind_key_hash)
    }

    #[inline]
    pub fn wake_seq(&self) -> u32 {
        self.header().wake_seq.0.load_seqcst()
    }

    #[inline]
    pub fn partition_count(&self) -> u32 {
        self.header().partition_count.0.load(Ordering::Acquire)
    }

    /// Block until `wake_seq` differs from `expected_seq` or `timeout` elapses, returning the latest `wake_seq`.
    pub fn wait_for_wake(&self, expected_seq: u32, timeout: Duration) -> u32 {
        let hdr = self.header();
        if timeout.is_zero() {
            return hdr.wake_seq.0.load_seqcst();
        }
        let current = hdr.wake_seq.0.load_seqcst();
        if current != expected_seq {
            return current;
        }
        hdr.waiters.0.fetch_add(1, Ordering::SeqCst);
        let _guard = WaitGuard(&hdr.waiters.0);
        let after_reg = hdr.wake_seq.0.load_seqcst();
        if after_reg != expected_seq {
            return after_reg;
        }
        let _ = hdr.wake_seq.0.wait_timeout(expected_seq, timeout);
        hdr.wake_seq.0.load_seqcst()
    }

    /// Refresh `last_heartbeat_ns` for an active lease matching `(slot_index, generation)`.
    pub fn heartbeat(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError> {
        let total = self.total_capacity();
        if slot_index >= total {
            return Err(BrokerError::LeaseNotFound);
        }
        let slot = self.slot(slot_index);
        for _ in 0..1024 {
            let cur_gen = slot.generation.load(Ordering::Acquire);
            if cur_gen != generation {
                return Err(BrokerError::StaleGeneration);
            }
            let raw = slot.state_and_pid.load(Ordering::Acquire);
            let (state, pid) = unpack_state_pid(raw);
            if state == SLOT_STATE_CLAIMING || state == SLOT_STATE_RECLAIMING {
                std::hint::spin_loop();
                continue;
            }
            if state != SLOT_STATE_PRIMARY && state != SLOT_STATE_OVERFLOW {
                return Err(BrokerError::StaleGeneration);
            }
            let claim_state_pid = pack_state_pid(SLOT_STATE_CLAIMING, pid);
            if slot
                .state_and_pid
                .compare_exchange(raw, claim_state_pid, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            if slot.generation.load(Ordering::Acquire) != generation {
                let _ = slot.state_and_pid.compare_exchange(
                    claim_state_pid,
                    raw,
                    Ordering::Release,
                    Ordering::Relaxed,
                );
                return Err(BrokerError::StaleGeneration);
            }
            let now = monotonic_now_ns();
            slot.last_heartbeat_ns.store(now, Ordering::Release);
            let ttl = slot.ttl_ns.load(Ordering::Relaxed);
            if ttl > 0 {
                slot.expiry_ns
                    .store(now.saturating_add(ttl), Ordering::Relaxed);
            }
            if slot
                .state_and_pid
                .compare_exchange(claim_state_pid, raw, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(BrokerError::StaleGeneration);
            }
            return Ok(());
        }
        Err(BrokerError::StaleGeneration)
    }

    fn wake_waiters_seqcst(&self) {
        let hdr = self.header();
        hdr.wake_seq.0.fetch_add_seqcst(1);
        if hdr.waiters.0.load(Ordering::SeqCst) > 0 {
            let _ = hdr.wake_seq.0.wake_all();
        }
    }

    fn wake_one_waiter_seqcst(&self) {
        let hdr = self.header();
        hdr.wake_seq.0.fetch_add_seqcst(1);
        if hdr.waiters.0.load(Ordering::SeqCst) > 0 {
            if hdr.partition_count.0.load(Ordering::Acquire) <= 1 {
                let _ = hdr.wake_seq.0.wake_one();
            } else {
                let _ = hdr.wake_seq.0.wake_all();
            }
        }
    }

    fn release_claimed_slot(
        &self,
        slot: &LeaseSlot,
        raw: u64,
        expected_gen: Option<u64>,
        bump_generation: bool,
        counter: &AtomicU64,
        kind: ReleaseKind,
    ) -> Result<bool, BrokerError> {
        let (_, pid) = unpack_state_pid(raw);
        let reclaiming_raw = pack_state_pid(SLOT_STATE_RECLAIMING, pid);
        let active_claim_raw = match kind {
            ReleaseKind::Release => {
                let claim_raw = pack_state_pid(SLOT_STATE_CLAIMING, pid);
                if slot
                    .state_and_pid
                    .compare_exchange(raw, claim_raw, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return Ok(false);
                }
                if let Some(exp_gen) = expected_gen
                    && slot.generation.load(Ordering::Acquire) != exp_gen
                {
                    let _ = slot.state_and_pid.compare_exchange(
                        claim_raw,
                        raw,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                    return Err(BrokerError::GenerationMismatch);
                }
                if slot
                    .state_and_pid
                    .compare_exchange(
                        claim_raw,
                        reclaiming_raw,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    return Err(BrokerError::GenerationMismatch);
                }
                reclaiming_raw
            }
            ReleaseKind::Expire | ReleaseKind::Reclaim | ReleaseKind::DisposeQuarantined => {
                if slot
                    .state_and_pid
                    .compare_exchange(raw, reclaiming_raw, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return Ok(false);
                }
                if let Some(exp_gen) = expected_gen
                    && slot.generation.load(Ordering::Acquire) != exp_gen
                {
                    let _ = slot.state_and_pid.compare_exchange(
                        reclaiming_raw,
                        raw,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                    return Err(BrokerError::GenerationMismatch);
                }
                reclaiming_raw
            }
        };

        let part_id = slot.partition_id.load(Ordering::Acquire);
        if bump_generation {
            slot.generation.fetch_add(1, Ordering::AcqRel);
        }
        slot.partition_id.store(NO_PARTITION, Ordering::Relaxed);
        slot.state_and_pid
            .compare_exchange(active_claim_raw, 0, Ordering::AcqRel, Ordering::Acquire)
            .expect("exclusive SLOT_STATE_RECLAIMING ownership during release");

        let hdr = self.header();
        hdr.active_leases.0.fetch_sub(1, Ordering::SeqCst);
        if part_id < MAX_POOL_PARTITIONS as u32 {
            let entry = &hdr.partitions[part_id as usize];
            entry.active_leases.fetch_sub(1, Ordering::SeqCst);
            match kind {
                ReleaseKind::DisposeQuarantined => {
                    entry.quarantined.fetch_sub(1, Ordering::AcqRel);
                    entry.total_disposed.fetch_add(1, Ordering::Relaxed);
                }
                ReleaseKind::Release => {
                    entry.total_released.fetch_add(1, Ordering::Relaxed);
                }
                ReleaseKind::Reclaim => {
                    entry.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                }
                ReleaseKind::Expire => {}
            }
        }
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    pub fn release(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError> {
        let total = self.total_capacity();
        if slot_index >= total {
            return Err(BrokerError::LeaseNotFound);
        }

        let slot = self.slot(slot_index);
        let hdr = self.header();

        for _ in 0..1024 {
            let cur_gen = slot.generation.load(Ordering::Acquire);
            if cur_gen != generation {
                return Err(BrokerError::GenerationMismatch);
            }

            let raw = slot.state_and_pid.load(Ordering::Acquire);
            let (state, _) = unpack_state_pid(raw);
            if state == SLOT_STATE_CLAIMING || state == SLOT_STATE_RECLAIMING {
                std::hint::spin_loop();
                continue;
            }
            if state == SLOT_STATE_QUARANTINED {
                return Err(BrokerError::InvalidState);
            }
            if state != SLOT_STATE_PRIMARY && state != SLOT_STATE_OVERFLOW {
                return Err(BrokerError::LeaseNotFound);
            }

            if slot.generation.load(Ordering::Acquire) != generation {
                return Err(BrokerError::GenerationMismatch);
            }

            match self.release_claimed_slot(
                slot,
                raw,
                Some(generation),
                false,
                &hdr.total_released,
                ReleaseKind::Release,
            ) {
                Ok(true) => {
                    self.wake_one_waiter_seqcst();
                    return Ok(());
                }
                Ok(false) => {
                    let (cur_state, _) =
                        unpack_state_pid(slot.state_and_pid.load(Ordering::Acquire));
                    if cur_state == SLOT_STATE_CLAIMING {
                        std::hint::spin_loop();
                        continue;
                    }
                    return Err(BrokerError::GenerationMismatch);
                }
                Err(err) => return Err(err),
            }
        }

        Err(BrokerError::GenerationMismatch)
    }

    pub fn reap_expired(&self, now_ns: u64) -> u32 {
        let total = self.total_capacity();
        let hdr = self.header();
        let mut reaped = 0u32;

        for idx in 0..total {
            let slot = self.slot(idx);
            let cur_gen = slot.generation.load(Ordering::Acquire);
            let raw = slot.state_and_pid.load(Ordering::Acquire);
            let (state, _) = unpack_state_pid(raw);

            if state == SLOT_STATE_PRIMARY || state == SLOT_STATE_OVERFLOW {
                let ttl = slot.ttl_ns.load(Ordering::Relaxed);
                let last_hb = slot.last_heartbeat_ns.load(Ordering::Acquire);
                if slot.generation.load(Ordering::Acquire) == cur_gen
                    && ttl > 0
                    && now_ns.saturating_sub(last_hb) > ttl
                    && self
                        .release_claimed_slot(
                            slot,
                            raw,
                            Some(cur_gen),
                            true,
                            &hdr.total_expired,
                            ReleaseKind::Expire,
                        )
                        .unwrap_or(false)
                {
                    reaped += 1;
                }
            }
        }

        if reaped > 0 {
            self.wake_waiters_seqcst();
        }

        reaped
    }

    /// Transition a leased slot into [`LeaseState::Quarantined`] while keeping its capacity
    /// admission reserved until [`Self::dispose_quarantined`] completes.
    pub fn quarantine(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError> {
        let total = self.total_capacity();
        if slot_index >= total {
            return Err(BrokerError::LeaseNotFound);
        }
        let slot = self.slot(slot_index);
        for _ in 0..1024 {
            if slot.generation.load(Ordering::Acquire) != generation {
                return Err(BrokerError::GenerationMismatch);
            }
            let raw = slot.state_and_pid.load(Ordering::Acquire);
            let (state, pid) = unpack_state_pid(raw);
            if state == SLOT_STATE_CLAIMING || state == SLOT_STATE_RECLAIMING {
                std::hint::spin_loop();
                continue;
            }
            if state != SLOT_STATE_PRIMARY && state != SLOT_STATE_OVERFLOW {
                return Err(BrokerError::InvalidState);
            }
            if slot.generation.load(Ordering::Acquire) != generation {
                return Err(BrokerError::GenerationMismatch);
            }
            let claim_state_pid = pack_state_pid(SLOT_STATE_CLAIMING, pid);
            if slot
                .state_and_pid
                .compare_exchange(raw, claim_state_pid, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                if slot.generation.load(Ordering::Acquire) != generation {
                    return Err(BrokerError::GenerationMismatch);
                }
                let (cur_state, _) = unpack_state_pid(slot.state_and_pid.load(Ordering::Acquire));
                if cur_state == SLOT_STATE_CLAIMING || cur_state == SLOT_STATE_RECLAIMING {
                    std::hint::spin_loop();
                    continue;
                }
                return Err(BrokerError::InvalidState);
            }
            if slot.generation.load(Ordering::Acquire) != generation {
                let _ = slot.state_and_pid.compare_exchange(
                    claim_state_pid,
                    raw,
                    Ordering::Release,
                    Ordering::Relaxed,
                );
                return Err(BrokerError::GenerationMismatch);
            }
            let reclaiming_raw = pack_state_pid(SLOT_STATE_RECLAIMING, pid);
            if slot
                .state_and_pid
                .compare_exchange(
                    claim_state_pid,
                    reclaiming_raw,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return Err(BrokerError::GenerationMismatch);
            }
            let hdr = self.header();
            hdr.total_quarantined.fetch_add(1, Ordering::Relaxed);
            let part_id = slot.partition_id.load(Ordering::Acquire);
            if part_id < MAX_POOL_PARTITIONS as u32 {
                let entry = &hdr.partitions[part_id as usize];
                entry.quarantined.fetch_add(1, Ordering::AcqRel);
                entry.total_quarantined.fetch_add(1, Ordering::Relaxed);
            }
            slot.state_and_pid
                .compare_exchange(
                    reclaiming_raw,
                    pack_state_pid(SLOT_STATE_QUARANTINED, pid),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .expect("exclusive SLOT_STATE_RECLAIMING ownership during quarantine");
            return Ok(());
        }
        if slot.generation.load(Ordering::Acquire) != generation {
            Err(BrokerError::GenerationMismatch)
        } else {
            Err(BrokerError::InvalidState)
        }
    }

    /// Dispose a quarantined slot, returning it to [`LeaseState::Free`] and waking waiters.
    pub fn dispose_quarantined(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError> {
        let total = self.total_capacity();
        if slot_index >= total {
            return Err(BrokerError::LeaseNotFound);
        }
        let slot = self.slot(slot_index);
        let hdr = self.header();
        for _ in 0..1024 {
            if slot.generation.load(Ordering::Acquire) != generation {
                return Err(BrokerError::GenerationMismatch);
            }
            let raw = slot.state_and_pid.load(Ordering::Acquire);
            let (state, _) = unpack_state_pid(raw);
            if state == SLOT_STATE_CLAIMING || state == SLOT_STATE_RECLAIMING {
                std::hint::spin_loop();
                continue;
            }
            if state != SLOT_STATE_QUARANTINED {
                return Err(BrokerError::InvalidState);
            }
            if slot.generation.load(Ordering::Acquire) != generation {
                return Err(BrokerError::GenerationMismatch);
            }
            match self.release_claimed_slot(
                slot,
                raw,
                Some(generation),
                false,
                &hdr.total_disposed,
                ReleaseKind::DisposeQuarantined,
            ) {
                Ok(true) => {
                    self.wake_one_waiter_seqcst();
                    return Ok(());
                }
                Ok(false) => {
                    if slot.generation.load(Ordering::Acquire) != generation {
                        return Err(BrokerError::GenerationMismatch);
                    }
                    let (cur_state, _) =
                        unpack_state_pid(slot.state_and_pid.load(Ordering::Acquire));
                    if cur_state == SLOT_STATE_CLAIMING {
                        std::hint::spin_loop();
                        continue;
                    }
                    return Err(BrokerError::InvalidState);
                }
                Err(err) => return Err(err),
            }
        }
        if slot.generation.load(Ordering::Acquire) != generation {
            Err(BrokerError::GenerationMismatch)
        } else {
            Err(BrokerError::InvalidState)
        }
    }

    /// Count slots currently in [`LeaseState::Quarantined`].
    #[must_use]
    pub fn quarantined_count(&self) -> u32 {
        let total = self.total_capacity();
        let mut count = 0u32;
        for idx in 0..total {
            let (state, _) = unpack_state_pid(self.slot(idx).state_and_pid.load(Ordering::Acquire));
            if state == SLOT_STATE_QUARANTINED {
                count += 1;
            }
        }
        count
    }

    /// Transition every live slot held by `worker_pid` into [`LeaseState::Quarantined`] with bumped
    /// generation counters so [`BrokerHost`] can execute server-side `ROLLBACK` before disposal.
    pub fn reclaim_worker_leases(&self, worker_pid: u32) -> Vec<ReclaimedLease> {
        let total = self.total_capacity();
        let hdr = self.header();
        let mut reclaimed = Vec::new();
        let reaper_raw = pack_state_pid(SLOT_STATE_RECLAIMING, worker_pid);

        for idx in 0..total {
            let slot = self.slot(idx);
            let mut claim_deadline: Option<Instant> = None;
            loop {
                let raw = slot.state_and_pid.load(Ordering::Acquire);
                let (state, pid) = unpack_state_pid(raw);
                if pid != worker_pid {
                    break;
                }
                if state == SLOT_STATE_CLAIMING {
                    let deadline = *claim_deadline
                        .get_or_insert_with(|| Instant::now() + Duration::from_millis(20));
                    if Instant::now() < deadline {
                        std::thread::yield_now();
                        continue;
                    }
                    if slot
                        .state_and_pid
                        .compare_exchange(raw, reaper_raw, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue;
                    }
                    let next_gen = slot.generation.fetch_add(1, Ordering::AcqRel) + 1;
                    let part = slot.partition_id.load(Ordering::Acquire);
                    let pool_id = if part < MAX_POOL_PARTITIONS as u32 {
                        let entry = &hdr.partitions[part as usize];
                        entry.quarantined.fetch_add(1, Ordering::AcqRel);
                        entry.total_quarantined.fetch_add(1, Ordering::Relaxed);
                        entry.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                        Some(part as u16)
                    } else {
                        None
                    };
                    hdr.total_quarantined.fetch_add(1, Ordering::Relaxed);
                    hdr.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                    slot.state_and_pid
                        .compare_exchange(
                            reaper_raw,
                            pack_state_pid(SLOT_STATE_QUARANTINED, worker_pid),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .expect(
                            "exclusive SLOT_STATE_RECLAIMING ownership in reclaim_worker_leases",
                        );
                    reclaimed.push(ReclaimedLease {
                        slot_index: idx,
                        generation: next_gen,
                        pool_id,
                    });
                    break;
                }
                if state != SLOT_STATE_PRIMARY && state != SLOT_STATE_OVERFLOW {
                    break;
                }
                if slot
                    .state_and_pid
                    .compare_exchange(raw, reaper_raw, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    let next_gen = slot.generation.fetch_add(1, Ordering::AcqRel) + 1;
                    let part = slot.partition_id.load(Ordering::Acquire);
                    let pool_id = if part < MAX_POOL_PARTITIONS as u32 {
                        let entry = &hdr.partitions[part as usize];
                        entry.quarantined.fetch_add(1, Ordering::AcqRel);
                        entry.total_quarantined.fetch_add(1, Ordering::Relaxed);
                        entry.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                        Some(part as u16)
                    } else {
                        None
                    };
                    hdr.total_quarantined.fetch_add(1, Ordering::Relaxed);
                    hdr.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                    slot.state_and_pid
                        .compare_exchange(
                            reaper_raw,
                            pack_state_pid(SLOT_STATE_QUARANTINED, worker_pid),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .expect(
                            "exclusive SLOT_STATE_RECLAIMING ownership in reclaim_worker_leases",
                        );
                    reclaimed.push(ReclaimedLease {
                        slot_index: idx,
                        generation: next_gen,
                        pool_id,
                    });
                    break;
                }
            }
        }

        reclaimed
    }

    pub fn reclaim_worker(&self, worker_pid: u32) -> u32 {
        let total = self.total_capacity();
        let hdr = self.header();
        let mut reclaimed = 0u32;
        let reaper_raw = pack_state_pid(SLOT_STATE_RECLAIMING, worker_pid);

        for idx in 0..total {
            let slot = self.slot(idx);
            let mut claim_deadline: Option<Instant> = None;
            loop {
                let raw = slot.state_and_pid.load(Ordering::Acquire);
                let (state, pid) = unpack_state_pid(raw);
                if pid != worker_pid {
                    break;
                }
                if state == SLOT_STATE_CLAIMING {
                    let deadline = *claim_deadline
                        .get_or_insert_with(|| Instant::now() + Duration::from_millis(20));
                    if Instant::now() < deadline {
                        std::thread::yield_now();
                        continue;
                    }
                    if slot
                        .state_and_pid
                        .compare_exchange(raw, reaper_raw, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue;
                    }
                    let part_id = slot.partition_id.load(Ordering::Acquire);
                    slot.generation.fetch_add(1, Ordering::AcqRel);
                    slot.partition_id.store(NO_PARTITION, Ordering::Relaxed);
                    slot.state_and_pid
                        .compare_exchange(reaper_raw, 0, Ordering::AcqRel, Ordering::Acquire)
                        .expect("exclusive SLOT_STATE_RECLAIMING ownership in reclaim_worker");
                    hdr.active_leases.0.fetch_sub(1, Ordering::SeqCst);
                    if part_id < MAX_POOL_PARTITIONS as u32 {
                        let entry = &hdr.partitions[part_id as usize];
                        entry.active_leases.fetch_sub(1, Ordering::SeqCst);
                        entry.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                    }
                    hdr.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                    reclaimed += 1;
                    break;
                }
                if state != SLOT_STATE_PRIMARY && state != SLOT_STATE_OVERFLOW {
                    break;
                }
                if self
                    .release_claimed_slot(
                        slot,
                        raw,
                        None,
                        true,
                        &hdr.total_reclaimed,
                        ReleaseKind::Reclaim,
                    )
                    .unwrap_or(false)
                {
                    reclaimed += 1;
                    break;
                }
            }
        }

        if reclaimed > 0 {
            self.wake_waiters_seqcst();
        }

        reclaimed
    }

    pub fn active_leases_for_worker(&self, worker_pid: u32) -> Vec<(u16, u32, u64)> {
        let total = self.total_capacity();
        let mut active = Vec::new();

        for idx in 0..total {
            let slot = self.slot(idx);
            let raw = slot.state_and_pid.load(Ordering::Acquire);
            let (state, pid) = unpack_state_pid(raw);
            if (state == SLOT_STATE_PRIMARY || state == SLOT_STATE_OVERFLOW) && pid == worker_pid {
                let generation = slot.generation.load(Ordering::Acquire);
                let bind_hash = slot.bind_key_hash.load(Ordering::Relaxed);
                let pool_id = if bind_hash <= u16::MAX as u64 {
                    bind_hash as u16
                } else {
                    0u16
                };
                active.push((pool_id, idx, generation));
            }
        }

        active
    }

    #[inline]
    fn channel_header_and_slots(&self) -> Option<(&ShmChannelHeader, *const ShmChannelSlot, u32)> {
        let hdr = self.header();
        let cap = hdr.channel_capacity;
        let offset = usize::try_from(hdr.channel_offset).ok()?;
        if cap == 0 || offset == 0 {
            return None;
        }
        let slots_bytes = (cap as usize).checked_mul(std::mem::size_of::<ShmChannelSlot>())?;
        let section_bytes = std::mem::size_of::<ShmChannelHeader>()
            .checked_add(slots_bytes)
            .and_then(|raw| raw.checked_add(63))
            .map(|aligned| aligned & !63)?;
        let base = self.region.as_ptr() as *const u8;
        let region_len = self.region.len();
        let ch_hdr_ptr =
            checked_aligned_section::<ShmChannelHeader>(base, offset, section_bytes, region_len)
                .ok()?;
        let slots_offset = offset.checked_add(std::mem::size_of::<ShmChannelHeader>())?;
        let slots_ptr =
            checked_aligned_section::<ShmChannelSlot>(base, slots_offset, slots_bytes, region_len)
                .ok()?;
        // SAFETY: `ch_hdr_ptr` and `slots_ptr` were validated for bounds and alignment by `checked_aligned_section`.
        unsafe { Some((&*ch_hdr_ptr, slots_ptr as *const ShmChannelSlot, cap)) }
    }

    pub fn publish_channel(
        &self,
        channel: &[u8],
        payload: &[u8],
        timestamp_ns: u64,
        lease_id: Option<&[u8]>,
    ) -> bool {
        if channel.is_empty()
            || channel.len() > CHAN_MAX_NAME_BYTES
            || payload.len() > CHAN_MAX_PAYLOAD_BYTES
            || lease_id.is_some_and(|l| l.len() > CHAN_MAX_LEASE_ID_BYTES)
        {
            return false;
        }

        let Some((ch_hdr, slots_ptr, cap)) = self.channel_header_and_slots() else {
            return false;
        };

        let seq = ch_hdr.write_seq.fetch_add(1, Ordering::AcqRel) + 1;
        let cap_usize = cap as usize;
        let start = ((seq as usize).wrapping_sub(1)) % cap_usize;

        let mut claimed_idx: Option<usize> = None;

        // Pass 1: find a free slot
        for offset in 0..cap_usize {
            let idx = (start + offset) % cap_usize;
            // SAFETY: `idx < cap_usize` and `slots_ptr` spans `cap_usize` `ShmChannelSlot` entries.
            let slot = unsafe { &*slots_ptr.add(idx) };
            if slot.state.load(Ordering::Acquire) == CHAN_SLOT_FREE
                && slot
                    .state
                    .compare_exchange(
                        CHAN_SLOT_FREE,
                        CHAN_SLOT_WRITING,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                claimed_idx = Some(idx);
                break;
            }
        }

        // Pass 2: if full, evict the oldest ready slot
        if claimed_idx.is_none() {
            let mut oldest_idx = start;
            let mut oldest_seq = u64::MAX;
            for idx in 0..cap_usize {
                // SAFETY: `idx < cap_usize` and `slots_ptr` spans `cap_usize` `ShmChannelSlot` entries.
                let slot = unsafe { &*slots_ptr.add(idx) };
                if slot.state.load(Ordering::Acquire) == CHAN_SLOT_READY {
                    let s = slot.seq.load(Ordering::Relaxed);
                    if s < oldest_seq {
                        oldest_seq = s;
                        oldest_idx = idx;
                    }
                }
            }
            // SAFETY: `oldest_idx < cap_usize` and `slots_ptr` spans `cap_usize` `ShmChannelSlot` entries.
            let slot = unsafe { &*slots_ptr.add(oldest_idx) };
            if slot
                .state
                .compare_exchange(
                    CHAN_SLOT_READY,
                    CHAN_SLOT_WRITING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                ch_hdr.total_dropped.fetch_add(1, Ordering::Relaxed);
                claimed_idx = Some(oldest_idx);
            }
        }

        let Some(idx) = claimed_idx else {
            return false;
        };

        // SAFETY: `idx < cap_usize` and `slots_ptr` spans `cap_usize` `ShmChannelSlot` entries.
        let slot = unsafe { &*slots_ptr.add(idx) };
        let ts = if timestamp_ns > 0 {
            timestamp_ns
        } else {
            monotonic_now_ns()
        };

        let mut ch_buf = [0u8; CHAN_MAX_NAME_BYTES];
        ch_buf[..channel.len()].copy_from_slice(channel);
        for (i, chunk) in ch_buf
            .as_chunks::<8>()
            .0
            .iter()
            .take(channel.len().div_ceil(8))
            .enumerate()
        {
            slot.channel_words[i].store(u64::from_ne_bytes(*chunk), Ordering::Relaxed);
        }

        let lid_len = if let Some(lid) = lease_id {
            let mut lid_buf = [0u8; CHAN_MAX_LEASE_ID_BYTES];
            lid_buf[..lid.len()].copy_from_slice(lid);
            for (i, chunk) in lid_buf
                .as_chunks::<8>()
                .0
                .iter()
                .take(lid.len().div_ceil(8))
                .enumerate()
            {
                slot.lease_id_words[i].store(u64::from_ne_bytes(*chunk), Ordering::Relaxed);
            }
            lid.len() as u16
        } else {
            0
        };

        if !payload.is_empty() {
            let mut pay_buf = [0u8; CHAN_MAX_PAYLOAD_BYTES];
            pay_buf[..payload.len()].copy_from_slice(payload);
            for (i, chunk) in pay_buf
                .as_chunks::<8>()
                .0
                .iter()
                .take(payload.len().div_ceil(8))
                .enumerate()
            {
                slot.payload_words[i].store(u64::from_ne_bytes(*chunk), Ordering::Relaxed);
            }
        }

        slot.channel_len
            .store(channel.len() as u16, Ordering::Relaxed);
        slot.payload_len
            .store(payload.len() as u16, Ordering::Relaxed);
        slot.lease_id_len.store(lid_len, Ordering::Relaxed);
        slot.timestamp_ns.store(ts, Ordering::Relaxed);
        slot.seq.store(seq, Ordering::Relaxed);

        slot.state.store(CHAN_SLOT_READY, Ordering::Release);
        ch_hdr.total_published.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn drain_channel(&self, channel: &[u8]) -> Vec<ChannelFrame> {
        let Some((ch_hdr, slots_ptr, cap)) = self.channel_header_and_slots() else {
            return Vec::new();
        };

        let mut collected: Vec<(u64, ChannelFrame)> = Vec::new();

        for idx in 0..cap as usize {
            // SAFETY: `idx < cap as usize` and `slots_ptr` spans `cap` `ShmChannelSlot` entries.
            let slot = unsafe { &*slots_ptr.add(idx) };
            if slot.state.load(Ordering::Acquire) != CHAN_SLOT_READY {
                continue;
            }
            let ch_len = slot.channel_len.load(Ordering::Relaxed) as usize;
            if ch_len != channel.len() || ch_len > CHAN_MAX_NAME_BYTES {
                continue;
            }

            if slot
                .state
                .compare_exchange(
                    CHAN_SLOT_READY,
                    CHAN_SLOT_WRITING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }

            let ch_len2 = slot.channel_len.load(Ordering::Relaxed) as usize;
            if ch_len2 != channel.len() || ch_len2 > CHAN_MAX_NAME_BYTES {
                slot.state.store(CHAN_SLOT_READY, Ordering::Release);
                continue;
            }

            let mut ch_words = [0u64; 8];
            for (i, word) in ch_words.iter_mut().enumerate().take(ch_len2.div_ceil(8)) {
                *word = slot.channel_words[i].load(Ordering::Relaxed);
            }
            // SAFETY: `ch_len2 <= CHAN_MAX_NAME_BYTES` (64) == `size_of_val(&ch_words)`.
            let ch_slice =
                unsafe { std::slice::from_raw_parts(ch_words.as_ptr() as *const u8, ch_len2) };
            if ch_slice != channel {
                slot.state.store(CHAN_SLOT_READY, Ordering::Release);
                continue;
            }

            let pay_len =
                (slot.payload_len.load(Ordering::Relaxed) as usize).min(CHAN_MAX_PAYLOAD_BYTES);
            let mut pay_words = [0u64; 32];
            for (i, word) in pay_words.iter_mut().enumerate().take(pay_len.div_ceil(8)) {
                *word = slot.payload_words[i].load(Ordering::Relaxed);
            }
            // SAFETY: `pay_len <= CHAN_MAX_PAYLOAD_BYTES` (256) == `size_of_val(&pay_words)`.
            let payload_vec = unsafe {
                std::slice::from_raw_parts(pay_words.as_ptr() as *const u8, pay_len).to_vec()
            };

            let lid_len =
                (slot.lease_id_len.load(Ordering::Relaxed) as usize).min(CHAN_MAX_LEASE_ID_BYTES);
            let lease_id = if lid_len > 0 {
                let mut lid_words = [0u64; 4];
                for (i, word) in lid_words.iter_mut().enumerate().take(lid_len.div_ceil(8)) {
                    *word = slot.lease_id_words[i].load(Ordering::Relaxed);
                }
                // SAFETY: `lid_len <= CHAN_MAX_LEASE_ID_BYTES` (32) == `size_of_val(&lid_words)`.
                Some(unsafe {
                    std::slice::from_raw_parts(lid_words.as_ptr() as *const u8, lid_len).to_vec()
                })
            } else {
                None
            };

            let ts = slot.timestamp_ns.load(Ordering::Relaxed);
            let seq = slot.seq.load(Ordering::Relaxed);

            slot.state.store(CHAN_SLOT_FREE, Ordering::Release);
            ch_hdr.total_drained.fetch_add(1, Ordering::Relaxed);

            collected.push((
                seq,
                ChannelFrame {
                    channel: channel.to_vec(),
                    payload: payload_vec,
                    timestamp_ns: ts,
                    lease_id,
                },
            ));
        }

        collected.sort_unstable_by_key(|(seq, _)| *seq);
        collected.into_iter().map(|(_, f)| f).collect()
    }

    pub fn drain_all_channels(&self) -> Vec<ChannelFrame> {
        let Some((ch_hdr, slots_ptr, cap)) = self.channel_header_and_slots() else {
            return Vec::new();
        };

        let mut collected: Vec<(u64, ChannelFrame)> = Vec::new();

        for idx in 0..cap as usize {
            // SAFETY: `idx < cap as usize` and `slots_ptr` spans `cap` `ShmChannelSlot` entries.
            let slot = unsafe { &*slots_ptr.add(idx) };
            if slot.state.load(Ordering::Acquire) != CHAN_SLOT_READY {
                continue;
            }
            if slot
                .state
                .compare_exchange(
                    CHAN_SLOT_READY,
                    CHAN_SLOT_WRITING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }

            let ch_len =
                (slot.channel_len.load(Ordering::Relaxed) as usize).min(CHAN_MAX_NAME_BYTES);
            let mut ch_words = [0u64; 8];
            for (i, word) in ch_words.iter_mut().enumerate().take(ch_len.div_ceil(8)) {
                *word = slot.channel_words[i].load(Ordering::Relaxed);
            }
            // SAFETY: `ch_len <= CHAN_MAX_NAME_BYTES` (64) == `size_of_val(&ch_words)`.
            let channel_vec = unsafe {
                std::slice::from_raw_parts(ch_words.as_ptr() as *const u8, ch_len).to_vec()
            };

            let pay_len =
                (slot.payload_len.load(Ordering::Relaxed) as usize).min(CHAN_MAX_PAYLOAD_BYTES);
            let mut pay_words = [0u64; 32];
            for (i, word) in pay_words.iter_mut().enumerate().take(pay_len.div_ceil(8)) {
                *word = slot.payload_words[i].load(Ordering::Relaxed);
            }
            // SAFETY: `pay_len <= CHAN_MAX_PAYLOAD_BYTES` (256) == `size_of_val(&pay_words)`.
            let payload_vec = unsafe {
                std::slice::from_raw_parts(pay_words.as_ptr() as *const u8, pay_len).to_vec()
            };

            let lid_len =
                (slot.lease_id_len.load(Ordering::Relaxed) as usize).min(CHAN_MAX_LEASE_ID_BYTES);
            let lease_id = if lid_len > 0 {
                let mut lid_words = [0u64; 4];
                for (i, word) in lid_words.iter_mut().enumerate().take(lid_len.div_ceil(8)) {
                    *word = slot.lease_id_words[i].load(Ordering::Relaxed);
                }
                // SAFETY: `lid_len <= CHAN_MAX_LEASE_ID_BYTES` (32) == `size_of_val(&lid_words)`.
                Some(unsafe {
                    std::slice::from_raw_parts(lid_words.as_ptr() as *const u8, lid_len).to_vec()
                })
            } else {
                None
            };

            let ts = slot.timestamp_ns.load(Ordering::Relaxed);
            let seq = slot.seq.load(Ordering::Relaxed);

            slot.state.store(CHAN_SLOT_FREE, Ordering::Release);
            ch_hdr.total_drained.fetch_add(1, Ordering::Relaxed);

            collected.push((
                seq,
                ChannelFrame {
                    channel: channel_vec,
                    payload: payload_vec,
                    timestamp_ns: ts,
                    lease_id,
                },
            ));
        }

        collected.sort_unstable_by_key(|(seq, _)| *seq);
        collected.into_iter().map(|(_, f)| f).collect()
    }

    pub fn stats(&self) -> LeaseTableStats {
        let hdr = self.header();
        let pool_sz = self.pool_size();
        let total = self.total_capacity();
        let mut primary_in_use = 0u32;
        let mut overflow_in_use = 0u32;
        let mut quarantined = 0u32;

        for idx in 0..total {
            let slot = self.slot(idx);
            let (state, _) = unpack_state_pid(slot.state_and_pid.load(Ordering::Relaxed));
            match state {
                SLOT_STATE_PRIMARY => primary_in_use += 1,
                SLOT_STATE_OVERFLOW => overflow_in_use += 1,
                SLOT_STATE_QUARANTINED => quarantined += 1,
                _ => {}
            }
        }

        LeaseTableStats {
            pool_size: pool_sz,
            max_overflow: self.max_overflow(),
            active_leases: hdr
                .active_leases
                .0
                .load(Ordering::Relaxed)
                .saturating_sub(quarantined),
            primary_in_use,
            overflow_in_use,
            waiting_threads: hdr.waiters.0.load(Ordering::Relaxed),
            total_acquired: hdr.total_acquired.load(Ordering::Relaxed),
            total_released: hdr.total_released.load(Ordering::Relaxed),
            total_expired: hdr.total_expired.load(Ordering::Relaxed),
            total_timeouts: hdr.total_timeouts.load(Ordering::Relaxed),
            total_reclaimed: hdr.total_reclaimed.load(Ordering::Relaxed),
            quarantined,
            total_quarantined: hdr.total_quarantined.load(Ordering::Relaxed),
            total_disposed: hdr.total_disposed.load(Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Shared-Memory Prepared Statement Cache (Task 2.1)
// ============================================================================

#[repr(C, align(64))]
pub struct StatementSlot {
    pub seq: AtomicU64,
    pub sql_hash: AtomicU64,
    pub dialect_and_clock: AtomicU32,
    pub param_count: AtomicU16,
    pub sql_len: AtomicU16,
    pub plan_len: AtomicU16,
    pub _reserved: [u8; 6],
    pub payload: [AtomicU64; 28],
}

const _: () = assert!(std::mem::size_of::<StatementSlot>() == 256);
const _: () = assert!(std::mem::align_of::<StatementSlot>() == 64);

#[repr(C, align(64))]
pub struct ShmStatementHeader {
    pub capacity: u32,
    pub set_mask: u32,
    pub _pad0: [u8; 24],
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub inserts: AtomicU64,
    pub evictions: AtomicU64,
}

const _: () = assert!(std::mem::size_of::<ShmStatementHeader>() == 64);
const _: () = assert!(std::mem::align_of::<ShmStatementHeader>() == 64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementPlanEntry {
    pub sql_hash: u64,
    pub dialect_tag: u8,
    pub param_count: u16,
    pub compiled_sql: Vec<u8>,
    pub param_plan: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatementCacheStats {
    pub capacity: u32,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
}

pub struct ShmStatementCache {
    header: *mut ShmStatementHeader,
    slots: *mut StatementSlot,
    _region: Option<Arc<ShmRegion>>,
}

// SAFETY: All StatementSlot and ShmStatementHeader memory is accessed via atomic primitives.
unsafe impl Send for ShmStatementCache {}
// SAFETY: Shared references only perform atomic operations or read immutable header metadata.
unsafe impl Sync for ShmStatementCache {}

impl ShmStatementCache {
    pub fn compute_sql_hash(dialect_tag: u8, raw_sql: &[u8]) -> u64 {
        let mut stack_buf = [0u8; 512];
        if raw_sql.len() < stack_buf.len() {
            stack_buf[0] = dialect_tag;
            stack_buf[1..=raw_sql.len()].copy_from_slice(raw_sql);
            return stable_hash64(&stack_buf[..=raw_sql.len()]);
        }
        let mut heap_buf = Vec::with_capacity(raw_sql.len() + 1);
        heap_buf.push(dialect_tag);
        heap_buf.extend_from_slice(raw_sql);
        stable_hash64(&heap_buf)
    }

    pub const fn required_size(capacity: u32) -> usize {
        let base = if capacity < 4 { 4 } else { capacity };
        let cap = if base > MAX_STATEMENT_CAPACITY {
            MAX_STATEMENT_CAPACITY
        } else {
            base.next_power_of_two()
        };
        let raw = std::mem::size_of::<ShmStatementHeader>()
            .saturating_add((cap as usize).saturating_mul(std::mem::size_of::<StatementSlot>()));
        raw.saturating_add(63) & !63
    }

    pub fn from_region_at_offset(
        region: Arc<ShmRegion>,
        offset: usize,
        max_entries: u32,
        create: bool,
    ) -> Result<Self, BrokerError> {
        let base = max_entries.max(4);
        if base > MAX_STATEMENT_CAPACITY {
            return Err(BrokerError::InvalidCapacity);
        }
        let cap = base
            .checked_next_power_of_two()
            .ok_or(BrokerError::InvalidCapacity)?;
        let req_size = Self::required_size(cap);
        let region_len = region.len();
        let region_base = region.as_ptr() as *const u8;
        let header_ptr = checked_aligned_section::<ShmStatementHeader>(
            region_base,
            offset,
            req_size,
            region_len,
        )?;
        let slots_offset = offset
            .checked_add(std::mem::size_of::<ShmStatementHeader>())
            .ok_or(BrokerError::RegionTooSmall {
                expected: usize::MAX,
                actual: region_len,
            })?;
        let slots_bytes = (cap as usize)
            .checked_mul(std::mem::size_of::<StatementSlot>())
            .ok_or(BrokerError::InvalidCapacity)?;
        let slots_ptr = checked_aligned_section::<StatementSlot>(
            region_base,
            slots_offset,
            slots_bytes,
            region_len,
        )?;

        if create {
            // SAFETY: `header_ptr` and `req_size` bytes at `offset` were validated by `checked_aligned_section`.
            unsafe {
                std::ptr::write_bytes(header_ptr as *mut u8, 0, req_size);
                let hdr = &mut *header_ptr;
                hdr.capacity = cap;
                hdr.set_mask = (cap / 4) - 1;
                hdr.hits.store(0, Ordering::Relaxed);
                hdr.misses.store(0, Ordering::Relaxed);
                hdr.inserts.store(0, Ordering::Relaxed);
                hdr.evictions.store(0, Ordering::Relaxed);
            }
        }

        Ok(Self {
            header: header_ptr,
            slots: slots_ptr,
            _region: Some(region),
        })
    }

    pub fn init_in_region(region: ShmRegion, capacity: u32) -> Result<Self, BrokerError> {
        Self::from_region_at_offset(Arc::new(region), 0, capacity, true)
    }

    pub fn attach_region(region: ShmRegion, capacity: u32) -> Result<Self, BrokerError> {
        Self::from_region_at_offset(Arc::new(region), 0, capacity, false)
    }

    #[cfg(unix)]
    pub fn open_or_create_named(name: &str, capacity: u32) -> Result<(Self, bool), BrokerError> {
        let base = capacity.max(4);
        if base > MAX_STATEMENT_CAPACITY {
            return Err(BrokerError::InvalidCapacity);
        }
        let cap = base
            .checked_next_power_of_two()
            .ok_or(BrokerError::InvalidCapacity)?;
        let req_size = Self::required_size(cap);
        let attach = ShmRegion::open_or_create_named(name, req_size, |region| {
            let len = region.len();
            if len < req_size {
                return Err(ShmError::InvalidSize);
            }
            // SAFETY: region slice has exclusive access during initialization.
            unsafe {
                let slice = region.as_mut_slice();
                slice[..req_size].fill(0);
                let shdr = &mut *(slice.as_mut_ptr() as *mut ShmStatementHeader);
                shdr.capacity = cap;
                shdr.set_mask = (cap / 4) - 1;
            }
            Ok(())
        })
        .map_err(|e| map_shm_error(e, name))?;
        let (region, created) = match attach {
            NamedAttach::Created(r) => (r, true),
            NamedAttach::Attached(r) => (r, false),
        };
        let cache = Self::attach_region(region, cap)?;
        if !created && cache.capacity() != cap {
            return Err(BrokerError::SegmentInUse {
                owner_pid: cache.owner_pid().unwrap_or(0),
            });
        }
        Ok((cache, created))
    }

    #[inline]
    pub fn owner_pid(&self) -> Option<u32> {
        self._region.as_ref().and_then(|r| r.owner_pid())
    }

    #[cfg(unix)]
    pub fn unlink(&self) -> bool {
        self._region.as_ref().is_some_and(|r| r.unlink_now())
    }

    #[cfg(not(unix))]
    pub fn unlink(&self) -> bool {
        false
    }

    /// Attach statement cache from raw pointers (for attached sub-caches within ShmLeaseTable).
    ///
    /// # SAFETY
    /// The caller must ensure `header_ptr` and `slots_ptr` point to valid memory allocated
    /// for at least `Self::required_size(capacity)` bytes.
    pub unsafe fn from_raw_ptrs(
        header_ptr: *mut ShmStatementHeader,
        slots_ptr: *mut StatementSlot,
    ) -> Self {
        Self {
            header: header_ptr,
            slots: slots_ptr,
            _region: None,
        }
    }

    #[inline]
    fn header(&self) -> &ShmStatementHeader {
        // SAFETY: `self.header` points to a validated `ShmStatementHeader` kept alive by `self._region` or caller.
        unsafe { &*self.header }
    }

    #[inline]
    fn slot(&self, index: usize) -> &StatementSlot {
        // SAFETY: `index` is bounded by `set_mask * 4 + 4 <= capacity` and `self.slots` spans `capacity` entries.
        unsafe { &*self.slots.add(index) }
    }

    #[inline]
    pub fn capacity(&self) -> u32 {
        self.header().capacity
    }

    pub fn get(&self, dialect_tag: u8, raw_sql: &[u8]) -> Option<StatementPlanEntry> {
        self.lookup(dialect_tag, raw_sql)
    }

    pub fn put(
        &self,
        dialect_tag: u8,
        raw_sql: &[u8],
        param_count: u16,
        compiled_sql: &[u8],
        param_plan: &[u8],
    ) -> bool {
        self.insert(dialect_tag, raw_sql, param_count, compiled_sql, param_plan)
    }

    pub fn invalidate_all(&self) {
        let cap = self.header().capacity as usize;
        for idx in 0..cap {
            let slot = self.slot(idx);
            slot.sql_hash.store(0, Ordering::Relaxed);
            slot.seq.store(0, Ordering::Release);
        }
    }

    pub fn lookup(&self, dialect_tag: u8, raw_sql: &[u8]) -> Option<StatementPlanEntry> {
        let hash = Self::compute_sql_hash(dialect_tag, raw_sql);
        let hdr = self.header();
        let set_idx = (hash as usize) & (hdr.set_mask as usize);
        let base = set_idx * 4;

        for way in 0..4 {
            let slot = self.slot(base + way);
            let seq1 = slot.seq.load(Ordering::Acquire);

            // Odd sequence means write in progress, 0 means empty
            if seq1 == 0 || (seq1 & 1) != 0 {
                continue;
            }

            if slot.sql_hash.load(Ordering::Relaxed) != hash {
                continue;
            }

            let dc = slot.dialect_and_clock.load(Ordering::Relaxed);
            if (dc as u8) != dialect_tag {
                continue;
            }

            let sql_len = slot.sql_len.load(Ordering::Relaxed) as usize;
            let plan_len = slot.plan_len.load(Ordering::Relaxed) as usize;
            let param_count = slot.param_count.load(Ordering::Relaxed);

            if sql_len + plan_len > STMT_SLOT_PAYLOAD_BYTES {
                continue;
            }

            // Copy 64-bit words from payload
            let total_bytes = sql_len + plan_len;
            let num_words = total_bytes.div_ceil(8);
            let mut words = [0u64; 28];
            for (i, word) in words.iter_mut().enumerate().take(num_words.min(28)) {
                *word = slot.payload[i].load(Ordering::Relaxed);
            }

            std::sync::atomic::fence(Ordering::Acquire);
            let seq2 = slot.seq.load(Ordering::Relaxed);

            // Optimistic seqlock validation
            if seq1 == seq2 {
                slot.dialect_and_clock
                    .fetch_or(CLOCK_REF_BIT, Ordering::Relaxed);
                hdr.hits.fetch_add(1, Ordering::Relaxed);

                let byte_ptr = words.as_ptr() as *const u8;
                // SAFETY: `total_bytes == sql_len + plan_len <= STMT_SLOT_PAYLOAD_BYTES` (224) == `size_of_val(&words)`.
                let payload_slice = unsafe { std::slice::from_raw_parts(byte_ptr, total_bytes) };

                let compiled_sql = payload_slice[..sql_len].to_vec();
                let param_plan = payload_slice[sql_len..sql_len + plan_len].to_vec();

                return Some(StatementPlanEntry {
                    sql_hash: hash,
                    dialect_tag,
                    param_count,
                    compiled_sql,
                    param_plan,
                });
            }
        }

        hdr.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn insert(
        &self,
        dialect_tag: u8,
        raw_sql: &[u8],
        param_count: u16,
        compiled_sql: &[u8],
        param_plan: &[u8],
    ) -> bool {
        let total_bytes = compiled_sql.len() + param_plan.len();
        if total_bytes > STMT_SLOT_PAYLOAD_BYTES {
            return false;
        }

        let hash = Self::compute_sql_hash(dialect_tag, raw_sql);
        let hdr = self.header();
        let set_idx = (hash as usize) & (hdr.set_mask as usize);
        let base = set_idx * 4;

        // Choose victim way
        let mut target_way = None;

        // Pass 1: exact match or empty slot
        for way in 0..4 {
            let slot = self.slot(base + way);
            let seq = slot.seq.load(Ordering::Relaxed);
            if seq == 0 {
                target_way = Some(way);
                break;
            }
            if (seq & 1) == 0
                && slot.sql_hash.load(Ordering::Relaxed) == hash
                && (slot.dialect_and_clock.load(Ordering::Relaxed) as u8) == dialect_tag
            {
                target_way = Some(way);
                break;
            }
        }

        // Pass 2: Clock sweep across the 4 ways
        if target_way.is_none() {
            for _ in 0..2 {
                for way in 0..4 {
                    let slot = self.slot(base + way);
                    let dc = slot.dialect_and_clock.load(Ordering::Relaxed);
                    if (dc & CLOCK_REF_BIT) != 0 {
                        slot.dialect_and_clock
                            .fetch_and(!CLOCK_REF_BIT, Ordering::Relaxed);
                    } else {
                        target_way = Some(way);
                        break;
                    }
                }
                if target_way.is_some() {
                    break;
                }
            }
        }

        let way = target_way.unwrap_or(0);
        let slot = self.slot(base + way);

        // Claim slot for writing via seqlock odd increment, recovering if a writer crashed mid-update.
        const MAX_TOTAL_INSERT_SPINS: u32 = MAX_STUCK_SPINS * 16;
        let mut cur = slot.seq.load(Ordering::Acquire);
        let mut last_odd_seq = 0u64;
        let mut stuck_spins = 0u32;
        let mut total_spins = 0u32;
        let (claimed_odd, was_eviction) = loop {
            if (cur & 1) == 0 {
                match slot.seq.compare_exchange_weak(
                    cur,
                    cur.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break (cur.wrapping_add(1), cur > 0),
                    Err(actual) => {
                        cur = actual;
                    }
                }
            } else {
                if cur == last_odd_seq {
                    stuck_spins += 1;
                } else {
                    last_odd_seq = cur;
                    stuck_spins = 1;
                }
                if stuck_spins >= MAX_STUCK_SPINS {
                    let next_odd = cur.saturating_add(2);
                    match slot.seq.compare_exchange(
                        cur,
                        next_odd,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break (next_odd, true),
                        Err(actual) => {
                            cur = actual;
                            last_odd_seq = 0;
                            stuck_spins = 0;
                        }
                    }
                } else {
                    if stuck_spins > 64 {
                        std::thread::yield_now();
                    } else {
                        std::hint::spin_loop();
                    }
                    cur = slot.seq.load(Ordering::Acquire);
                }
            }
            total_spins += 1;
            if total_spins >= MAX_TOTAL_INSERT_SPINS {
                return false;
            }
        };

        // Prepare payload buffer
        let mut payload_buf = [0u8; STMT_SLOT_PAYLOAD_BYTES];
        payload_buf[..compiled_sql.len()].copy_from_slice(compiled_sql);
        payload_buf[compiled_sql.len()..compiled_sql.len() + param_plan.len()]
            .copy_from_slice(param_plan);

        let num_words = total_bytes.div_ceil(8).min(28);

        // Write fields
        slot.dialect_and_clock
            .store((dialect_tag as u32) | CLOCK_REF_BIT, Ordering::Relaxed);
        slot.param_count.store(param_count, Ordering::Relaxed);
        slot.sql_len
            .store(compiled_sql.len() as u16, Ordering::Relaxed);
        slot.plan_len
            .store(param_plan.len() as u16, Ordering::Relaxed);

        for (i, chunk) in payload_buf
            .as_chunks::<8>()
            .0
            .iter()
            .take(num_words)
            .enumerate()
        {
            slot.payload[i].store(u64::from_ne_bytes(*chunk), Ordering::Relaxed);
        }

        slot.sql_hash.store(hash, Ordering::Relaxed);

        // Commit seqlock: next_odd + 1 (never 0)
        let next_even = claimed_odd.wrapping_add(1);
        let pub_seq = if next_even == 0 { 2 } else { next_even };
        slot.seq.store(pub_seq, Ordering::Release);

        hdr.inserts.fetch_add(1, Ordering::Relaxed);
        if was_eviction {
            hdr.evictions.fetch_add(1, Ordering::Relaxed);
        }

        true
    }

    pub fn stats(&self) -> StatementCacheStats {
        let hdr = self.header();
        StatementCacheStats {
            capacity: hdr.capacity,
            hits: hdr.hits.load(Ordering::Relaxed),
            misses: hdr.misses.load(Ordering::Relaxed),
            inserts: hdr.inserts.load(Ordering::Relaxed),
            evictions: hdr.evictions.load(Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Shared-Memory Key-Value Cache (ShmCache)
// ============================================================================

pub const SHM_CACHE_MAX_KEY_BYTES: usize = 128;
pub const SHM_CACHE_MAX_VALUE_BYTES: usize = 856;
pub const MAX_SHM_CACHE_CAPACITY: u32 = 1 << 20;
pub const SHM_CACHE_PROBE_STEPS: usize = 16;

#[repr(C, align(64))]
pub struct ShmCacheSlot {
    pub seq: AtomicU64,
    pub key_hash: AtomicU64,
    pub expiry_ns: AtomicU64,
    pub val_len: AtomicU32,
    pub clock_and_flags: AtomicU32,
    pub key_len: AtomicU16,
    pub _reserved: [u8; 6],
    pub key: [AtomicU64; 16],
    pub value: [AtomicU64; 107],
}

const _: () = assert!(std::mem::size_of::<ShmCacheSlot>() == 1024);
const _: () = assert!(std::mem::align_of::<ShmCacheSlot>() == 64);

#[repr(C, align(64))]
pub struct ShmCacheHeader {
    pub capacity: u32,
    pub max_entries: u32,
    pub capacity_bytes: u64,
    pub clock_hand: AtomicU32,
    pub _pad0: [u8; 4],
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub sets: AtomicU64,
    pub evictions: AtomicU64,
    pub expirations: AtomicU64,
}

const _: () = assert!(std::mem::size_of::<ShmCacheHeader>() == 64);
const _: () = assert!(std::mem::align_of::<ShmCacheHeader>() == 64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShmCacheStats {
    pub capacity: u32,
    pub max_entries: u32,
    pub capacity_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub sets: u64,
    pub evictions: u64,
    pub expirations: u64,
}

pub struct ShmCache {
    header: *mut ShmCacheHeader,
    slots: *mut ShmCacheSlot,
    capacity: usize,
    _region: Option<Arc<ShmRegion>>,
    lease_table: std::sync::RwLock<Option<Arc<ShmLeaseTable>>>,
}

#[derive(Clone, Copy)]
enum CacheSlotSnapshot {
    Empty,
    Tombstone {
        seq: u64,
    },
    Unreadable,
    Occupied {
        seq: u64,
        key_hash: u64,
        expiry_ns: u64,
        key_len: usize,
        key_words: [u64; 16],
    },
}

// SAFETY: All `ShmCacheHeader` and `ShmCacheSlot` fields accessed after initialization use atomics,
// and `capacity` is validated once and stored immutably on `ShmCache`.
unsafe impl Send for ShmCache {}
// SAFETY: Shared references only perform atomic operations or read immutable capacity/pointers.
unsafe impl Sync for ShmCache {}

impl ShmCache {
    #[inline]
    pub fn compute_key_hash(key: &[u8]) -> u64 {
        let mut hash = 0xcbf29ce484222325u64;
        for &b in key {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    pub const fn required_size(capacity: u32) -> usize {
        let base = if capacity < 16 { 16 } else { capacity };
        let cap = if base > MAX_SHM_CACHE_CAPACITY {
            MAX_SHM_CACHE_CAPACITY
        } else {
            base.next_power_of_two()
        };
        let raw = std::mem::size_of::<ShmCacheHeader>()
            .saturating_add((cap as usize).saturating_mul(std::mem::size_of::<ShmCacheSlot>()));
        raw.saturating_add(63) & !63
    }

    pub fn from_region_at_offset(
        region: Arc<ShmRegion>,
        offset: usize,
        capacity: u32,
        create: bool,
    ) -> Result<Self, BrokerError> {
        if !offset.is_multiple_of(std::mem::align_of::<ShmCacheHeader>()) {
            return Err(BrokerError::InvalidCapacity);
        }

        let validated_cap = if create {
            let base = capacity.max(16);
            if base > MAX_SHM_CACHE_CAPACITY {
                return Err(BrokerError::InvalidCapacity);
            }
            let cap = base
                .checked_next_power_of_two()
                .ok_or(BrokerError::InvalidCapacity)?;
            let req_size = Self::required_size(cap);
            let expected_end = offset
                .checked_add(req_size)
                .ok_or(BrokerError::InvalidCapacity)?;
            if region.len() < expected_end {
                return Err(BrokerError::RegionTooSmall {
                    expected: expected_end,
                    actual: region.len(),
                });
            }
            // SAFETY: `region` spans at least `offset + req_size` bytes and `offset` is 64B-aligned.
            unsafe {
                std::ptr::write_bytes(region.as_ptr().add(offset), 0, req_size);
                let hdr = &mut *(region.as_ptr().add(offset) as *mut ShmCacheHeader);
                hdr.capacity = cap;
                hdr.max_entries = cap;
                hdr.capacity_bytes = req_size as u64;
                hdr.clock_hand.store(0, Ordering::Relaxed);
                hdr.hits.store(0, Ordering::Relaxed);
                hdr.misses.store(0, Ordering::Relaxed);
                hdr.sets.store(0, Ordering::Relaxed);
                hdr.evictions.store(0, Ordering::Relaxed);
                hdr.expirations.store(0, Ordering::Relaxed);
            }
            cap
        } else {
            let hdr_end = offset
                .checked_add(std::mem::size_of::<ShmCacheHeader>())
                .ok_or(BrokerError::InvalidCapacity)?;
            if region.len() < hdr_end {
                return Err(BrokerError::RegionTooSmall {
                    expected: hdr_end,
                    actual: region.len(),
                });
            }
            // SAFETY: `region` spans at least `offset + size_of::<ShmCacheHeader>()` and `offset` is 64B-aligned.
            let cap = unsafe {
                std::ptr::addr_of!(
                    (*(region.as_ptr().add(offset) as *const ShmCacheHeader)).capacity
                )
                .read_volatile()
            };
            if !(16..=MAX_SHM_CACHE_CAPACITY).contains(&cap) || !cap.is_power_of_two() {
                return Err(BrokerError::InvalidCapacity);
            }
            let req_size = Self::required_size(cap);
            let expected_end = offset
                .checked_add(req_size)
                .ok_or(BrokerError::InvalidCapacity)?;
            if region.len() < expected_end {
                return Err(BrokerError::RegionTooSmall {
                    expected: expected_end,
                    actual: region.len(),
                });
            }
            cap
        };

        // SAFETY: `offset + Self::required_size(validated_cap) <= region.len()` was verified above.
        let header_ptr = unsafe { region.as_ptr().add(offset) as *mut ShmCacheHeader };
        // SAFETY: `slots_ptr` immediately follows `ShmCacheHeader` within the validated region.
        let slots_ptr = unsafe {
            (header_ptr as *mut u8).add(std::mem::size_of::<ShmCacheHeader>()) as *mut ShmCacheSlot
        };

        Ok(Self {
            header: header_ptr,
            slots: slots_ptr,
            capacity: validated_cap as usize,
            _region: Some(region),
            lease_table: std::sync::RwLock::new(None),
        })
    }

    pub fn init_in_region(region: ShmRegion, capacity: u32) -> Result<Self, BrokerError> {
        Self::from_region_at_offset(Arc::new(region), 0, capacity, true)
    }

    pub fn attach_region(region: ShmRegion, capacity: u32) -> Result<Self, BrokerError> {
        Self::from_region_at_offset(Arc::new(region), 0, capacity, false)
    }

    #[cfg(unix)]
    pub fn open_or_create_named(
        name: &str,
        capacity_bytes: usize,
        max_entries: u32,
    ) -> Result<(Self, bool), BrokerError> {
        let entries_from_bytes = (capacity_bytes / std::mem::size_of::<ShmCacheSlot>()) as u32;
        let target = max_entries.max(entries_from_bytes).max(16);
        if target > MAX_SHM_CACHE_CAPACITY {
            return Err(BrokerError::InvalidCapacity);
        }
        let cap = target
            .checked_next_power_of_two()
            .ok_or(BrokerError::InvalidCapacity)?;
        let req_size = Self::required_size(cap);
        let normalized = if name.starts_with('/') {
            name.to_string()
        } else {
            format!("/{name}")
        };
        let attach = ShmRegion::open_or_create_named(&normalized, req_size, |region| {
            let len = region.len();
            if len < req_size {
                return Err(ShmError::InvalidSize);
            }
            // SAFETY: Exclusive initialization closure owns `region` up to `req_size`.
            unsafe {
                let slice = region.as_mut_slice();
                slice[..req_size].fill(0);
                let chdr = &mut *(slice.as_mut_ptr() as *mut ShmCacheHeader);
                chdr.capacity = cap;
                chdr.max_entries = cap;
                chdr.capacity_bytes = req_size as u64;
                chdr.clock_hand.store(0, Ordering::Relaxed);
            }
            Ok(())
        })
        .map_err(|e| map_shm_error(e, &normalized))?;

        let (mut region, created) = match attach {
            NamedAttach::Created(r) => (r, true),
            NamedAttach::Attached(r) => (r, false),
        };
        if !created && region.len() >= std::mem::size_of::<ShmCacheHeader>() {
            // SAFETY: `region.len() >= size_of::<ShmCacheHeader>()`.
            let hdr_cap = unsafe {
                std::ptr::addr_of!((*(region.as_ptr() as *const ShmCacheHeader)).capacity)
                    .read_volatile()
            };
            if (16..=MAX_SHM_CACHE_CAPACITY).contains(&hdr_cap) && hdr_cap.is_power_of_two() {
                let needed = Self::required_size(hdr_cap);
                if region.len() < needed {
                    drop(region);
                    region = ShmRegion::open_named(&normalized, needed)
                        .map_err(|e| map_shm_error(e, &normalized))?;
                }
            }
        }
        let cache = Self::attach_region(region, cap)?;
        Ok((cache, created))
    }

    #[inline]
    fn header(&self) -> &ShmCacheHeader {
        // SAFETY: `self.header` points to a validated `ShmCacheHeader` kept alive by `self._region`.
        unsafe { &*self.header }
    }

    #[inline]
    fn slot(&self, index: usize) -> &ShmCacheSlot {
        debug_assert!(index < self.capacity);
        // SAFETY: `index < self.capacity` and `self.slots` was validated for `self.capacity` entries.
        unsafe { &*self.slots.add(index) }
    }

    #[inline]
    pub fn capacity(&self) -> u32 {
        self.capacity as u32
    }

    #[cfg(test)]
    fn set_slot_expiry_for_test(&self, index: usize, expiry_ns: u64) {
        let slot = self.slot(index);
        let seq = slot.seq.load(Ordering::Acquire);
        let odd_seq = if (seq & 1) == 0 {
            seq.wrapping_add(1)
        } else {
            seq
        };
        slot.seq.store(odd_seq, Ordering::Release);
        slot.expiry_ns.store(expiry_ns, Ordering::Relaxed);
        Self::unlock_slot(slot, odd_seq);
    }

    #[inline]
    fn unlock_slot(slot: &ShmCacheSlot, odd_seq: u64) {
        debug_assert_eq!(odd_seq & 1, 1);
        let next_even = odd_seq.wrapping_add(1);
        let pub_seq = if next_even == 0 { 2 } else { next_even };
        slot.seq.store(pub_seq, Ordering::Release);
    }

    #[inline]
    fn write_tombstone_locked(slot: &ShmCacheSlot, odd_seq: u64) {
        slot.key_hash.store(0, Ordering::Relaxed);
        slot.expiry_ns.store(0, Ordering::Relaxed);
        slot.key_len.store(0, Ordering::Relaxed);
        slot.val_len.store(0, Ordering::Relaxed);
        slot.clock_and_flags.store(0, Ordering::Relaxed);
        Self::unlock_slot(slot, odd_seq);
    }

    #[inline]
    fn write_entry_locked(
        slot: &ShmCacheSlot,
        odd_seq: u64,
        hash: u64,
        key: &[u8],
        value: &[u8],
        expiry_ns: u64,
    ) {
        debug_assert!(key.len() <= SHM_CACHE_MAX_KEY_BYTES);
        debug_assert!(value.len() <= SHM_CACHE_MAX_VALUE_BYTES);

        let mut key_buf = [0u8; SHM_CACHE_MAX_KEY_BYTES];
        key_buf[..key.len()].copy_from_slice(key);
        let num_key_words = key.len().div_ceil(8);
        let (key_chunks, _) = key_buf.as_chunks::<8>();
        for (dst, chunk) in slot.key.iter().zip(key_chunks.iter()).take(num_key_words) {
            let w = u64::from_ne_bytes(*chunk);
            dst.store(w, Ordering::Relaxed);
        }

        let mut val_buf = [0u8; SHM_CACHE_MAX_VALUE_BYTES];
        val_buf[..value.len()].copy_from_slice(value);
        let num_val_words = value.len().div_ceil(8);
        let (val_chunks, _) = val_buf.as_chunks::<8>();
        for (dst, chunk) in slot.value.iter().zip(val_chunks.iter()).take(num_val_words) {
            let w = u64::from_ne_bytes(*chunk);
            dst.store(w, Ordering::Relaxed);
        }

        slot.key_len.store(key.len() as u16, Ordering::Relaxed);
        slot.val_len.store(value.len() as u32, Ordering::Relaxed);
        slot.expiry_ns.store(expiry_ns, Ordering::Relaxed);
        slot.clock_and_flags.store(CLOCK_REF_BIT, Ordering::Relaxed);
        slot.key_hash.store(hash, Ordering::Relaxed);
        Self::unlock_slot(slot, odd_seq);
    }

    fn read_slot_snapshot(slot: &ShmCacheSlot) -> CacheSlotSnapshot {
        let mut last_odd_seq = 0u64;
        let mut stuck_spins = 0u32;
        loop {
            let seq1 = slot.seq.load(Ordering::Acquire);
            if seq1 == 0 {
                return CacheSlotSnapshot::Empty;
            }
            if (seq1 & 1) != 0 {
                if seq1 == last_odd_seq {
                    stuck_spins += 1;
                    if stuck_spins > 2048 {
                        return CacheSlotSnapshot::Unreadable;
                    }
                    if stuck_spins > 64 {
                        std::thread::yield_now();
                    } else {
                        std::hint::spin_loop();
                    }
                } else {
                    last_odd_seq = seq1;
                    stuck_spins = 1;
                    std::hint::spin_loop();
                }
                continue;
            }
            let klen = slot.key_len.load(Ordering::Relaxed) as usize;
            if klen == 0 {
                std::sync::atomic::fence(Ordering::Acquire);
                if slot.seq.load(Ordering::Relaxed) == seq1 {
                    return CacheSlotSnapshot::Tombstone { seq: seq1 };
                }
                std::hint::spin_loop();
                continue;
            }
            if klen > SHM_CACHE_MAX_KEY_BYTES {
                std::sync::atomic::fence(Ordering::Acquire);
                if slot.seq.load(Ordering::Relaxed) == seq1 {
                    return CacheSlotSnapshot::Unreadable;
                }
                std::hint::spin_loop();
                continue;
            }
            let key_hash = slot.key_hash.load(Ordering::Relaxed);
            let expiry_ns = slot.expiry_ns.load(Ordering::Relaxed);
            let vlen = slot.val_len.load(Ordering::Relaxed) as usize;
            if vlen > SHM_CACHE_MAX_VALUE_BYTES {
                std::sync::atomic::fence(Ordering::Acquire);
                if slot.seq.load(Ordering::Relaxed) == seq1 {
                    return CacheSlotSnapshot::Unreadable;
                }
                std::hint::spin_loop();
                continue;
            }
            let mut key_words = [0u64; 16];
            let num_kwords = klen.div_ceil(8);
            for (i, w) in key_words.iter_mut().enumerate().take(num_kwords) {
                *w = slot.key[i].load(Ordering::Relaxed);
            }
            std::sync::atomic::fence(Ordering::Acquire);
            if slot.seq.load(Ordering::Relaxed) == seq1 {
                return CacheSlotSnapshot::Occupied {
                    seq: seq1,
                    key_hash,
                    expiry_ns,
                    key_len: klen,
                    key_words,
                };
            }
            std::hint::spin_loop();
        }
    }

    #[inline]
    fn key_words_eq(key_words: &[u64; 16], klen: usize, expected: &[u8]) -> bool {
        if klen != expected.len() || klen > SHM_CACHE_MAX_KEY_BYTES {
            return false;
        }
        // SAFETY: `klen <= SHM_CACHE_MAX_KEY_BYTES` (128) == `size_of_val(key_words)`.
        let key_bytes =
            unsafe { std::slice::from_raw_parts(key_words.as_ptr().cast::<u8>(), klen) };
        key_bytes == expected
    }

    fn select_clock_victim(&self, start_idx: usize, max_probes: usize, mask: usize) -> usize {
        for _ in 0..2 {
            for step in 0..max_probes {
                let idx = (start_idx + step) & mask;
                let slot = self.slot(idx);
                let cf = slot.clock_and_flags.load(Ordering::Relaxed);
                if (cf & CLOCK_REF_BIT) != 0 {
                    slot.clock_and_flags
                        .fetch_and(!CLOCK_REF_BIT, Ordering::Relaxed);
                } else {
                    return idx;
                }
            }
        }
        start_idx
    }

    pub fn attach_lease_table(&self, table: Arc<ShmLeaseTable>) {
        if let Ok(mut guard) = self.lease_table.write() {
            *guard = Some(table);
        }
    }

    fn publish_invalidation(&self, payload: &[u8]) {
        if let Ok(guard) = self.lease_table.read()
            && let Some(table) = guard.as_ref()
        {
            let _ = table.publish_channel(b"vtb:cache:inv", payload, 0, None);
        }
    }

    pub fn drain_invalidations(&self, max_frames: usize) -> Vec<Vec<u8>> {
        if let Ok(guard) = self.lease_table.read()
            && let Some(table) = guard.as_ref()
        {
            let frames = table.drain_channel(b"vtb:cache:inv");
            return frames
                .into_iter()
                .take(max_frames)
                .map(|f| f.payload)
                .collect();
        }
        Vec::new()
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if key.is_empty() || key.len() > SHM_CACHE_MAX_KEY_BYTES {
            self.header().misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let hash = Self::compute_key_hash(key);
        let hdr = self.header();
        let cap = self.capacity;
        let mask = cap - 1;
        let start_idx = (hash as usize) & mask;
        let now = monotonic_now_ns();

        for step in 0..SHM_CACHE_PROBE_STEPS.min(cap) {
            let idx = (start_idx + step) & mask;
            let slot = self.slot(idx);
            let mut last_odd_seq = 0u64;
            let mut stuck_spins = 0u32;

            loop {
                let seq1 = slot.seq.load(Ordering::Acquire);
                if seq1 == 0 {
                    hdr.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                if (seq1 & 1) != 0 {
                    if seq1 == last_odd_seq {
                        stuck_spins += 1;
                        if stuck_spins > 2048 {
                            break;
                        }
                        if stuck_spins > 64 {
                            std::thread::yield_now();
                        } else {
                            std::hint::spin_loop();
                        }
                    } else {
                        last_odd_seq = seq1;
                        stuck_spins = 1;
                        std::hint::spin_loop();
                    }
                    continue;
                }
                let klen = slot.key_len.load(Ordering::Relaxed) as usize;
                if klen == 0 {
                    std::sync::atomic::fence(Ordering::Acquire);
                    if slot.seq.load(Ordering::Relaxed) == seq1 {
                        break;
                    }
                    std::hint::spin_loop();
                    continue;
                }
                if klen > SHM_CACHE_MAX_KEY_BYTES {
                    std::sync::atomic::fence(Ordering::Acquire);
                    if slot.seq.load(Ordering::Relaxed) == seq1 {
                        break;
                    }
                    std::hint::spin_loop();
                    continue;
                }
                let slot_hash = slot.key_hash.load(Ordering::Relaxed);
                if slot_hash != hash || klen != key.len() {
                    std::sync::atomic::fence(Ordering::Acquire);
                    if slot.seq.load(Ordering::Relaxed) == seq1 {
                        break;
                    }
                    std::hint::spin_loop();
                    continue;
                }
                let exp = slot.expiry_ns.load(Ordering::Relaxed);
                let vlen = slot.val_len.load(Ordering::Relaxed) as usize;
                if vlen > SHM_CACHE_MAX_VALUE_BYTES {
                    std::sync::atomic::fence(Ordering::Acquire);
                    if slot.seq.load(Ordering::Relaxed) == seq1 {
                        break;
                    }
                    std::hint::spin_loop();
                    continue;
                }

                let mut key_words = [0u64; 16];
                let num_kwords = klen.div_ceil(8);
                for (i, w) in key_words.iter_mut().enumerate().take(num_kwords) {
                    *w = slot.key[i].load(Ordering::Relaxed);
                }

                let expired = exp > 0 && now >= exp;
                let mut val_words = [0u64; 107];
                if !expired {
                    let num_vwords = vlen.div_ceil(8);
                    for (i, w) in val_words.iter_mut().enumerate().take(num_vwords) {
                        *w = slot.value[i].load(Ordering::Relaxed);
                    }
                }

                std::sync::atomic::fence(Ordering::Acquire);
                let seq2 = slot.seq.load(Ordering::Relaxed);
                if seq1 != seq2 {
                    std::hint::spin_loop();
                    continue;
                }

                if !Self::key_words_eq(&key_words, klen, key) {
                    break;
                }
                if expired {
                    hdr.expirations.fetch_add(1, Ordering::Relaxed);
                    hdr.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                slot.clock_and_flags
                    .fetch_or(CLOCK_REF_BIT, Ordering::Relaxed);
                hdr.hits.fetch_add(1, Ordering::Relaxed);
                // SAFETY: `vlen <= SHM_CACHE_MAX_VALUE_BYTES` (856) == `size_of_val(&val_words)`.
                let val_bytes =
                    unsafe { std::slice::from_raw_parts(val_words.as_ptr().cast::<u8>(), vlen) };
                return Some(val_bytes.to_vec());
            }
        }

        hdr.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn set(&self, key: &[u8], value: &[u8], ttl_ns: Option<u64>) -> bool {
        if key.is_empty()
            || key.len() > SHM_CACHE_MAX_KEY_BYTES
            || value.len() > SHM_CACHE_MAX_VALUE_BYTES
        {
            return false;
        }
        let hash = Self::compute_key_hash(key);
        let hdr = self.header();
        let cap = self.capacity;
        let mask = cap - 1;
        let start_idx = (hash as usize) & mask;
        let max_probes = SHM_CACHE_PROBE_STEPS.min(cap);

        for _attempt in 0..65_536 {
            let now = monotonic_now_ns();
            let expiry = ttl_ns.map(|ns| now.saturating_add(ns)).unwrap_or(0);
            let mut existing_match: Option<(usize, u64)> = None;
            let mut first_reusable: Option<(usize, u64)> = None;

            for step in 0..max_probes {
                let idx = (start_idx + step) & mask;
                let slot = self.slot(idx);
                match Self::read_slot_snapshot(slot) {
                    CacheSlotSnapshot::Empty => {
                        if first_reusable.is_none() {
                            first_reusable = Some((idx, 0));
                        }
                        break;
                    }
                    CacheSlotSnapshot::Tombstone { seq } => {
                        if first_reusable.is_none() {
                            first_reusable = Some((idx, seq));
                        }
                    }
                    CacheSlotSnapshot::Unreadable => {}
                    CacheSlotSnapshot::Occupied {
                        seq,
                        key_hash,
                        expiry_ns,
                        key_len,
                        key_words,
                    } => {
                        if key_hash == hash && Self::key_words_eq(&key_words, key_len, key) {
                            existing_match = Some((idx, seq));
                            break;
                        }
                        if expiry_ns > 0 && now >= expiry_ns && first_reusable.is_none() {
                            first_reusable = Some((idx, seq));
                        }
                    }
                }
            }

            let (target_idx, expected_seq, is_eviction) = if let Some((idx, seq)) = existing_match {
                (idx, seq, false)
            } else if let Some((idx, seq)) = first_reusable {
                (idx, seq, false)
            } else {
                let chosen = self.select_clock_victim(start_idx, max_probes, mask);
                let seq = self.slot(chosen).seq.load(Ordering::Acquire);
                if (seq & 1) != 0 {
                    std::hint::spin_loop();
                    continue;
                }
                (chosen, seq, true)
            };

            let slot = self.slot(target_idx);
            let odd_seq = expected_seq.wrapping_add(1);
            if slot
                .seq
                .compare_exchange(expected_seq, odd_seq, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                Self::write_entry_locked(slot, odd_seq, hash, key, value, expiry);
                if is_eviction {
                    hdr.evictions.fetch_add(1, Ordering::Relaxed);
                }
                hdr.sets.fetch_add(1, Ordering::Relaxed);
                self.publish_invalidation(key);
                return true;
            }
            std::hint::spin_loop();
        }
        false
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        if key.is_empty() || key.len() > SHM_CACHE_MAX_KEY_BYTES {
            return false;
        }
        let hash = Self::compute_key_hash(key);
        let cap = self.capacity;
        let mask = cap - 1;
        let start_idx = (hash as usize) & mask;
        let max_probes = SHM_CACHE_PROBE_STEPS.min(cap);

        for _attempt in 0..65_536 {
            let now = monotonic_now_ns();
            let mut contended = false;

            for step in 0..max_probes {
                let idx = (start_idx + step) & mask;
                let slot = self.slot(idx);
                match Self::read_slot_snapshot(slot) {
                    CacheSlotSnapshot::Empty => return false,
                    CacheSlotSnapshot::Tombstone { .. } | CacheSlotSnapshot::Unreadable => {}
                    CacheSlotSnapshot::Occupied {
                        seq,
                        key_hash,
                        expiry_ns,
                        key_len,
                        key_words,
                    } => {
                        if key_hash == hash && Self::key_words_eq(&key_words, key_len, key) {
                            let was_live = expiry_ns == 0 || now < expiry_ns;
                            let odd_seq = seq.wrapping_add(1);
                            if slot
                                .seq
                                .compare_exchange(seq, odd_seq, Ordering::AcqRel, Ordering::Acquire)
                                .is_ok()
                            {
                                Self::write_tombstone_locked(slot, odd_seq);
                                if was_live {
                                    self.publish_invalidation(key);
                                }
                                return was_live;
                            }
                            contended = true;
                            break;
                        }
                    }
                }
            }

            if !contended {
                return false;
            }
            std::hint::spin_loop();
        }
        false
    }

    pub fn add(&self, key: &[u8], value: &[u8], ttl_ns: Option<u64>) -> bool {
        if key.is_empty()
            || key.len() > SHM_CACHE_MAX_KEY_BYTES
            || value.len() > SHM_CACHE_MAX_VALUE_BYTES
        {
            return false;
        }
        let hash = Self::compute_key_hash(key);
        let hdr = self.header();
        let cap = self.capacity;
        let mask = cap - 1;
        let start_idx = (hash as usize) & mask;
        let max_probes = SHM_CACHE_PROBE_STEPS.min(cap);

        for _attempt in 0..65_536 {
            let now = monotonic_now_ns();
            let expiry = ttl_ns.map(|ns| now.saturating_add(ns)).unwrap_or(0);
            let mut existing_expired: Option<(usize, u64)> = None;
            let mut first_reusable: Option<(usize, u64)> = None;

            for step in 0..max_probes {
                let idx = (start_idx + step) & mask;
                let slot = self.slot(idx);
                match Self::read_slot_snapshot(slot) {
                    CacheSlotSnapshot::Empty => {
                        if first_reusable.is_none() {
                            first_reusable = Some((idx, 0));
                        }
                        break;
                    }
                    CacheSlotSnapshot::Tombstone { seq } => {
                        if first_reusable.is_none() {
                            first_reusable = Some((idx, seq));
                        }
                    }
                    CacheSlotSnapshot::Unreadable => {}
                    CacheSlotSnapshot::Occupied {
                        seq,
                        key_hash,
                        expiry_ns,
                        key_len,
                        key_words,
                    } => {
                        if key_hash == hash && Self::key_words_eq(&key_words, key_len, key) {
                            if expiry_ns == 0 || now < expiry_ns {
                                return false;
                            }
                            existing_expired = Some((idx, seq));
                            break;
                        }
                        if expiry_ns > 0 && now >= expiry_ns && first_reusable.is_none() {
                            first_reusable = Some((idx, seq));
                        }
                    }
                }
            }

            let (target_idx, expected_seq, is_eviction) = if let Some((idx, seq)) = existing_expired
            {
                (idx, seq, false)
            } else if let Some((idx, seq)) = first_reusable {
                (idx, seq, false)
            } else {
                let chosen = self.select_clock_victim(start_idx, max_probes, mask);
                let seq = self.slot(chosen).seq.load(Ordering::Acquire);
                if (seq & 1) != 0 {
                    std::hint::spin_loop();
                    continue;
                }
                (chosen, seq, true)
            };

            let slot = self.slot(target_idx);
            let odd_seq = expected_seq.wrapping_add(1);
            if slot
                .seq
                .compare_exchange(expected_seq, odd_seq, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                Self::write_entry_locked(slot, odd_seq, hash, key, value, expiry);
                if is_eviction {
                    hdr.evictions.fetch_add(1, Ordering::Relaxed);
                }
                hdr.sets.fetch_add(1, Ordering::Relaxed);
                self.publish_invalidation(key);
                return true;
            }
            std::hint::spin_loop();
        }
        false
    }

    pub fn has_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    pub fn touch(&self, key: &[u8], ttl_ns: Option<u64>) -> bool {
        if key.is_empty() || key.len() > SHM_CACHE_MAX_KEY_BYTES {
            return false;
        }
        let hash = Self::compute_key_hash(key);
        let cap = self.capacity;
        let mask = cap - 1;
        let start_idx = (hash as usize) & mask;
        let max_probes = SHM_CACHE_PROBE_STEPS.min(cap);

        for _attempt in 0..65_536 {
            let now = monotonic_now_ns();
            let new_expiry = ttl_ns.map(|ns| now.saturating_add(ns)).unwrap_or(0);
            let mut contended = false;

            for step in 0..max_probes {
                let idx = (start_idx + step) & mask;
                let slot = self.slot(idx);
                match Self::read_slot_snapshot(slot) {
                    CacheSlotSnapshot::Empty => return false,
                    CacheSlotSnapshot::Tombstone { .. } | CacheSlotSnapshot::Unreadable => {}
                    CacheSlotSnapshot::Occupied {
                        seq,
                        key_hash,
                        expiry_ns,
                        key_len,
                        key_words,
                    } => {
                        if key_hash == hash && Self::key_words_eq(&key_words, key_len, key) {
                            if expiry_ns > 0 && now >= expiry_ns {
                                return false;
                            }
                            let odd_seq = seq.wrapping_add(1);
                            if slot
                                .seq
                                .compare_exchange(seq, odd_seq, Ordering::AcqRel, Ordering::Acquire)
                                .is_ok()
                            {
                                slot.expiry_ns.store(new_expiry, Ordering::Relaxed);
                                slot.clock_and_flags
                                    .fetch_or(CLOCK_REF_BIT, Ordering::Relaxed);
                                Self::unlock_slot(slot, odd_seq);
                                return true;
                            }
                            contended = true;
                            break;
                        }
                    }
                }
            }

            if !contended {
                return false;
            }
            std::hint::spin_loop();
        }
        false
    }

    pub fn incr(&self, key: &[u8], delta: i64) -> Result<i64, BrokerError> {
        if key.is_empty() || key.len() > SHM_CACHE_MAX_KEY_BYTES {
            return Err(BrokerError::InvalidCapacity);
        }
        let hash = Self::compute_key_hash(key);
        let hdr = self.header();
        let cap = self.capacity;
        let mask = cap - 1;
        let start_idx = (hash as usize) & mask;
        let max_probes = SHM_CACHE_PROBE_STEPS.min(cap);

        for _attempt in 0..65_536 {
            let now = monotonic_now_ns();
            let mut existing_match: Option<(usize, u64)> = None;
            let mut first_reusable: Option<(usize, u64)> = None;

            for step in 0..max_probes {
                let idx = (start_idx + step) & mask;
                let slot = self.slot(idx);
                match Self::read_slot_snapshot(slot) {
                    CacheSlotSnapshot::Empty => {
                        if first_reusable.is_none() {
                            first_reusable = Some((idx, 0));
                        }
                        break;
                    }
                    CacheSlotSnapshot::Tombstone { seq } => {
                        if first_reusable.is_none() {
                            first_reusable = Some((idx, seq));
                        }
                    }
                    CacheSlotSnapshot::Unreadable => {}
                    CacheSlotSnapshot::Occupied {
                        seq,
                        key_hash,
                        expiry_ns,
                        key_len,
                        key_words,
                    } => {
                        if key_hash == hash && Self::key_words_eq(&key_words, key_len, key) {
                            existing_match = Some((idx, seq));
                            break;
                        }
                        if expiry_ns > 0 && now >= expiry_ns && first_reusable.is_none() {
                            first_reusable = Some((idx, seq));
                        }
                    }
                }
            }

            if let Some((idx, expected_seq)) = existing_match {
                let slot = self.slot(idx);
                let odd_seq = expected_seq.wrapping_add(1);
                if slot
                    .seq
                    .compare_exchange(expected_seq, odd_seq, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    let exp = slot.expiry_ns.load(Ordering::Relaxed);
                    let live = exp == 0 || now < exp;
                    let (new_val, preserved_expiry) = if live {
                        let vlen = slot.val_len.load(Ordering::Relaxed) as usize;
                        let cur = if vlen <= SHM_CACHE_MAX_VALUE_BYTES {
                            let mut val_words = [0u64; 107];
                            let num_vwords = vlen.div_ceil(8);
                            for (i, w) in val_words.iter_mut().enumerate().take(num_vwords) {
                                *w = slot.value[i].load(Ordering::Relaxed);
                            }
                            // SAFETY: `vlen <= SHM_CACHE_MAX_VALUE_BYTES` (856) == `size_of_val(&val_words)`.
                            let val_bytes = unsafe {
                                std::slice::from_raw_parts(val_words.as_ptr().cast::<u8>(), vlen)
                            };
                            if let Ok(s) = std::str::from_utf8(val_bytes) {
                                s.trim().parse::<i64>().unwrap_or(0)
                            } else if val_bytes.len() == 8 {
                                i64::from_le_bytes(val_bytes.try_into().unwrap_or([0; 8]))
                            } else {
                                0
                            }
                        } else {
                            0
                        };
                        (cur.wrapping_add(delta), exp)
                    } else {
                        (delta, 0)
                    };
                    let encoded = new_val.to_string();
                    Self::write_entry_locked(
                        slot,
                        odd_seq,
                        hash,
                        key,
                        encoded.as_bytes(),
                        preserved_expiry,
                    );
                    hdr.sets.fetch_add(1, Ordering::Relaxed);
                    self.publish_invalidation(key);
                    return Ok(new_val);
                }
                std::hint::spin_loop();
                continue;
            }

            let (target_idx, expected_seq, is_eviction) = if let Some((idx, seq)) = first_reusable {
                (idx, seq, false)
            } else {
                let chosen = self.select_clock_victim(start_idx, max_probes, mask);
                let seq = self.slot(chosen).seq.load(Ordering::Acquire);
                if (seq & 1) != 0 {
                    std::hint::spin_loop();
                    continue;
                }
                (chosen, seq, true)
            };

            let slot = self.slot(target_idx);
            let odd_seq = expected_seq.wrapping_add(1);
            if slot
                .seq
                .compare_exchange(expected_seq, odd_seq, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let encoded = delta.to_string();
                Self::write_entry_locked(slot, odd_seq, hash, key, encoded.as_bytes(), 0);
                if is_eviction {
                    hdr.evictions.fetch_add(1, Ordering::Relaxed);
                }
                hdr.sets.fetch_add(1, Ordering::Relaxed);
                self.publish_invalidation(key);
                return Ok(delta);
            }
            std::hint::spin_loop();
        }
        Err(BrokerError::PoolExhausted)
    }

    pub fn decr(&self, key: &[u8], delta: i64) -> Result<i64, BrokerError> {
        self.incr(key, delta.wrapping_neg())
    }

    pub fn invalidate_prefix(&self, prefix: &[u8]) -> u32 {
        if prefix.len() > SHM_CACHE_MAX_KEY_BYTES {
            return 0;
        }
        let cap = self.capacity;
        let now = monotonic_now_ns();
        let mut count = 0u32;

        for idx in 0..cap {
            let slot = self.slot(idx);
            for _attempt in 0..64 {
                match Self::read_slot_snapshot(slot) {
                    CacheSlotSnapshot::Empty
                    | CacheSlotSnapshot::Tombstone { .. }
                    | CacheSlotSnapshot::Unreadable => break,
                    CacheSlotSnapshot::Occupied {
                        seq,
                        expiry_ns,
                        key_len,
                        key_words,
                        ..
                    } => {
                        if key_len < prefix.len() || key_len > SHM_CACHE_MAX_KEY_BYTES {
                            break;
                        }
                        // SAFETY: `key_len <= SHM_CACHE_MAX_KEY_BYTES` (128) == `size_of_val(&key_words)`.
                        let key_bytes = unsafe {
                            std::slice::from_raw_parts(key_words.as_ptr().cast::<u8>(), key_len)
                        };
                        if !key_bytes.starts_with(prefix) {
                            break;
                        }
                        let was_live = expiry_ns == 0 || now < expiry_ns;
                        let odd_seq = seq.wrapping_add(1);
                        if slot
                            .seq
                            .compare_exchange(seq, odd_seq, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            Self::write_tombstone_locked(slot, odd_seq);
                            if was_live {
                                count += 1;
                            }
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            }
        }
        if count > 0 {
            self.publish_invalidation(prefix);
        }
        count
    }

    pub fn clear(&self) {
        let cap = self.capacity;
        for idx in 0..cap {
            let slot = self.slot(idx);
            for _spin in 0..64 {
                let seq1 = slot.seq.load(Ordering::Acquire);
                if seq1 == 0 {
                    break;
                }
                if (seq1 & 1) != 0 {
                    std::hint::spin_loop();
                    continue;
                }
                let odd_seq = seq1.wrapping_add(1);
                if slot
                    .seq
                    .compare_exchange_weak(seq1, odd_seq, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    Self::write_tombstone_locked(slot, odd_seq);
                    break;
                }
                std::hint::spin_loop();
            }
        }
        self.publish_invalidation(b"");
    }

    pub fn stats(&self) -> ShmCacheStats {
        let hdr = self.header();
        ShmCacheStats {
            capacity: self.capacity as u32,
            max_entries: hdr.max_entries,
            capacity_bytes: hdr.capacity_bytes,
            hits: hdr.hits.load(Ordering::Relaxed),
            misses: hdr.misses.load(Ordering::Relaxed),
            sets: hdr.sets.load(Ordering::Relaxed),
            evictions: hdr.evictions.load(Ordering::Relaxed),
            expirations: hdr.expirations.load(Ordering::Relaxed),
        }
    }

    #[cfg(unix)]
    pub fn unlink(&self) -> bool {
        self._region.as_ref().is_some_and(|r| r.unlink_now())
    }

    #[cfg(not(unix))]
    pub fn unlink(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_table_basic_acquire_release() {
        let req_size = ShmLeaseTable::required_size(4, 2);
        let region = ShmRegion::create(req_size).unwrap();
        let table = ShmLeaseTable::init_in_region(region, 4, 2).unwrap();

        assert_eq!(table.pool_size(), 4);
        assert_eq!(table.max_overflow(), 2);
        assert_eq!(table.total_capacity(), 6);

        let grant1 = table.try_acquire(1001, 10_000_000_000, 42).unwrap();
        assert_eq!(grant1.worker_pid, 1001);
        assert!(!grant1.is_overflow);
        assert_eq!(grant1.slot_index, 0);

        let stats = table.stats();
        assert_eq!(stats.active_leases, 1);
        assert_eq!(stats.primary_in_use, 1);
        assert_eq!(stats.overflow_in_use, 0);

        table.release(grant1.slot_index, grant1.generation).unwrap();
        let stats_after = table.stats();
        assert_eq!(stats_after.active_leases, 0);
        assert_eq!(stats_after.primary_in_use, 0);
    }

    #[test]
    fn test_lease_table_overflow_marking() {
        let req_size = ShmLeaseTable::required_size(2, 2);
        let region = ShmRegion::create(req_size).unwrap();
        let table = ShmLeaseTable::init_in_region(region, 2, 2).unwrap();

        let g0 = table.try_acquire(1001, 0, 0).unwrap();
        let g1 = table.try_acquire(1002, 0, 0).unwrap();
        assert!(!g0.is_overflow);
        assert!(!g1.is_overflow);

        let g2 = table.try_acquire(1003, 0, 0).unwrap();
        let g3 = table.try_acquire(1004, 0, 0).unwrap();
        assert!(g2.is_overflow);
        assert!(g3.is_overflow);

        assert_eq!(
            table.try_acquire(1005, 0, 0),
            Err(BrokerError::PoolExhausted)
        );

        let stats = table.stats();
        assert_eq!(stats.active_leases, 4);
        assert_eq!(stats.primary_in_use, 2);
        assert_eq!(stats.overflow_in_use, 2);
    }

    #[test]
    fn test_lease_table_generation_mismatch() {
        let req_size = ShmLeaseTable::required_size(2, 0);
        let region = ShmRegion::create(req_size).unwrap();
        let table = ShmLeaseTable::init_in_region(region, 2, 0).unwrap();

        let g = table.try_acquire(1001, 0, 0).unwrap();
        // Stale generation release
        let err = table.release(g.slot_index, g.generation + 99);
        assert_eq!(err, Err(BrokerError::GenerationMismatch));

        table.release(g.slot_index, g.generation).unwrap();
        // Double release
        let err2 = table.release(g.slot_index, g.generation);
        assert!(err2.is_err());
    }

    #[test]
    fn test_statement_cache_lookup_insert() {
        let req_size = ShmStatementCache::required_size(16);
        let region = ShmRegion::create(req_size).unwrap();
        let cache = ShmStatementCache::init_in_region(region, 16).unwrap();

        let sql = b"SELECT id, name FROM users WHERE id = $1";
        let compiled = b"SELECT id, name FROM users WHERE id = :p0";
        let plan = b"\x91\xa2p0";

        assert_eq!(cache.lookup(1, sql), None);

        let ok = cache.insert(1, sql, 1, compiled, plan);
        assert!(ok);

        let entry = cache.lookup(1, sql).unwrap();
        assert_eq!(entry.dialect_tag, 1);
        assert_eq!(entry.param_count, 1);
        assert_eq!(entry.compiled_sql, compiled);
        assert_eq!(entry.param_plan, plan);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.inserts, 1);
    }

    #[test]
    fn test_colocated_statement_cache_single_region_and_no_overlap() {
        let req_size = ShmLeaseTable::required_size_with_statements(4, 2, 32);
        let region = ShmRegion::create(req_size).unwrap();
        let table = ShmLeaseTable::init_in_region_with_statements(region, 4, 2, 32).unwrap();

        assert!(table.stmt_offset() > 0);
        assert_eq!(table.stmt_capacity(), 32);

        let cache = table.statement_cache().expect("co-located statement cache");
        assert_eq!(cache.capacity(), 32);

        let sql = b"SELECT * FROM orders WHERE tenant_id = $1";
        let compiled = b"SELECT * FROM orders WHERE tenant_id = :p0";
        let plan = b"\x91\xa2p0";

        assert!(cache.put(1, sql, 1, compiled, plan));

        // Exhaust all leases while statement cache is populated
        let mut grants = Vec::new();
        for i in 0..6 {
            grants.push(table.try_acquire(2000 + i, 0, (i as u64) + 1).unwrap());
        }
        assert_eq!(table.stats().active_leases, 6);

        // Verify statement cache entry is completely unaffected by lease mutations
        let entry = cache.get(1, sql).expect("statement entry present");
        assert_eq!(entry.compiled_sql, compiled);
        assert_eq!(entry.param_plan, plan);

        // Attach a second handle from the same underlying FD (if unix)
        #[cfg(unix)]
        if let Some(fd) = table.region_handle() {
            // SAFETY: `fd` is a valid open shared-memory file descriptor owned by `table`.
            let dup_fd = unsafe { libc::dup(fd) };
            assert!(dup_fd >= 0);
            let region2 = ShmRegion::from_fd(dup_fd, table.region_size()).unwrap();
            let table2 = ShmLeaseTable::attach_region(region2).unwrap();
            let cache2 = table2.statement_cache().expect("attached statement cache");
            let entry2 = cache2.lookup(1, sql).expect("shared across fd");
            assert_eq!(entry2.compiled_sql, compiled);
        }

        // Invalidate all statements and verify leases remain intact
        cache.invalidate_all();
        assert_eq!(cache.get(1, sql), None);
        assert_eq!(table.stats().active_leases, 6);

        for g in grants {
            table.release(g.slot_index, g.generation).unwrap();
        }
        assert_eq!(table.stats().active_leases, 0);
    }

    #[test]
    fn test_active_leases_for_worker_and_reclaim_worker() {
        let req_size = ShmLeaseTable::required_size_with_statements(4, 2, 16);
        let region = ShmRegion::create(req_size).unwrap();
        let table = ShmLeaseTable::init_in_region_with_statements(region, 4, 2, 16).unwrap();

        let g1 = table.try_acquire(777, 0, 3).unwrap();
        let g2 = table.try_acquire(888, 0, 5).unwrap();
        let g3 = table.try_acquire(777, 0, 3).unwrap();

        let worker_777 = table.active_leases_for_worker(777);
        assert_eq!(worker_777.len(), 2);
        assert!(worker_777.contains(&(3, g1.slot_index, g1.generation)));
        assert!(worker_777.contains(&(3, g3.slot_index, g3.generation)));

        let worker_888 = table.active_leases_for_worker(888);
        assert_eq!(worker_888, vec![(5, g2.slot_index, g2.generation)]);

        let reclaimed = table.reclaim_worker(777);
        assert_eq!(reclaimed, 2);
        assert!(table.active_leases_for_worker(777).is_empty());
        assert_eq!(table.stats().active_leases, 1);

        table.release(g2.slot_index, g2.generation).unwrap();
        assert_eq!(table.stats().active_leases, 0);
    }

    #[test]
    fn test_channel_ring_publish_drain_and_multi_channel_isolation() {
        let req_size = ShmLeaseTable::required_size_with_statements(4, 2, 16);
        let region = ShmRegion::create(req_size).unwrap();
        let table = ShmLeaseTable::init_in_region_with_statements(region, 4, 2, 16).unwrap();

        assert!(table.publish_channel(b"cache_invalidate", b"users:1", 100, Some(b"0:1")));
        assert!(table.publish_channel(b"tx_events", b"commit:99", 200, None));
        assert!(table.publish_channel(b"cache_invalidate", b"users:2", 300, Some(b"1:2")));

        let inv_frames = table.drain_channel(b"cache_invalidate");
        assert_eq!(inv_frames.len(), 2);
        assert_eq!(inv_frames[0].channel, b"cache_invalidate");
        assert_eq!(inv_frames[0].payload, b"users:1");
        assert_eq!(inv_frames[0].timestamp_ns, 100);
        assert_eq!(inv_frames[0].lease_id.as_deref(), Some(&b"0:1"[..]));
        assert_eq!(inv_frames[1].payload, b"users:2");
        assert_eq!(inv_frames[1].timestamp_ns, 300);
        assert_eq!(inv_frames[1].lease_id.as_deref(), Some(&b"1:2"[..]));

        // Second drain of cache_invalidate is empty
        assert!(table.drain_channel(b"cache_invalidate").is_empty());

        // tx_events was not disturbed by draining cache_invalidate
        let tx_frames = table.drain_channel(b"tx_events");
        assert_eq!(tx_frames.len(), 1);
        assert_eq!(tx_frames[0].payload, b"commit:99");
        assert_eq!(tx_frames[0].lease_id, None);
    }

    #[test]
    fn test_invalid_huge_capacities_fail_loudly_without_overflow() {
        let region = ShmRegion::create(ShmLeaseTable::required_size(2, 0)).unwrap();
        assert_eq!(
            ShmLeaseTable::init_in_region(region, u32::MAX, 1).err(),
            Some(BrokerError::InvalidCapacity)
        );
        let region2 = ShmRegion::create(ShmLeaseTable::required_size(2, 0)).unwrap();
        assert_eq!(
            ShmLeaseTable::init_in_region_with_statements(region2, 2, 0, u32::MAX).err(),
            Some(BrokerError::InvalidCapacity)
        );
        assert!(ShmLeaseTable::required_size_with_statements(4096, 0, u32::MAX) > 0);
    }

    #[test]
    fn test_reap_expired_does_not_reap_reacquired_same_pid_lease_aba() {
        let req_size = ShmLeaseTable::required_size(1, 0);
        let region = ShmRegion::create(req_size).unwrap();
        let table = Arc::new(ShmLeaseTable::init_in_region(region, 1, 0).unwrap());

        let old = table.try_acquire(4242, 10_000_000, 1).unwrap();
        table.release(old.slot_index, old.generation).unwrap();
        let fresh = table.try_acquire(4242, 60_000_000_000, 1).unwrap();
        assert_ne!(fresh.generation, old.generation);

        let future = fresh.acquired_ns.saturating_add(50_000_000);
        assert_eq!(table.reap_expired(future), 0);
        assert_eq!(table.stats().active_leases, 1);

        let slot = table.slot(old.slot_index);
        let stale_raw = pack_state_pid(SLOT_STATE_PRIMARY, 4242);
        assert_eq!(
            table.release_claimed_slot(
                slot,
                stale_raw,
                Some(old.generation),
                true,
                &table.header().total_expired,
                ReleaseKind::Expire,
            ),
            Err(BrokerError::GenerationMismatch)
        );
        assert_eq!(table.stats().active_leases, 1);
        table.release(fresh.slot_index, fresh.generation).unwrap();
    }

    #[test]
    fn test_concurrent_register_partition_deduplicates_identical_keys() {
        let req_size = ShmLeaseTable::required_size(8, 2);
        let region = ShmRegion::create(req_size).unwrap();
        let table = Arc::new(ShmLeaseTable::init_in_region(region, 8, 2).unwrap());

        let key = stable_hash64(b"shared-partition-race");
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let ids: Vec<u16> = std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(8);
            for _ in 0..8 {
                let t = Arc::clone(&table);
                let b = Arc::clone(&barrier);
                handles.push(s.spawn(move || {
                    b.wait();
                    t.register_partition(PoolPartitionSpec {
                        pool_key_hash: key,
                        pool_size: 2,
                        max_overflow: 1,
                    })
                    .unwrap()
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let first = ids[0];
        assert!(ids.iter().all(|&id| id == first));
        assert_eq!(table.header().partition_count.0.load(Ordering::Acquire), 1);
    }

    #[test]
    fn test_quarantine_and_dispose_never_underflows_partition_counter() {
        let req_size = ShmLeaseTable::required_size(2, 0);
        let region = ShmRegion::create(req_size).unwrap();
        let table = Arc::new(ShmLeaseTable::init_in_region(region, 2, 0).unwrap());

        let key = stable_hash64(b"quarantine-dispose-race");
        let part = table
            .register_partition(PoolPartitionSpec {
                pool_key_hash: key,
                pool_size: 1,
                max_overflow: 0,
            })
            .unwrap();
        let grant = table.try_acquire_in(part, 5150, 0, key).unwrap().unwrap();
        assert_eq!(
            table.dispose_quarantined(grant.slot_index, grant.generation),
            Err(BrokerError::InvalidState)
        );
        table
            .quarantine(grant.slot_index, grant.generation)
            .unwrap();
        let stats = table.partition_stats(key).unwrap();
        assert_eq!(stats.quarantined, 1);
        table
            .dispose_quarantined(grant.slot_index, grant.generation)
            .unwrap();
        let after = table.partition_stats(key).unwrap();
        assert_eq!(after.quarantined, 0);
        assert_eq!(after.total_disposed, 1);
    }

    #[test]
    fn test_reclaim_worker_steals_stalled_claiming_slot_without_underflow() {
        let req_size = ShmLeaseTable::required_size(2, 0);
        let region = ShmRegion::create(req_size).unwrap();
        let table = Arc::new(ShmLeaseTable::init_in_region(region, 2, 0).unwrap());

        assert_eq!(
            table.slot(0).partition_id.load(Ordering::Acquire),
            NO_PARTITION
        );
        assert_eq!(
            table.slot(1).partition_id.load(Ordering::Acquire),
            NO_PARTITION
        );

        let key = stable_hash64(b"stalled-claim");
        let part = table
            .register_partition(PoolPartitionSpec {
                pool_key_hash: key,
                pool_size: 2,
                max_overflow: 0,
            })
            .unwrap();

        let grant = table
            .try_acquire_in(part, u32::MAX, 0, key)
            .unwrap()
            .unwrap();
        let slot = table.slot(grant.slot_index);
        let worker_raw = pack_state_pid(SLOT_STATE_PRIMARY, u32::MAX);
        let claiming_raw = pack_state_pid(SLOT_STATE_CLAIMING, u32::MAX);
        assert!(
            slot.state_and_pid
                .compare_exchange(
                    worker_raw,
                    claiming_raw,
                    Ordering::AcqRel,
                    Ordering::Acquire
                )
                .is_ok()
        );

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let t_reclaim = Arc::clone(&table);
        let b_reclaim = Arc::clone(&barrier);
        let handle = std::thread::spawn(move || {
            b_reclaim.wait();
            t_reclaim.reclaim_worker_leases(u32::MAX)
        });

        barrier.wait();
        while slot.state_and_pid.load(Ordering::Acquire) == claiming_raw {
            std::thread::yield_now();
        }
        assert!(
            slot.state_and_pid
                .compare_exchange(
                    claiming_raw,
                    pack_state_pid(SLOT_STATE_RECLAIMING, u32::MAX),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        );

        let reclaimed = handle.join().unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].slot_index, grant.slot_index);
        assert_eq!(reclaimed[0].generation, grant.generation + 1);
        assert_eq!(reclaimed[0].pool_id, Some(part));
        assert_eq!(table.partition_stats(key).unwrap().quarantined, 1);

        let g2 = table.try_acquire_in(part, 6262, 0, key).unwrap().unwrap();
        let slot2 = table.slot(g2.slot_index);
        let worker2_raw = pack_state_pid(SLOT_STATE_PRIMARY, 6262);
        let claiming2_raw = pack_state_pid(SLOT_STATE_CLAIMING, 6262);
        assert!(
            slot2
                .state_and_pid
                .compare_exchange(
                    worker2_raw,
                    claiming2_raw,
                    Ordering::AcqRel,
                    Ordering::Acquire
                )
                .is_ok()
        );
        assert_eq!(table.reclaim_worker(6262), 1);

        table
            .dispose_quarantined(reclaimed[0].slot_index, reclaimed[0].generation)
            .unwrap();
        assert_eq!(table.stats().active_leases, 0);
        assert_eq!(table.stats().quarantined, 0);
        let end_stats = table.partition_stats(key).unwrap();
        assert_eq!(end_stats.active_leases, 0);
        assert_eq!(end_stats.quarantined, 0);
        assert_eq!(end_stats.total_disposed, 1);
        assert_eq!(end_stats.total_reclaimed, 2);
    }

    #[test]
    fn test_concurrent_reclaim_vs_heartbeat_release_and_quarantine_preserves_invariants() {
        let req_size = ShmLeaseTable::required_size(4, 2);
        let region = ShmRegion::create(req_size).unwrap();
        let table = Arc::new(ShmLeaseTable::init_in_region(region, 4, 2).unwrap());

        let key = stable_hash64(b"concurrent-reclaim-stress");
        let part = table
            .register_partition(PoolPartitionSpec {
                pool_key_hash: key,
                pool_size: 4,
                max_overflow: 2,
            })
            .unwrap();

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let barrier = Arc::new(std::sync::Barrier::new(3));
        std::thread::scope(|s| {
            let t1 = Arc::clone(&table);
            let b1 = Arc::clone(&barrier);
            let s1 = Arc::clone(&stop);
            let worker = s.spawn(move || {
                b1.wait();
                for iter in 0..400u64 {
                    if let Ok(Some(g)) = t1.try_acquire_in(part, 8888, 0, key) {
                        let _ = t1.heartbeat(g.slot_index, g.generation);
                        if iter % 3 == 0 {
                            if t1.quarantine(g.slot_index, g.generation).is_ok() {
                                let _ = t1.dispose_quarantined(g.slot_index, g.generation);
                            }
                        } else {
                            let _ = t1.release(g.slot_index, g.generation);
                        }
                    }
                }
                s1.store(true, Ordering::Release);
            });

            let t2 = Arc::clone(&table);
            let b2 = Arc::clone(&barrier);
            let s2 = Arc::clone(&stop);
            let reaper = s.spawn(move || {
                b2.wait();
                while !s2.load(Ordering::Acquire) {
                    for r in t2.reclaim_worker_leases(8888) {
                        let _ = t2.dispose_quarantined(r.slot_index, r.generation);
                    }
                    std::thread::yield_now();
                }
            });

            barrier.wait();
            worker.join().unwrap();
            reaper.join().unwrap();
        });

        for r in table.reclaim_worker_leases(8888) {
            let _ = table.dispose_quarantined(r.slot_index, r.generation);
        }
        assert_eq!(table.stats().active_leases, 0);
        assert_eq!(table.stats().quarantined, 0);
        let pstats = table.partition_stats(key).unwrap();
        assert_eq!(pstats.active_leases, 0);
        assert_eq!(pstats.quarantined, 0);
    }

    fn unique_test_shm_name(prefix: &str) -> String {
        static TEST_SHM_SEQ: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let seq = TEST_SHM_SEQ.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{}-{seq}", std::process::id())
    }

    #[test]
    fn test_shm_cache_lifecycle() {
        let name = unique_test_shm_name("test-shm-cache");
        let (cache, created) = ShmCache::open_or_create_named(&name, 1024 * 32, 32).unwrap();
        assert!(created);
        assert!(cache.set(b"key1", b"val1", None));
        assert_eq!(cache.get(b"key1").unwrap(), b"val1");
        assert!(cache.has_key(b"key1"));
        assert!(!cache.has_key(b"key2"));

        // incr / decr
        assert_eq!(cache.incr(b"counter", 5).unwrap(), 5);
        assert_eq!(cache.incr(b"counter", 2).unwrap(), 7);
        assert_eq!(cache.decr(b"counter", 3).unwrap(), 4);

        // prefix invalidation
        cache.set(b"pref:a", b"1", None);
        cache.set(b"pref:b", b"2", None);
        assert_eq!(cache.invalidate_prefix(b"pref:"), 2);
        assert!(!cache.has_key(b"pref:a"));

        // stats
        let s = cache.stats();
        assert!(s.hits >= 1);
        assert!(s.sets >= 5);

        cache.clear();
        assert!(!cache.has_key(b"key1"));
        cache.unlink();
    }

    #[test]
    fn test_shm_cache_attach_validates_capacity_and_corrupted_slot_lengths() {
        let req_size = ShmCache::required_size(16);
        let region = Arc::new(ShmRegion::create(req_size).unwrap());
        let cache = ShmCache::from_region_at_offset(Arc::clone(&region), 0, 16, true).unwrap();
        assert_eq!(cache.capacity(), 16);
        assert!(cache.set(b"alpha", b"1", None));

        // Corrupting `hdr.capacity` to 0, non-power-of-two, or oversized must reject attach
        // while existing `cache` keeps its validated local capacity.
        // SAFETY: `region` holds a valid `ShmCacheHeader` at offset 0 and no concurrent mutators exist in this test.
        unsafe {
            let hdr = &mut *(region.as_ptr() as *mut ShmCacheHeader);
            hdr.capacity = 0;
        }
        assert_eq!(
            ShmCache::from_region_at_offset(Arc::clone(&region), 0, 16, false).err(),
            Some(BrokerError::InvalidCapacity)
        );
        // SAFETY: `region` holds a valid `ShmCacheHeader` at offset 0 and no concurrent mutators exist in this test.
        unsafe {
            let hdr = &mut *(region.as_ptr() as *mut ShmCacheHeader);
            hdr.capacity = 24;
        }
        assert_eq!(
            ShmCache::from_region_at_offset(Arc::clone(&region), 0, 16, false).err(),
            Some(BrokerError::InvalidCapacity)
        );
        // SAFETY: `region` holds a valid `ShmCacheHeader` at offset 0 and no concurrent mutators exist in this test.
        unsafe {
            let hdr = &mut *(region.as_ptr() as *mut ShmCacheHeader);
            hdr.capacity = 64;
        }
        assert!(matches!(
            ShmCache::from_region_at_offset(Arc::clone(&region), 0, 16, false).err(),
            Some(BrokerError::RegionTooSmall { .. })
        ));
        // Existing attached instance does not re-read corrupted `hdr.capacity`.
        assert_eq!(cache.capacity(), 16);
        assert_eq!(cache.get(b"alpha").as_deref(), Some(&b"1"[..]));

        // Corrupt `key_len` and `val_len` on slot 0 beyond stack buffer bounds and verify no OOB.
        let slot0 = cache.slot(0);
        slot0.seq.store(2, Ordering::Relaxed);
        slot0.key_len.store(u16::MAX, Ordering::Relaxed);
        slot0.val_len.store(u32::MAX, Ordering::Relaxed);
        slot0
            .key_hash
            .store(ShmCache::compute_key_hash(b"probe"), Ordering::Relaxed);
        slot0.seq.store(4, Ordering::Release);

        assert_eq!(cache.get(b"probe"), None);
        assert_eq!(cache.invalidate_prefix(b""), 1);
        assert!(!cache.delete(b"probe"));
        assert!(!cache.touch(b"probe", None));
    }

    #[test]
    fn test_shm_cache_tombstone_monotonicity_and_expired_collision_probing() {
        let req_size = ShmCache::required_size(16);
        let region = ShmRegion::create(req_size).unwrap();
        let cache = ShmCache::init_in_region(region, 16).unwrap();

        // Find three distinct keys hashing to the same bucket index (`hash & 15`).
        let target_bucket = (ShmCache::compute_key_hash(b"k0") as usize) & 15;
        let mut colliding = vec![b"k0".to_vec()];
        let mut candidate = 1u64;
        while colliding.len() < 3 {
            let k = format!("cand-{candidate}").into_bytes();
            if ((ShmCache::compute_key_hash(&k) as usize) & 15) == target_bucket {
                colliding.push(k);
            }
            candidate += 1;
        }
        let (k0, k1, k2) = (&colliding[0], &colliding[1], &colliding[2]);

        assert!(cache.set(k0, b"v0", Some(40_000_000))); // 40ms TTL -> expires after k1/k2 land behind it
        assert!(cache.set(k1, b"v1", None));
        assert!(cache.set(k2, b"v2", None));
        cache.set_slot_expiry_for_test(target_bucket, 1);

        // Expired `k0` at the head of the chain must NOT abort linear probing for `k1` or `k2`.
        assert_eq!(cache.get(k0), None);
        assert_eq!(cache.get(k1).as_deref(), Some(&b"v1"[..]));
        assert_eq!(cache.get(k2).as_deref(), Some(&b"v2"[..]));

        // Deleting `k1` creates a tombstone (`seq != 0`, `key_len == 0`) without breaking `k2`.
        assert!(cache.delete(k1));
        let slot1 = cache.slot((target_bucket + 1) & 15);
        assert_ne!(slot1.seq.load(Ordering::Acquire), 0);
        assert_eq!(slot1.key_len.load(Ordering::Acquire), 0);
        assert_eq!(cache.get(k2).as_deref(), Some(&b"v2"[..]));

        // Updating `k2` must update `k2` in place despite earlier expired/tombstone slots.
        assert!(cache.set(k2, b"v2-updated", None));
        assert_eq!(cache.get(k2).as_deref(), Some(&b"v2-updated"[..]));
        assert!(cache.delete(k2));
        assert_eq!(cache.get(k2), None);

        // Wrap-around from `u64::MAX - 1` must publish `2`, never `0`.
        slot1.seq.store(u64::MAX - 1, Ordering::Release);
        cache.clear();
        assert_eq!(slot1.seq.load(Ordering::Acquire), 2);
    }

    #[test]
    fn test_shm_cache_concurrent_atomic_add_and_incr_preserves_ttl() {
        let req_size = ShmCache::required_size(64);
        let region = ShmRegion::create(req_size).unwrap();
        let cache = Arc::new(ShmCache::init_in_region(region, 64).unwrap());

        let barrier = Arc::new(std::sync::Barrier::new(8));
        let wins: usize = std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(8);
            for i in 0..8usize {
                let c = Arc::clone(&cache);
                let b = Arc::clone(&barrier);
                handles.push(s.spawn(move || {
                    b.wait();
                    usize::from(c.add(b"once-key", format!("val-{i}").as_bytes(), None))
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).sum()
        });
        assert_eq!(wins, 1);

        assert!(cache.set(b"ttl-counter", b"10", Some(60_000_000_000)));
        let bucket = (ShmCache::compute_key_hash(b"ttl-counter") as usize) & 63;
        let orig_expiry = cache.slot(bucket).expiry_ns.load(Ordering::Acquire);
        assert!(orig_expiry > 0);

        let barrier2 = Arc::new(std::sync::Barrier::new(8));
        std::thread::scope(|s| {
            for _ in 0..8 {
                let c = Arc::clone(&cache);
                let b = Arc::clone(&barrier2);
                s.spawn(move || {
                    b.wait();
                    for _ in 0..50 {
                        c.incr(b"ttl-counter", 1).unwrap();
                    }
                });
            }
        });

        assert_eq!(cache.get(b"ttl-counter").as_deref(), Some(&b"410"[..]));
        assert_eq!(
            cache.slot(bucket).expiry_ns.load(Ordering::Acquire),
            orig_expiry
        );
    }

    #[test]
    fn test_statement_cache_recovers_from_stuck_odd_seqlock() {
        let req_size = ShmStatementCache::required_size(16);
        let region = ShmRegion::create(req_size).unwrap();
        let cache = ShmStatementCache::init_in_region(region, 16).unwrap();

        let raw_sql = b"SELECT * FROM stuck_seqlock WHERE id = $1";
        let compiled_sql = b"SELECT * FROM stuck_seqlock WHERE id = :p0";
        let param_plan = b"\x91\xa2p0";
        let hash = ShmStatementCache::compute_sql_hash(1, raw_sql);
        let set_idx = (hash as usize) & (cache.header().set_mask as usize);
        let base = set_idx * 4;

        for way in 0..4 {
            cache.slot(base + way).seq.store(1, Ordering::Release);
        }

        assert!(cache.insert(1, raw_sql, 2, compiled_sql, param_plan));
        let entry = cache
            .lookup(1, raw_sql)
            .expect("recovered statement cache slot must be readable");
        assert_eq!(entry.dialect_tag, 1);
        assert_eq!(entry.param_count, 2);
        assert_eq!(&entry.compiled_sql[..], compiled_sql);
        assert_eq!(&entry.param_plan[..], param_plan);
        assert_eq!(cache.slot(base).seq.load(Ordering::Acquire), 4);
        assert_eq!(cache.stats().inserts, 1);
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn test_broker_section_offset_alignment_and_bounds_rejection() {
        let req_size = ShmStatementCache::required_size(16);
        let padded_region = Arc::new(ShmRegion::create(req_size + 64).unwrap());
        assert_eq!(
            ShmStatementCache::from_region_at_offset(Arc::clone(&padded_region), 1, 16, false)
                .err(),
            Some(BrokerError::MisalignedRegion {
                offset: 1,
                align: std::mem::align_of::<ShmStatementHeader>(),
            })
        );

        let exact_region = Arc::new(ShmRegion::create(req_size).unwrap());
        let actual_len = exact_region.len();
        let oversized_offset = actual_len & !63;
        assert_eq!(
            ShmStatementCache::from_region_at_offset(Arc::clone(&exact_region), 1, 16, false).err(),
            Some(BrokerError::MisalignedRegion {
                offset: 1,
                align: std::mem::align_of::<ShmStatementHeader>(),
            })
        );
        assert_eq!(
            ShmStatementCache::from_region_at_offset(
                Arc::clone(&exact_region),
                oversized_offset,
                16,
                false,
            )
            .err(),
            Some(BrokerError::RegionTooSmall {
                expected: oversized_offset + req_size,
                actual: actual_len,
            })
        );
        assert_eq!(
            ShmStatementCache::from_region_at_offset(
                Arc::clone(&exact_region),
                usize::MAX - 63,
                16,
                false,
            )
            .err(),
            Some(BrokerError::RegionTooSmall {
                expected: usize::MAX,
                actual: actual_len,
            })
        );

        let lease_req = ShmLeaseTable::required_size(2, 0);
        let mut backing = vec![0u8; lease_req.max(req_size) + 128];
        let raw_ptr = backing.as_mut_ptr();
        let align_pad = (64 - (raw_ptr as usize % 64)) % 64;
        // SAFETY: `backing` is at least `lease_req.max(req_size) + 128` bytes, so `align_pad + 1`
        // leaves at least `lease_req.max(req_size)` valid bytes.
        let unaligned_ptr = unsafe { raw_ptr.add(align_pad + 1) };
        // SAFETY: `unaligned_ptr` points into `backing` and spans `req_size` valid bytes.
        let unaligned_stmt_region =
            unsafe { Arc::new(ShmRegion::from_raw(unaligned_ptr, req_size)) };
        assert_eq!(
            ShmStatementCache::from_region_at_offset(unaligned_stmt_region, 0, 16, false).err(),
            Some(BrokerError::MisalignedRegion {
                offset: 0,
                align: std::mem::align_of::<ShmStatementHeader>(),
            })
        );
        // SAFETY: `unaligned_ptr` points into `backing` and spans `lease_req` valid bytes.
        let unaligned_lease_region = unsafe { ShmRegion::from_raw(unaligned_ptr, lease_req) };
        assert_eq!(
            ShmLeaseTable::attach_region(unaligned_lease_region).err(),
            Some(BrokerError::MisalignedRegion {
                offset: 0,
                align: std::mem::align_of::<ShmLeaseHeader>(),
            })
        );

        let table_region = ShmRegion::create(lease_req).unwrap();
        let table = ShmLeaseTable::init_in_region(table_region, 2, 0).unwrap();
        let orig_channel_offset = table.header().channel_offset;
        assert!(table.channel_header_and_slots().is_some());

        // SAFETY: `table.header` is a valid initialized `ShmLeaseHeader` with exclusive access in this test.
        unsafe {
            (&mut *table.header).channel_offset = orig_channel_offset + 1;
        }
        assert!(table.channel_header_and_slots().is_none());

        // SAFETY: `table.header` is a valid initialized `ShmLeaseHeader` with exclusive access in this test.
        unsafe {
            (&mut *table.header).channel_offset = (usize::MAX - 63) as u64;
        }
        assert!(table.channel_header_and_slots().is_none());

        // SAFETY: `table.header` is a valid initialized `ShmLeaseHeader` with exclusive access in this test.
        unsafe {
            (&mut *table.header).channel_offset = table.region.len() as u64;
        }
        assert!(table.channel_header_and_slots().is_none());

        // SAFETY: `table.header` is a valid initialized `ShmLeaseHeader` with exclusive access in this test.
        unsafe {
            (&mut *table.header).channel_offset = orig_channel_offset;
        }
        assert!(table.channel_header_and_slots().is_some());
    }

    #[test]
    fn test_wait_for_wake_immediate_timeout_and_release_modes() {
        let req = ShmLeaseTable::required_size(2, 0);
        let region = ShmRegion::create(req).unwrap();
        let table = Arc::new(ShmLeaseTable::init_in_region(region, 2, 0).unwrap());
        assert_eq!(table.partition_count(), 0);

        let initial_seq = table.wake_seq();
        assert_eq!(
            table.wait_for_wake(initial_seq, Duration::ZERO),
            initial_seq
        );
        assert_eq!(table.stats().waiting_threads, 0);

        let grant = table.try_acquire(1, 0, 0).unwrap();
        table.release(grant.slot_index, grant.generation).unwrap();
        let bumped_seq = table.wake_seq();
        assert_ne!(bumped_seq, initial_seq);
        assert_eq!(
            table.wait_for_wake(initial_seq, Duration::from_secs(1)),
            bumped_seq
        );
        assert_eq!(table.stats().waiting_threads, 0);

        let timed_out_seq = table.wait_for_wake(bumped_seq, Duration::from_millis(5));
        assert_eq!(timed_out_seq, bumped_seq);
        assert_eq!(table.stats().waiting_threads, 0);

        let held = table.try_acquire(1, 0, 0).unwrap();
        let wait_seq = table.wake_seq();
        let waiter_table = Arc::clone(&table);
        let waiter = std::thread::spawn(move || {
            waiter_table.wait_for_wake(wait_seq, Duration::from_secs(2))
        });
        while table.stats().waiting_threads == 0 {
            std::thread::yield_now();
        }
        table.release(held.slot_index, held.generation).unwrap();
        let woken_seq = waiter.join().unwrap();
        assert_ne!(woken_seq, wait_seq);
        assert_eq!(table.stats().waiting_threads, 0);

        let p0 = table
            .register_partition(PoolPartitionSpec {
                pool_key_hash: 101,
                pool_size: 1,
                max_overflow: 0,
            })
            .unwrap();
        assert_eq!(table.partition_count(), 1);
        let held_p0 = table.try_acquire_in(p0, 1, 0, 101).unwrap().unwrap();
        let p0_seq = table.wake_seq();
        let p0_table = Arc::clone(&table);
        let p0_waiter =
            std::thread::spawn(move || p0_table.wait_for_wake(p0_seq, Duration::from_secs(2)));
        while table.stats().waiting_threads == 0 {
            std::thread::yield_now();
        }
        table
            .release(held_p0.slot_index, held_p0.generation)
            .unwrap();
        assert_ne!(p0_waiter.join().unwrap(), p0_seq);
        assert_eq!(table.stats().waiting_threads, 0);

        let p1 = table
            .register_partition(PoolPartitionSpec {
                pool_key_hash: 202,
                pool_size: 1,
                max_overflow: 0,
            })
            .unwrap();
        assert_eq!(table.partition_count(), 2);
        let held_p1 = table.try_acquire_in(p1, 1, 0, 202).unwrap().unwrap();
        let multi_seq = table.wake_seq();
        let w1_table = Arc::clone(&table);
        let w2_table = Arc::clone(&table);
        let w1 =
            std::thread::spawn(move || w1_table.wait_for_wake(multi_seq, Duration::from_secs(2)));
        let w2 =
            std::thread::spawn(move || w2_table.wait_for_wake(multi_seq, Duration::from_secs(2)));
        while table.stats().waiting_threads < 2 {
            std::thread::yield_now();
        }
        table
            .release(held_p1.slot_index, held_p1.generation)
            .unwrap();
        assert_ne!(w1.join().unwrap(), multi_seq);
        assert_ne!(w2.join().unwrap(), multi_seq);
        assert_eq!(table.stats().waiting_threads, 0);
    }
}
