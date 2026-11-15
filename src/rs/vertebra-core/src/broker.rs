//! Shared-memory atomic lease broker and prepared statement cache.
//!
//! Provides cross-process database connection admission control, crash lease
//! reclamation, and optimistic seqlock-guarded statement plan caching.

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::layout::CacheLine;
use crate::platform::WaitWord;
use crate::shm::{NamedAttach, ShmError, ShmRegion};

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

fn libc_errno(err: &std::io::Error) -> i32 {
    err.raw_os_error().unwrap_or(5)
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

#[repr(C, align(64))]
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

const _: () = assert!(std::mem::size_of::<ShmLeaseHeader>() == 4544);
const _: () = assert!(std::mem::align_of::<ShmLeaseHeader>() == 64);

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
        if region.len() < req_size {
            return Err(BrokerError::RegionTooSmall {
                expected: req_size,
                actual: region.len(),
            });
        }

        let region_len = region.len();

        // SAFETY: region.as_mut_slice() is valid for req_size bytes. Zero out entire broker region.
        unsafe {
            let slice = region.as_mut_slice();
            slice[..req_size].fill(0);
        }

        let header_ptr = unsafe { region.as_mut_slice() }.as_mut_ptr() as *mut ShmLeaseHeader;

        let lease_bytes = Self::lease_section_size(pool_size, max_overflow);
        let (eff_stmt_cap, stmt_offset, stmt_bytes) = if eff_stmt_cap_checked > 0 {
            let cap = eff_stmt_cap_checked;
            let offset = lease_bytes as u64;
            let bytes = ShmStatementCache::required_size(cap);
            let stmt_hdr_ptr =
                unsafe { (header_ptr as *mut u8).add(lease_bytes) as *mut ShmStatementHeader };
            unsafe {
                let shdr = &mut *stmt_hdr_ptr;
                shdr.capacity = cap;
                shdr.set_mask = (cap / 4) - 1;
            }
            (cap, offset, bytes)
        } else {
            (0, 0, 0)
        };

        let channel_offset = (lease_bytes + stmt_bytes) as u64;
        let chan_hdr_ptr = unsafe {
            (header_ptr as *mut u8).add(channel_offset as usize) as *mut ShmChannelHeader
        };
        unsafe {
            let chdr = &mut *chan_hdr_ptr;
            chdr.capacity = DEFAULT_CHANNEL_CAPACITY;
            chdr.write_seq.store(0, Ordering::Release);
            chdr.total_published.store(0, Ordering::Release);
            chdr.total_drained.store(0, Ordering::Release);
            chdr.total_dropped.store(0, Ordering::Release);
        }

        // SAFETY: header_ptr is valid and aligned. Initialize header fields.
        unsafe {
            let hdr = &mut *header_ptr;
            hdr.magic = BROKER_MAGIC;
            hdr.abi_version = BROKER_ABI_VERSION;
            hdr.pool_size = pool_size;
            hdr.max_overflow = max_overflow;
            hdr.stmt_capacity = eff_stmt_cap;
            hdr.stmt_offset = stmt_offset;
            hdr.total_size = region_len as u64;
            hdr.channel_capacity = DEFAULT_CHANNEL_CAPACITY;
            hdr.channel_offset = channel_offset;
            hdr.active_leases.0.store(0, Ordering::Release);
            hdr.waiters.0.store(0, Ordering::Release);
            hdr.wake_seq.0.store(0);
            hdr.rr_cursor.0.store(0, Ordering::Release);
            hdr.partition_count.0.store(0, Ordering::Release);
        }

        Ok(())
    }

    pub fn attach_region(mut region: ShmRegion) -> Result<Self, BrokerError> {
        if region.len() < std::mem::size_of::<ShmLeaseHeader>() {
            return Err(BrokerError::RegionTooSmall {
                expected: std::mem::size_of::<ShmLeaseHeader>(),
                actual: region.len(),
            });
        }

        let header_ptr = unsafe { region.as_mut_slice() }.as_mut_ptr() as *mut ShmLeaseHeader;
        let live_owner = region
            .owner_alive()
            .then(|| region.owner_pid().unwrap_or(0));
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
        if region.len() < req_size {
            if let Some(owner_pid) = live_owner {
                return Err(BrokerError::SegmentInUse { owner_pid });
            }
            return Err(BrokerError::RegionTooSmall {
                expected: req_size,
                actual: region.len(),
            });
        }

        let slots_ptr = unsafe {
            (header_ptr as *mut u8).add(std::mem::size_of::<ShmLeaseHeader>()) as *mut LeaseSlot
        };

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
        let total_size = unsafe {
            let hdr = &*(probe.as_ptr() as *const ShmLeaseHeader);
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
                    let claim_state_pid = pack_state_pid(SLOT_STATE_CLAIMING, worker_pid);
                    if slot
                        .state_and_pid
                        .compare_exchange(
                            state_pid,
                            claim_state_pid,
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
                            .store(pack_state_pid(target_state, worker_pid), Ordering::Release);

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
                } else if state == SLOT_STATE_CLAIMING {
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

        struct WaitGuard<'a>(&'a AtomicU32);
        impl Drop for WaitGuard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

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
            if state == SLOT_STATE_CLAIMING {
                std::hint::spin_loop();
                continue;
            }
            if state != SLOT_STATE_PRIMARY && state != SLOT_STATE_OVERFLOW {
                return Err(BrokerError::StaleGeneration);
            }
            if slot
                .state_and_pid
                .compare_exchange(
                    raw,
                    pack_state_pid(SLOT_STATE_CLAIMING, pid),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }
            if slot.generation.load(Ordering::Acquire) != generation {
                slot.state_and_pid.store(raw, Ordering::Release);
                return Err(BrokerError::StaleGeneration);
            }
            let now = monotonic_now_ns();
            slot.last_heartbeat_ns.store(now, Ordering::Release);
            let ttl = slot.ttl_ns.load(Ordering::Relaxed);
            if ttl > 0 {
                slot.expiry_ns
                    .store(now.saturating_add(ttl), Ordering::Relaxed);
            }
            slot.state_and_pid.store(raw, Ordering::Release);
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
        if slot
            .state_and_pid
            .compare_exchange(
                raw,
                pack_state_pid(SLOT_STATE_CLAIMING, pid),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Ok(false);
        }

        if let Some(exp_gen) = expected_gen
            && slot.generation.load(Ordering::Acquire) != exp_gen
        {
            slot.state_and_pid.store(raw, Ordering::Release);
            return Err(BrokerError::GenerationMismatch);
        }

        let part_id = slot.partition_id.load(Ordering::Acquire);
        if bump_generation {
            slot.generation.fetch_add(1, Ordering::AcqRel);
        }
        slot.partition_id.store(NO_PARTITION, Ordering::Relaxed);
        slot.state_and_pid.store(0, Ordering::Release);

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
            if state == SLOT_STATE_CLAIMING {
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
                    self.wake_waiters_seqcst();
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
            if state == SLOT_STATE_CLAIMING {
                std::hint::spin_loop();
                continue;
            }
            if state != SLOT_STATE_PRIMARY && state != SLOT_STATE_OVERFLOW {
                return Err(BrokerError::InvalidState);
            }
            if slot.generation.load(Ordering::Acquire) != generation {
                return Err(BrokerError::GenerationMismatch);
            }
            if slot
                .state_and_pid
                .compare_exchange(
                    raw,
                    pack_state_pid(SLOT_STATE_CLAIMING, pid),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                if slot.generation.load(Ordering::Acquire) != generation {
                    return Err(BrokerError::GenerationMismatch);
                }
                let (cur_state, _) = unpack_state_pid(slot.state_and_pid.load(Ordering::Acquire));
                if cur_state == SLOT_STATE_CLAIMING {
                    std::hint::spin_loop();
                    continue;
                }
                return Err(BrokerError::InvalidState);
            }
            if slot.generation.load(Ordering::Acquire) != generation {
                slot.state_and_pid.store(raw, Ordering::Release);
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
            slot.state_and_pid.store(
                pack_state_pid(SLOT_STATE_QUARANTINED, pid),
                Ordering::Release,
            );
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
            if state == SLOT_STATE_CLAIMING {
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
                    self.wake_waiters_seqcst();
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

        for idx in 0..total {
            let slot = self.slot(idx);
            for spin in 0..1024 {
                let raw = slot.state_and_pid.load(Ordering::Acquire);
                let (state, pid) = unpack_state_pid(raw);
                if pid != worker_pid {
                    break;
                }
                if state == SLOT_STATE_CLAIMING {
                    if spin < 1023 {
                        std::hint::spin_loop();
                        continue;
                    }
                    let cur_gen = slot.generation.load(Ordering::Acquire);
                    if slot
                        .generation
                        .compare_exchange(cur_gen, cur_gen + 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        break;
                    }
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
                    slot.state_and_pid.store(
                        pack_state_pid(SLOT_STATE_QUARANTINED, worker_pid),
                        Ordering::Release,
                    );
                    reclaimed.push(ReclaimedLease {
                        slot_index: idx,
                        generation: cur_gen + 1,
                        pool_id,
                    });
                    break;
                }
                if state != SLOT_STATE_PRIMARY && state != SLOT_STATE_OVERFLOW {
                    break;
                }
                if slot
                    .state_and_pid
                    .compare_exchange(
                        raw,
                        pack_state_pid(SLOT_STATE_CLAIMING, worker_pid),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
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
                    slot.state_and_pid.store(
                        pack_state_pid(SLOT_STATE_QUARANTINED, worker_pid),
                        Ordering::Release,
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

        for idx in 0..total {
            let slot = self.slot(idx);
            for spin in 0..1024 {
                let raw = slot.state_and_pid.load(Ordering::Acquire);
                let (state, pid) = unpack_state_pid(raw);
                if pid != worker_pid {
                    break;
                }
                if state == SLOT_STATE_CLAIMING {
                    if spin < 1023 {
                        std::hint::spin_loop();
                        continue;
                    }
                    let cur_gen = slot.generation.load(Ordering::Acquire);
                    if slot
                        .generation
                        .compare_exchange(cur_gen, cur_gen + 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        let part_id = slot.partition_id.load(Ordering::Acquire);
                        slot.partition_id.store(NO_PARTITION, Ordering::Relaxed);
                        slot.state_and_pid.store(0, Ordering::Release);
                        hdr.active_leases.0.fetch_sub(1, Ordering::SeqCst);
                        if part_id < MAX_POOL_PARTITIONS as u32 {
                            let entry = &hdr.partitions[part_id as usize];
                            entry.active_leases.fetch_sub(1, Ordering::SeqCst);
                            entry.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                        }
                        hdr.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                        reclaimed += 1;
                    }
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
        let offset = hdr.channel_offset as usize;
        if cap == 0 || offset == 0 {
            return None;
        }
        let req_end = offset + Self::channel_section_size(cap);
        if self.region.len() < req_end {
            return None;
        }
        unsafe {
            let ch_hdr_ptr = self.region.as_ptr().add(offset) as *const ShmChannelHeader;
            let slots_ptr = (ch_hdr_ptr as *const u8).add(std::mem::size_of::<ShmChannelHeader>())
                as *const ShmChannelSlot;
            Some((&*ch_hdr_ptr, slots_ptr, cap))
        }
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
                let slot = unsafe { &*slots_ptr.add(idx) };
                if slot.state.load(Ordering::Acquire) == CHAN_SLOT_READY {
                    let s = slot.seq.load(Ordering::Relaxed);
                    if s < oldest_seq {
                        oldest_seq = s;
                        oldest_idx = idx;
                    }
                }
            }
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

        let slot = unsafe { &*slots_ptr.add(idx) };
        let ts = if timestamp_ns > 0 {
            timestamp_ns
        } else {
            monotonic_now_ns()
        };

        let mut ch_buf = [0u8; CHAN_MAX_NAME_BYTES];
        ch_buf[..channel.len()].copy_from_slice(channel);
        let ch_words = unsafe {
            std::slice::from_raw_parts(ch_buf.as_ptr() as *const u64, channel.len().div_ceil(8))
        };
        for (i, &w) in ch_words.iter().enumerate() {
            slot.channel_words[i].store(w, Ordering::Relaxed);
        }

        let lid_len = if let Some(lid) = lease_id {
            let mut lid_buf = [0u8; CHAN_MAX_LEASE_ID_BYTES];
            lid_buf[..lid.len()].copy_from_slice(lid);
            let lid_words = unsafe {
                std::slice::from_raw_parts(lid_buf.as_ptr() as *const u64, lid.len().div_ceil(8))
            };
            for (i, &w) in lid_words.iter().enumerate() {
                slot.lease_id_words[i].store(w, Ordering::Relaxed);
            }
            lid.len() as u16
        } else {
            0
        };

        if !payload.is_empty() {
            let mut pay_buf = [0u8; CHAN_MAX_PAYLOAD_BYTES];
            pay_buf[..payload.len()].copy_from_slice(payload);
            let pay_words = unsafe {
                std::slice::from_raw_parts(
                    pay_buf.as_ptr() as *const u64,
                    payload.len().div_ceil(8),
                )
            };
            for (i, &w) in pay_words.iter().enumerate() {
                slot.payload_words[i].store(w, Ordering::Relaxed);
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
            let channel_vec = unsafe {
                std::slice::from_raw_parts(ch_words.as_ptr() as *const u8, ch_len).to_vec()
            };

            let pay_len =
                (slot.payload_len.load(Ordering::Relaxed) as usize).min(CHAN_MAX_PAYLOAD_BYTES);
            let mut pay_words = [0u64; 32];
            for (i, word) in pay_words.iter_mut().enumerate().take(pay_len.div_ceil(8)) {
                *word = slot.payload_words[i].load(Ordering::Relaxed);
            }
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
        let expected_end = offset
            .checked_add(req_size)
            .ok_or(BrokerError::InvalidCapacity)?;
        if region.len() < expected_end {
            return Err(BrokerError::RegionTooSmall {
                expected: expected_end,
                actual: region.len(),
            });
        }

        let header_ptr = unsafe { region.as_ptr().add(offset) as *mut ShmStatementHeader };
        let slots_ptr = unsafe {
            (header_ptr as *mut u8).add(std::mem::size_of::<ShmStatementHeader>())
                as *mut StatementSlot
        };

        if create {
            unsafe {
                std::ptr::write_bytes(region.as_ptr().add(offset), 0, req_size);
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
        unsafe { &*self.header }
    }

    #[inline]
    fn slot(&self, index: usize) -> &StatementSlot {
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

        // Claim slot for writing via seqlock odd increment
        let mut even_seq = slot.seq.load(Ordering::Relaxed);
        if (even_seq & 1) != 0 {
            even_seq = even_seq.wrapping_add(1);
        }

        loop {
            match slot.seq.compare_exchange_weak(
                even_seq,
                even_seq.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => {
                    even_seq = if (actual & 1) != 0 {
                        actual.wrapping_add(1)
                    } else {
                        actual
                    };
                }
            }
        }

        // Prepare payload buffer
        let mut payload_buf = [0u8; STMT_SLOT_PAYLOAD_BYTES];
        payload_buf[..compiled_sql.len()].copy_from_slice(compiled_sql);
        payload_buf[compiled_sql.len()..compiled_sql.len() + param_plan.len()]
            .copy_from_slice(param_plan);

        let num_words = total_bytes.div_ceil(8);
        let word_slice = unsafe {
            std::slice::from_raw_parts(payload_buf.as_ptr() as *const u64, num_words.min(28))
        };

        // Write fields
        slot.dialect_and_clock
            .store((dialect_tag as u32) | CLOCK_REF_BIT, Ordering::Relaxed);
        slot.param_count.store(param_count, Ordering::Relaxed);
        slot.sql_len
            .store(compiled_sql.len() as u16, Ordering::Relaxed);
        slot.plan_len
            .store(param_plan.len() as u16, Ordering::Relaxed);

        for (i, &word) in word_slice.iter().enumerate() {
            slot.payload[i].store(word, Ordering::Relaxed);
        }

        slot.sql_hash.store(hash, Ordering::Relaxed);

        // Commit seqlock: even_seq + 2
        slot.seq.store(even_seq.wrapping_add(2), Ordering::Release);

        hdr.inserts.fetch_add(1, Ordering::Relaxed);
        if even_seq > 0 {
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
}
