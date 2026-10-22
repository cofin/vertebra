//! Shared-memory atomic lease broker and prepared statement cache.
//!
//! Provides cross-process database connection admission control, crash lease
//! reclamation, and optimistic seqlock-guarded statement plan caching.

use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::layout::CacheLine;
use crate::platform::WaitWord;
use crate::shm::ShmRegion;

pub const BROKER_MAGIC: u64 = u64::from_le_bytes(*b"VTBBRKR1");
pub const BROKER_ABI_VERSION: u32 = 1;
pub const MAX_BROKER_SLOTS: u32 = 4096;

pub const SLOT_STATE_FREE: u32 = 0;
pub const SLOT_STATE_PRIMARY: u32 = 1;
pub const SLOT_STATE_OVERFLOW: u32 = 2;

pub const STMT_SLOT_PAYLOAD_BYTES: usize = 224;
pub const CLOCK_REF_BIT: u32 = 1 << 8;

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
    pub _pad0: [u8; 24],

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
    pub _pad1: [u8; 24],
}

#[repr(C, align(64))]
pub struct LeaseSlot {
    pub state_and_pid: AtomicU64,
    pub generation: AtomicU64,
    pub acquired_ns: AtomicU64,
    pub expiry_ns: AtomicU64,
    pub bind_key_hash: AtomicU64,
    pub _pad: [u8; 24],
}

const _: () = assert!(std::mem::size_of::<LeaseSlot>() == 64);
const _: () = assert!(std::mem::align_of::<LeaseSlot>() == 64);

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
}

pub struct ShmLeaseTable {
    region: ShmRegion,
    header: *mut ShmLeaseHeader,
    slots: *mut LeaseSlot,
}

// SAFETY: All fields accessed via ShmLeaseHeader and LeaseSlot are atomic or immutable.
unsafe impl Send for ShmLeaseTable {}
unsafe impl Sync for ShmLeaseTable {}

impl ShmLeaseTable {
    pub const fn required_size(pool_size: u32, max_overflow: u32) -> usize {
        let raw = std::mem::size_of::<ShmLeaseHeader>()
            + ((pool_size + max_overflow) as usize) * std::mem::size_of::<LeaseSlot>();
        (raw + 63) & !63
    }

    pub fn init_in_region(
        mut region: ShmRegion,
        pool_size: u32,
        max_overflow: u32,
    ) -> Result<Self, BrokerError> {
        if pool_size == 0 || pool_size + max_overflow > MAX_BROKER_SLOTS {
            return Err(BrokerError::InvalidCapacity);
        }
        let req_size = Self::required_size(pool_size, max_overflow);
        if region.len() < req_size {
            return Err(BrokerError::RegionTooSmall {
                expected: req_size,
                actual: region.len(),
            });
        }

        // SAFETY: region.as_mut_slice() is valid for req_size bytes. Zero out header and slots.
        unsafe {
            let slice = region.as_mut_slice();
            slice[..req_size].fill(0);
        }

        let header_ptr = unsafe { region.as_mut_slice() }.as_mut_ptr() as *mut ShmLeaseHeader;
        let slots_ptr = unsafe {
            (header_ptr as *mut u8).add(std::mem::size_of::<ShmLeaseHeader>()) as *mut LeaseSlot
        };

        // SAFETY: header_ptr is valid and aligned. Initialize header fields.
        unsafe {
            let hdr = &mut *header_ptr;
            hdr.magic = BROKER_MAGIC;
            hdr.abi_version = BROKER_ABI_VERSION;
            hdr.pool_size = pool_size;
            hdr.max_overflow = max_overflow;
            hdr.stmt_capacity = 0;
            hdr.stmt_offset = 0;
            hdr.total_size = region.len() as u64;
            hdr.active_leases.0.store(0, Ordering::Release);
            hdr.waiters.0.store(0, Ordering::Release);
            hdr.wake_seq.0.store(0);
            hdr.rr_cursor.0.store(0, Ordering::Release);
        }

        Ok(Self {
            region,
            header: header_ptr,
            slots: slots_ptr,
        })
    }

    pub fn attach_region(mut region: ShmRegion) -> Result<Self, BrokerError> {
        if region.len() < std::mem::size_of::<ShmLeaseHeader>() {
            return Err(BrokerError::RegionTooSmall {
                expected: std::mem::size_of::<ShmLeaseHeader>(),
                actual: region.len(),
            });
        }

        let header_ptr = unsafe { region.as_mut_slice() }.as_mut_ptr() as *mut ShmLeaseHeader;
        let (pool_size, max_overflow) = unsafe {
            let hdr = &*header_ptr;
            if hdr.magic != BROKER_MAGIC {
                return Err(BrokerError::InvalidMagic(hdr.magic));
            }
            if hdr.abi_version != BROKER_ABI_VERSION {
                return Err(BrokerError::UnsupportedAbi(hdr.abi_version));
            }
            (hdr.pool_size, hdr.max_overflow)
        };

        let req_size = Self::required_size(pool_size, max_overflow);
        if region.len() < req_size {
            return Err(BrokerError::RegionTooSmall {
                expected: req_size,
                actual: region.len(),
            });
        }

        let slots_ptr = unsafe {
            (header_ptr as *mut u8).add(std::mem::size_of::<ShmLeaseHeader>()) as *mut LeaseSlot
        };

        Ok(Self {
            region,
            header: header_ptr,
            slots: slots_ptr,
        })
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

    pub fn try_acquire(
        &self,
        worker_pid: u32,
        ttl_ns: u64,
        bind_key_hash: u64,
    ) -> Result<LeaseGrant, BrokerError> {
        let total = self.total_capacity();
        let hdr = self.header();

        // 1. Opportunistic TTL reap if at capacity
        if hdr.active_leases.0.load(Ordering::Acquire) >= total {
            self.reap_expired(monotonic_now_ns());
        }

        // 2. Optimistic CAS loop to reserve an active lease count
        let mut cur = hdr.active_leases.0.load(Ordering::Acquire);
        loop {
            if cur >= total {
                return Err(BrokerError::PoolExhausted);
            }
            match hdr.active_leases.0.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }

        // 3. Scan slots starting from rr_cursor to claim a free slot
        let pool_sz = self.pool_size();
        let start_idx = hdr.rr_cursor.0.fetch_add(1, Ordering::Relaxed) % total;

        for offset in 0..total {
            let idx = (start_idx + offset) % total;
            let slot = self.slot(idx);
            let state_pid = slot.state_and_pid.load(Ordering::Relaxed);
            let (state, _) = unpack_state_pid(state_pid);

            if state == SLOT_STATE_FREE {
                let target_state = if idx < pool_sz {
                    SLOT_STATE_PRIMARY
                } else {
                    SLOT_STATE_OVERFLOW
                };
                let new_state_pid = pack_state_pid(target_state, worker_pid);

                if slot
                    .state_and_pid
                    .compare_exchange(
                        state_pid,
                        new_state_pid,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    let grant_gen = slot.generation.fetch_add(1, Ordering::AcqRel) + 1;
                    let now = monotonic_now_ns();
                    let expiry = if ttl_ns > 0 { now + ttl_ns } else { 0 };

                    slot.acquired_ns.store(now, Ordering::Relaxed);
                    slot.expiry_ns.store(expiry, Ordering::Relaxed);
                    slot.bind_key_hash.store(bind_key_hash, Ordering::Relaxed);

                    hdr.total_acquired.fetch_add(1, Ordering::Relaxed);

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
            }
        }

        // If no slot was found (race with other writers), roll back counter
        hdr.active_leases.0.fetch_sub(1, Ordering::AcqRel);
        Err(BrokerError::PoolExhausted)
    }

    pub fn acquire(
        &self,
        worker_pid: u32,
        ttl_ns: u64,
        timeout: Duration,
        bind_key_hash: u64,
    ) -> Result<LeaseGrant, BrokerError> {
        // Fast path
        match self.try_acquire(worker_pid, ttl_ns, bind_key_hash) {
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

        // RAII guard to guarantee decrement of waiters count on return/panic
        struct WaitGuard<'a>(&'a AtomicU32);
        impl Drop for WaitGuard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }

        hdr.waiters.0.fetch_add(1, Ordering::AcqRel);
        let _guard = WaitGuard(&hdr.waiters.0);

        loop {
            let seq = hdr.wake_seq.0.load();

            // Retry after taking sequence snapshot
            match self.try_acquire(worker_pid, ttl_ns, bind_key_hash) {
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

    pub fn release(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError> {
        let total = self.total_capacity();
        if slot_index >= total {
            return Err(BrokerError::LeaseNotFound);
        }

        let slot = self.slot(slot_index);
        let cur_gen = slot.generation.load(Ordering::Acquire);
        if cur_gen != generation {
            return Err(BrokerError::GenerationMismatch);
        }

        let raw = slot.state_and_pid.load(Ordering::Acquire);
        let (state, _) = unpack_state_pid(raw);
        if state == SLOT_STATE_FREE {
            return Err(BrokerError::LeaseNotFound);
        }

        // Re-verify generation before clearing state
        if slot.generation.load(Ordering::Acquire) != generation {
            return Err(BrokerError::GenerationMismatch);
        }

        // CAS slot to free
        if slot
            .state_and_pid
            .compare_exchange(raw, 0, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Err(BrokerError::GenerationMismatch);
        }

        let hdr = self.header();
        hdr.active_leases.0.fetch_sub(1, Ordering::AcqRel);
        hdr.total_released.fetch_add(1, Ordering::Relaxed);

        // Wake one blocked waiter if any are queued
        if hdr.waiters.0.load(Ordering::Acquire) > 0 {
            hdr.wake_seq.0.fetch_add(1);
            let _ = hdr.wake_seq.0.wake_one();
        }

        Ok(())
    }

    pub fn reap_expired(&self, now_ns: u64) -> u32 {
        let total = self.total_capacity();
        let hdr = self.header();
        let mut reaped = 0u32;

        for idx in 0..total {
            let slot = self.slot(idx);
            let raw = slot.state_and_pid.load(Ordering::Acquire);
            let (state, _) = unpack_state_pid(raw);

            if state != SLOT_STATE_FREE {
                let exp = slot.expiry_ns.load(Ordering::Relaxed);
                if exp > 0 && now_ns >= exp {
                    // Invalidate generation first so late release fails with GenerationMismatch
                    slot.generation.fetch_add(1, Ordering::AcqRel);
                    if slot
                        .state_and_pid
                        .compare_exchange(raw, 0, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        reaped += 1;
                        hdr.active_leases.0.fetch_sub(1, Ordering::AcqRel);
                        hdr.total_expired.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        if reaped > 0 {
            hdr.wake_seq.0.fetch_add(1);
            let _ = hdr.wake_seq.0.wake_all();
        }

        reaped
    }

    pub fn reclaim_worker(&self, worker_pid: u32) -> u32 {
        let total = self.total_capacity();
        let hdr = self.header();
        let mut reclaimed = 0u32;

        for idx in 0..total {
            let slot = self.slot(idx);
            let raw = slot.state_and_pid.load(Ordering::Acquire);
            let (state, pid) = unpack_state_pid(raw);

            if state != SLOT_STATE_FREE && pid == worker_pid {
                slot.generation.fetch_add(1, Ordering::AcqRel);
                if slot
                    .state_and_pid
                    .compare_exchange(raw, 0, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    reclaimed += 1;
                    hdr.active_leases.0.fetch_sub(1, Ordering::AcqRel);
                    hdr.total_reclaimed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if reclaimed > 0 {
            hdr.wake_seq.0.fetch_add(1);
            let _ = hdr.wake_seq.0.wake_all();
        }

        reclaimed
    }

    pub fn stats(&self) -> LeaseTableStats {
        let hdr = self.header();
        let pool_sz = self.pool_size();
        let total = self.total_capacity();
        let mut primary_in_use = 0u32;
        let mut overflow_in_use = 0u32;

        for idx in 0..total {
            let slot = self.slot(idx);
            let (state, _) = unpack_state_pid(slot.state_and_pid.load(Ordering::Relaxed));
            match state {
                SLOT_STATE_PRIMARY => primary_in_use += 1,
                SLOT_STATE_OVERFLOW => overflow_in_use += 1,
                _ => {}
            }
        }

        LeaseTableStats {
            pool_size: pool_sz,
            max_overflow: self.max_overflow(),
            active_leases: hdr.active_leases.0.load(Ordering::Relaxed),
            primary_in_use,
            overflow_in_use,
            waiting_threads: hdr.waiters.0.load(Ordering::Relaxed),
            total_acquired: hdr.total_acquired.load(Ordering::Relaxed),
            total_released: hdr.total_released.load(Ordering::Relaxed),
            total_expired: hdr.total_expired.load(Ordering::Relaxed),
            total_timeouts: hdr.total_timeouts.load(Ordering::Relaxed),
            total_reclaimed: hdr.total_reclaimed.load(Ordering::Relaxed),
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
    _region: Option<ShmRegion>,
}

// SAFETY: All StatementSlot and ShmStatementHeader memory is accessed via atomic primitives.
unsafe impl Send for ShmStatementCache {}
unsafe impl Sync for ShmStatementCache {}

impl ShmStatementCache {
    pub fn compute_sql_hash(dialect_tag: u8, raw_sql: &[u8]) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET_BASIS;
        hash ^= dialect_tag as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
        for &byte in raw_sql {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    pub fn required_size(capacity: u32) -> usize {
        let cap = capacity.max(4).next_power_of_two();
        let raw = std::mem::size_of::<ShmStatementHeader>()
            + (cap as usize) * std::mem::size_of::<StatementSlot>();
        (raw + 63) & !63
    }

    pub fn init_in_region(mut region: ShmRegion, capacity: u32) -> Result<Self, BrokerError> {
        let cap = capacity.max(4).next_power_of_two();
        let req_size = Self::required_size(cap);
        if region.len() < req_size {
            return Err(BrokerError::RegionTooSmall {
                expected: req_size,
                actual: region.len(),
            });
        }

        // SAFETY: Zero out memory for statement cache.
        unsafe {
            let slice = region.as_mut_slice();
            slice[..req_size].fill(0);
        }

        let header_ptr = unsafe { region.as_mut_slice() }.as_mut_ptr() as *mut ShmStatementHeader;
        let slots_ptr = unsafe {
            (header_ptr as *mut u8).add(std::mem::size_of::<ShmStatementHeader>())
                as *mut StatementSlot
        };

        let num_sets = cap / 4;
        let set_mask = num_sets - 1;

        unsafe {
            let hdr = &mut *header_ptr;
            hdr.capacity = cap;
            hdr.set_mask = set_mask;
            hdr.hits.store(0, Ordering::Relaxed);
            hdr.misses.store(0, Ordering::Relaxed);
            hdr.inserts.store(0, Ordering::Relaxed);
            hdr.evictions.store(0, Ordering::Relaxed);
        }

        Ok(Self {
            header: header_ptr,
            slots: slots_ptr,
            _region: Some(region),
        })
    }

    pub fn attach_region(mut region: ShmRegion, capacity: u32) -> Result<Self, BrokerError> {
        let cap = capacity.max(4).next_power_of_two();
        let req_size = Self::required_size(cap);
        if region.len() < req_size {
            return Err(BrokerError::RegionTooSmall {
                expected: req_size,
                actual: region.len(),
            });
        }

        let header_ptr = unsafe { region.as_mut_slice() }.as_mut_ptr() as *mut ShmStatementHeader;
        let slots_ptr = unsafe {
            (header_ptr as *mut u8).add(std::mem::size_of::<ShmStatementHeader>())
                as *mut StatementSlot
        };

        Ok(Self {
            header: header_ptr,
            slots: slots_ptr,
            _region: Some(region),
        })
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
}
