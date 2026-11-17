//! SPSC ring pair for multi-process transport.
//!
//! A `RingPair` encapsulates the bidirectional communication channel between
//! the supervisor and a single worker process:
//!
//! - **Request ring** (supervisor → worker): carries serialized `BatchSnapshot` data (MPMC)
//! - **Response rings** (worker → supervisor): tiered by response size for slot efficiency (SPSC)
//! - **Notifiers**: eventfd/pipe pairs for instant cross-process wakeup
//!
//! # Lifecycle
//!
//! ```text
//! Supervisor:  RingPair::create(config) → fork worker → dispatch batches
//! Worker:      RingPair::from_inherited(fds) → poll_batch → send_response
//! ```

use std::sync::Arc;

#[cfg(unix)]
use std::os::fd::RawFd;

use bytes::Bytes;

use crate::buffer::{ChannelMode, RingLayout};
use crate::channel::{ChannelError, MpmcRing, SlotView, SpscRing};
use crate::notify::Notifier;
use crate::shm::ShmRegion;
use crate::tiered_ring::{RingTier, TieredRingConfig};

/// Magic marker for response wire format validation.
pub const RESPONSE_STREAM_MAGIC: u32 = 0x56_52_53_46;
const RESPONSE_STREAM_HEADER_SIZE: usize = 22;

pub const RESPONSE_MAGIC: u32 = 0x56_52_45_53; // "VRES"

/// Magic marker for out-of-line named SHM overflow responses (`> xlarge` tier).
pub const RESPONSE_OVERFLOW_MAGIC: u32 = 0x56_52_4F_56; // "VROV"
/// Maximum response frame size (64 MiB) allowed through a one-shot SHM overflow region.
pub const MAX_OVERFLOW_RESPONSE_SIZE: usize = 64 * 1024 * 1024;

/// Fixed header size for serialized responses (magic + correlation_id + status + headers_len + body_len).
const RESPONSE_HEADER_SIZE: usize = 4 + 8 + 2 + 2 + 4; // 20 bytes

/// Active tier bitmask constants stored in `TieredRingSet.small`'s `ShmHeader.active_flags`.
pub const TIER_BIT_SMALL: u32 = 1 << 0;
pub const TIER_BIT_MEDIUM: u32 = 1 << 1;
pub const TIER_BIT_LARGE: u32 = 1 << 2;
pub const TIER_BIT_XLARGE: u32 = 1 << 3;

/// Deserialized response data from a ring slot.
#[derive(Debug, Clone)]
pub struct ResponseData {
    pub correlation_id: u64,
    pub status: u16,
    pub headers_raw: Bytes,
    pub body: Bytes,
}

/// Zero-copy parsed `VRES` response frame view: `(correlation_id, status, headers_raw, body)`.
pub type ParsedResponseSlice<'a> = (u64, u16, &'a [u8], &'a [u8]);

/// Borrowed unary or stream response parsed directly from its ring slot.
#[derive(Debug, Clone, Copy)]
pub struct ResponseFrameView<'a> {
    pub correlation_id: u64,
    pub status: u16,
    pub flags: u16,
    pub headers_raw: &'a [u8],
    pub body: &'a [u8],
}

impl ResponseData {
    /// Parse either a legacy unary response or a flagged streaming frame.
    pub fn parse_frame(data: &[u8]) -> Result<ResponseFrameView<'_>, RingPairError> {
        if data.len() < 4 {
            return Err(RingPairError::InvalidResponse("too short"));
        }
        match u32::from_le_bytes(data[..4].try_into().unwrap()) {
            RESPONSE_MAGIC => {
                let (correlation_id, status, headers_raw, body) = Self::parse_slice(data)?;
                Ok(ResponseFrameView {
                    correlation_id,
                    status,
                    flags: crate::ipc::FLAG_FINAL,
                    headers_raw,
                    body,
                })
            }
            RESPONSE_STREAM_MAGIC => {
                if data.len() < RESPONSE_STREAM_HEADER_SIZE {
                    return Err(RingPairError::InvalidResponse("too short"));
                }
                let headers_len = u16::from_le_bytes(data[16..18].try_into().unwrap()) as usize;
                let body_len = u32::from_le_bytes(data[18..22].try_into().unwrap()) as usize;
                let headers_end = RESPONSE_STREAM_HEADER_SIZE + headers_len;
                let end = headers_end
                    .checked_add(body_len)
                    .ok_or(RingPairError::InvalidResponse("length overflow"))?;
                if end > data.len() {
                    return Err(RingPairError::InvalidResponse("truncated"));
                }
                Ok(ResponseFrameView {
                    correlation_id: u64::from_le_bytes(data[4..12].try_into().unwrap()),
                    status: u16::from_le_bytes(data[12..14].try_into().unwrap()),
                    flags: u16::from_le_bytes(data[14..16].try_into().unwrap()),
                    headers_raw: &data[RESPONSE_STREAM_HEADER_SIZE..headers_end],
                    body: &data[headers_end..end],
                })
            }
            _ => Err(RingPairError::InvalidResponse("bad magic")),
        }
    }

    /// Parse a serialized `VRES` response slot in-place without copying header or body slices.
    #[inline]
    pub fn parse_slice(data: &[u8]) -> Result<ParsedResponseSlice<'_>, RingPairError> {
        if data.len() < RESPONSE_HEADER_SIZE {
            return Err(RingPairError::InvalidResponse("too short"));
        }
        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != RESPONSE_MAGIC {
            return Err(RingPairError::InvalidResponse("bad magic"));
        }
        let correlation_id = u64::from_le_bytes(data[4..12].try_into().unwrap());
        let status = u16::from_le_bytes(data[12..14].try_into().unwrap());
        let headers_len = u16::from_le_bytes(data[14..16].try_into().unwrap()) as usize;
        let body_len = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;

        let expected = RESPONSE_HEADER_SIZE
            .checked_add(headers_len)
            .and_then(|s| s.checked_add(body_len))
            .ok_or(RingPairError::InvalidResponse("length overflow"))?;
        if data.len() < expected {
            return Err(RingPairError::InvalidResponse("truncated"));
        }

        let headers_end = RESPONSE_HEADER_SIZE + headers_len;
        let headers_raw = &data[RESPONSE_HEADER_SIZE..headers_end];
        let body = &data[headers_end..headers_end + body_len];
        Ok((correlation_id, status, headers_raw, body))
    }

    #[cfg(unix)]
    fn with_overflow_slice<R, F>(data: &[u8], f: F) -> Result<R, RingPairError>
    where
        F: FnOnce(ParsedResponseSlice<'_>) -> R,
    {
        Self::with_overflow_frame(data, |frame| {
            f((
                frame.correlation_id,
                frame.status,
                frame.headers_raw,
                frame.body,
            ))
        })
    }

    #[cfg(unix)]
    fn with_overflow_frame<R, F>(data: &[u8], f: F) -> Result<R, RingPairError>
    where
        F: FnOnce(ResponseFrameView<'_>) -> R,
    {
        if data.len() < 16 {
            return Err(RingPairError::InvalidResponse("overflow header too short"));
        }
        let total_size = u64::from_le_bytes(data[4..12].try_into().unwrap()) as usize;
        let name_len = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
        if data.len() < 16 + name_len {
            return Err(RingPairError::InvalidResponse("overflow header truncated"));
        }
        let name = std::str::from_utf8(&data[16..16 + name_len])
            .map_err(|_| RingPairError::InvalidResponse("invalid overflow region name"))?;
        let region = ShmRegion::open_named(name, total_size)?;
        let _ = crate::shm::shm_unlink_named(name);
        // SAFETY: open_named maps the validated overflow region size for this callback.
        let parsed = Self::parse_frame(unsafe { &region.as_slice()[..total_size] })?;
        Ok(f(parsed))
    }

    fn deserialize(data: &[u8]) -> Result<Self, RingPairError> {
        #[cfg(unix)]
        if data.len() >= 4
            && u32::from_le_bytes(data[0..4].try_into().unwrap()) == RESPONSE_OVERFLOW_MAGIC
        {
            return Self::with_overflow_slice(
                data,
                |(correlation_id, status, headers_raw, body)| Self {
                    correlation_id,
                    status,
                    headers_raw: Bytes::copy_from_slice(headers_raw),
                    body: Bytes::copy_from_slice(body),
                },
            );
        }
        let (correlation_id, status, headers_raw, body) = Self::parse_slice(data)?;
        Ok(Self {
            correlation_id,
            status,
            headers_raw: Bytes::copy_from_slice(headers_raw),
            body: Bytes::copy_from_slice(body),
        })
    }
}

/// Configuration for a `RingPair`.
#[derive(Debug, Clone)]
pub struct RingPairConfig {
    /// Slot size for the request ring (must accommodate a full serialized `BatchSnapshot`).
    pub request_slot_size: usize,
    /// Number of slots in the request ring (must be a power of 2).
    pub request_slots: usize,
    /// Tiered configuration for response rings.
    pub response_tiers: TieredRingConfig,
}

impl Default for RingPairConfig {
    fn default() -> Self {
        Self {
            // 64 KiB slots accommodate ~64 requests × 56-byte TransportNode + slab data;
            // 256 slots = 16 MiB per worker (~16,384 queued requests, 4x the server admission bound).
            request_slot_size: 64 * 1024,
            request_slots: 256,
            response_tiers: TieredRingConfig::default(),
        }
    }
}

impl RingPairConfig {
    /// Compact 12 MiB per-worker profile (`4 MiB` request ring + `8 MiB` response tiers)
    /// for memory-constrained containers (`<= 1 GiB` cgroup limit or `64 MiB` `/dev/shm`).
    pub fn compact() -> Self {
        Self {
            request_slot_size: 64 * 1024,
            request_slots: 64,
            response_tiers: TieredRingConfig::compact(),
        }
    }

    /// Select the appropriate `RingPairConfig` for `worker_count` based on `VERTEBRA_SHM_PROFILE`,
    /// cgroup v2 `memory.max`, and `/dev/shm` available space.
    pub fn for_environment(worker_count: usize) -> Self {
        if let Ok(profile) = std::env::var("VERTEBRA_SHM_PROFILE") {
            if profile.trim().eq_ignore_ascii_case("compact") {
                return Self::compact();
            }
            if profile.trim().eq_ignore_ascii_case("default")
                || profile.trim().eq_ignore_ascii_case("standard")
            {
                return Self::default();
            }
        }
        #[cfg(target_os = "linux")]
        {
            if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
                && let Ok(limit_bytes) = raw.trim().parse::<u64>()
                && limit_bytes <= 1024 * 1024 * 1024
            {
                return Self::compact();
            }
            let min_shm_bytes = (worker_count.max(1) as u64)
                .saturating_mul(48 * 1024 * 1024)
                .max(128 * 1024 * 1024);
            if !crate::shm::dev_shm_has_headroom(min_shm_bytes as usize) {
                return Self::compact();
            }
        }
        let _ = worker_count;
        Self::default()
    }
}

/// A set of SPSC rings, one per response size tier.
pub struct TieredRingSet {
    pub small: Arc<SpscRing>,
    pub medium: Arc<SpscRing>,
    pub large: Arc<SpscRing>,
    pub xlarge: Arc<SpscRing>,
    pub config: TieredRingConfig,
    next_tier: std::sync::atomic::AtomicUsize,
}

impl TieredRingSet {
    /// Create a new tiered ring set with freshly allocated shared memory regions.
    pub fn create(config: &TieredRingConfig) -> Result<Self, RingPairError> {
        let small = Self::create_tier_ring(&config.small)?;
        let medium = Self::create_tier_ring(&config.medium)?;
        let large = Self::create_tier_ring(&config.large)?;
        let xlarge = Self::create_tier_ring(&config.xlarge)?;

        Ok(Self {
            small,
            medium,
            large,
            xlarge,
            config: config.clone(),
            next_tier: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Attach to existing shared memory regions via inherited FDs.
    ///
    /// # Safety
    /// All FDs must be valid, open file descriptors pointing to properly
    /// initialized SPSC ring shared memory regions.
    #[cfg(unix)]
    unsafe fn from_inherited(
        config: &TieredRingConfig,
        small_fd: RawFd,
        medium_fd: RawFd,
        large_fd: RawFd,
        xlarge_fd: RawFd,
    ) -> Result<Self, RingPairError> {
        // SAFETY: caller guarantees each FD is a valid, open SPSC ring mapping matching `config`.
        let small = unsafe { Self::attach_tier_ring(&config.small, small_fd)? };
        // SAFETY: caller guarantees each FD is a valid, open SPSC ring mapping matching `config`.
        let medium = unsafe { Self::attach_tier_ring(&config.medium, medium_fd)? };
        // SAFETY: caller guarantees each FD is a valid, open SPSC ring mapping matching `config`.
        let large = unsafe { Self::attach_tier_ring(&config.large, large_fd)? };
        // SAFETY: caller guarantees each FD is a valid, open SPSC ring mapping matching `config`.
        let xlarge = unsafe { Self::attach_tier_ring(&config.xlarge, xlarge_fd)? };

        Ok(Self {
            small,
            medium,
            large,
            xlarge,
            config: config.clone(),
            next_tier: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn create_tier_ring(tier: &RingTier) -> Result<Arc<SpscRing>, RingPairError> {
        let layout = RingLayout::new(tier.slot_size, tier.slot_count, ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::create(layout.total_size)?);
        let pid = std::process::id();
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // SAFETY: we just created this region exclusively, so initialization is safe.
        let ring = unsafe { SpscRing::initialize(region, layout, pid, epoch)? };
        Ok(Arc::new(ring))
    }

    #[cfg(unix)]
    unsafe fn attach_tier_ring(tier: &RingTier, fd: RawFd) -> Result<Arc<SpscRing>, RingPairError> {
        let layout = RingLayout::new(tier.slot_size, tier.slot_count, ChannelMode::Spsc);
        // SAFETY: caller guarantees fd is valid and points to an initialized region.
        let region = Arc::new(unsafe { ShmRegion::map(fd, layout.total_size)? });
        // SAFETY: `region` was mapped with `layout.total_size` and initialized with `layout`.
        let ring = unsafe { SpscRing::attach(region, layout)? };
        Ok(Arc::new(ring))
    }

    fn write_response_slot(
        slot: &mut [u8],
        correlation_id: u64,
        status: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) {
        let mut offset = 0;
        slot[offset..offset + 4].copy_from_slice(&RESPONSE_MAGIC.to_le_bytes());
        offset += 4;
        slot[offset..offset + 8].copy_from_slice(&correlation_id.to_le_bytes());
        offset += 8;
        slot[offset..offset + 2].copy_from_slice(&status.to_le_bytes());
        offset += 2;
        let headers_len = headers_raw.len() as u16;
        slot[offset..offset + 2].copy_from_slice(&headers_len.to_le_bytes());
        offset += 2;
        let body_len = body.len() as u32;
        slot[offset..offset + 4].copy_from_slice(&body_len.to_le_bytes());
        offset += 4;
        slot[offset..offset + headers_raw.len()].copy_from_slice(headers_raw);
        offset += headers_raw.len();
        slot[offset..offset + body.len()].copy_from_slice(body);
    }

    /// Maximum combined headers and body size of an ordered stream frame.
    pub fn max_stream_frame_payload(&self) -> usize {
        self.xlarge
            .payload_capacity()
            .saturating_sub(RESPONSE_STREAM_HEADER_SIZE)
    }

    /// Send all stream frames through one tier so changing chunk sizes preserves order.
    pub fn send_stream_frame(
        &self,
        correlation_id: u64,
        status: u16,
        flags: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), RingPairError> {
        self.write_stream_frame(correlation_id, status, flags, headers_raw, body, false)
    }

    /// Send an ordered stream frame, waiting for capacity instead of losing data.
    pub fn send_stream_frame_blocking(
        &self,
        correlation_id: u64,
        status: u16,
        flags: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), RingPairError> {
        self.write_stream_frame(correlation_id, status, flags, headers_raw, body, true)
    }

    fn write_stream_frame(
        &self,
        correlation_id: u64,
        status: u16,
        flags: u16,
        headers_raw: &[u8],
        body: &[u8],
        blocking: bool,
    ) -> Result<(), RingPairError> {
        let total = RESPONSE_STREAM_HEADER_SIZE + headers_raw.len() + body.len();
        if headers_raw.len() > u16::MAX as usize || total > self.xlarge.payload_capacity() {
            return Err(RingPairError::ResponseTooLarge(total));
        }
        let write = |slot: &mut [u8]| {
            slot[..4].copy_from_slice(&RESPONSE_STREAM_MAGIC.to_le_bytes());
            slot[4..12].copy_from_slice(&correlation_id.to_le_bytes());
            slot[12..14].copy_from_slice(&status.to_le_bytes());
            slot[14..16].copy_from_slice(&flags.to_le_bytes());
            slot[16..18].copy_from_slice(&(headers_raw.len() as u16).to_le_bytes());
            slot[18..22].copy_from_slice(&(body.len() as u32).to_le_bytes());
            let headers_end = RESPONSE_STREAM_HEADER_SIZE + headers_raw.len();
            slot[RESPONSE_STREAM_HEADER_SIZE..headers_end].copy_from_slice(headers_raw);
            slot[headers_end..total].copy_from_slice(body);
        };
        self.mark_tier_active(TIER_BIT_XLARGE);
        let result = if blocking {
            self.xlarge.send_with(total, 0, write)
        } else {
            self.xlarge.try_send_with(total, 0, write)
        };
        if let Err(error) = result {
            self.clear_tier_if_empty(&self.xlarge, TIER_BIT_XLARGE);
            return Err(RingPairError::Channel(error));
        }
        Ok(())
    }

    /// Send a response through the appropriate tier ring without intermediate heap allocation.
    pub fn send_response(
        &self,
        correlation_id: u64,
        status: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), RingPairError> {
        if headers_raw.len() > u16::MAX as usize {
            return Err(RingPairError::InvalidResponse("headers exceed 65535 bytes"));
        }
        let total_size = RESPONSE_HEADER_SIZE + headers_raw.len() + body.len();
        let Some((ring, bit)) = self.select_ring_with_bit(total_size) else {
            #[cfg(unix)]
            {
                return self.send_overflow_response(total_size, false, |slot| {
                    Self::write_response_slot(slot, correlation_id, status, headers_raw, body)
                });
            }
            #[cfg(not(unix))]
            {
                return Err(RingPairError::ResponseTooLarge(total_size));
            }
        };

        self.mark_tier_active(bit);
        if let Err(e) = ring.try_send_with(total_size, 0, |slot| {
            Self::write_response_slot(slot, correlation_id, status, headers_raw, body);
        }) {
            self.clear_tier_if_empty(ring, bit);
            return Err(RingPairError::Channel(e));
        }
        Ok(())
    }

    /// Send a response through the appropriate tier ring, blocking until
    /// space is available if the ring is currently full.
    pub fn send_response_blocking(
        &self,
        correlation_id: u64,
        status: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), RingPairError> {
        if headers_raw.len() > u16::MAX as usize {
            return Err(RingPairError::InvalidResponse("headers exceed 65535 bytes"));
        }
        let total_size = RESPONSE_HEADER_SIZE + headers_raw.len() + body.len();
        let Some((ring, bit)) = self.select_ring_with_bit(total_size) else {
            #[cfg(unix)]
            {
                return self.send_overflow_response(total_size, true, |slot| {
                    Self::write_response_slot(slot, correlation_id, status, headers_raw, body)
                });
            }
            #[cfg(not(unix))]
            {
                return Err(RingPairError::ResponseTooLarge(total_size));
            }
        };

        self.mark_tier_active(bit);
        if let Err(e) = ring.send_with(total_size, 0, |slot| {
            Self::write_response_slot(slot, correlation_id, status, headers_raw, body);
        }) {
            self.clear_tier_if_empty(ring, bit);
            return Err(RingPairError::Channel(e));
        }
        Ok(())
    }

    /// Send a pre-encoded raw response frame through the appropriate tier ring.
    pub fn send_response_bytes(&self, raw_frame: &[u8]) -> Result<(), RingPairError> {
        let total_size = raw_frame.len();
        let Some((ring, bit)) = self.select_ring_with_bit(total_size) else {
            #[cfg(unix)]
            {
                return self.send_overflow_response(total_size, false, |slot| {
                    slot[..total_size].copy_from_slice(raw_frame);
                });
            }
            #[cfg(not(unix))]
            {
                return Err(RingPairError::ResponseTooLarge(total_size));
            }
        };

        self.mark_tier_active(bit);
        if let Err(e) = ring.try_send_with(total_size, 0, |slot| {
            slot[..total_size].copy_from_slice(raw_frame);
        }) {
            self.clear_tier_if_empty(ring, bit);
            return Err(RingPairError::Channel(e));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn send_overflow_response<F>(
        &self,
        total_size: usize,
        blocking: bool,
        write_fn: F,
    ) -> Result<(), RingPairError>
    where
        F: FnOnce(&mut [u8]),
    {
        if total_size > MAX_OVERFLOW_RESPONSE_SIZE {
            return Err(RingPairError::ResponseTooLarge(total_size));
        }
        let (name, mut region) = ShmRegion::create_overflow_region("rs", total_size)?;
        // SAFETY: `region` was just created exclusively with at least `total_size` bytes.
        write_fn(unsafe { &mut region.as_mut_slice()[..total_size] });
        let name_bytes = name.as_bytes();
        let env_len = 16 + name_bytes.len();
        self.mark_tier_active(TIER_BIT_SMALL);
        let send_res = if blocking {
            self.small.send_with(env_len, 0, |slot| {
                slot[0..4].copy_from_slice(&RESPONSE_OVERFLOW_MAGIC.to_le_bytes());
                slot[4..12].copy_from_slice(&(total_size as u64).to_le_bytes());
                slot[12..16].copy_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                slot[16..16 + name_bytes.len()].copy_from_slice(name_bytes);
            })
        } else {
            self.small.try_send_with(env_len, 0, |slot| {
                slot[0..4].copy_from_slice(&RESPONSE_OVERFLOW_MAGIC.to_le_bytes());
                slot[4..12].copy_from_slice(&(total_size as u64).to_le_bytes());
                slot[12..16].copy_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                slot[16..16 + name_bytes.len()].copy_from_slice(name_bytes);
            })
        };
        if let Err(e) = send_res {
            let _ = crate::shm::shm_unlink_named(&name);
            self.clear_tier_if_empty(&self.small, TIER_BIT_SMALL);
            return Err(RingPairError::Channel(e));
        }
        Ok(())
    }

    #[inline]
    fn mark_tier_active(&self, bit: u32) {
        let flags = self.small.active_flags();
        if (flags.load(std::sync::atomic::Ordering::SeqCst) & bit) == 0 {
            flags.fetch_or(bit, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[inline]
    fn clear_tier_if_empty(&self, ring: &SpscRing, bit: u32) {
        if !ring.has_data_acquire() {
            let flags = self.small.active_flags();
            flags.fetch_and(!bit, std::sync::atomic::Ordering::SeqCst);
            if ring.has_data_acquire() {
                flags.fetch_or(bit, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    /// Return the bitmask (`TIER_BIT_*`) of currently active response tiers.
    #[inline]
    pub fn active_tier_mask(&self) -> u32 {
        self.small
            .active_flags()
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Check whether any non-small response tier currently holds unread data (`Acquire` ordering).
    #[inline]
    fn has_non_small_data(&self) -> bool {
        self.medium.has_data_acquire()
            || self.large.has_data_acquire()
            || self.xlarge.has_data_acquire()
    }

    /// Check whether any response tier currently holds unread data (`Acquire` ordering).
    #[inline]
    pub fn has_any_data(&self) -> bool {
        self.small.has_data_acquire() || self.has_non_small_data()
    }

    /// Arm the Dekker `consumer_sleeping` flag (`1`) across all four response tiers.
    #[inline]
    pub fn prepare_sleep(&self) -> bool {
        self.small
            .consumer_sleeping()
            .store(1, std::sync::atomic::Ordering::SeqCst);
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        if self.has_any_data() {
            self.small
                .consumer_sleeping()
                .store(0, std::sync::atomic::Ordering::SeqCst);
            return false;
        }
        true
    }

    /// Disarm the Dekker `consumer_sleeping` flag (`0`).
    #[inline]
    pub fn cancel_sleep(&self) {
        self.small.cancel_sleep();
    }

    /// Check and clear `consumer_sleeping` (`SeqCst` Dekker).
    #[inline]
    pub fn should_notify_and_clear(&self) -> bool {
        self.small.should_notify_and_clear()
    }

    /// Notify `notifier` only when the response consumer has armed `consumer_sleeping == 1`.
    #[inline]
    pub fn notify_if_sleeping(&self, notifier: &Notifier) -> std::io::Result<bool> {
        self.small.notify_if_sleeping(notifier)
    }

    /// Try to receive a zero-copy [`SlotView`] from any active tier.
    pub fn try_recv_view(&self) -> Result<Option<SlotView>, RingPairError> {
        let mask = self.active_tier_mask();
        if (mask & !TIER_BIT_SMALL) == 0 && !self.has_non_small_data() {
            return match self.small.try_recv_view() {
                Ok(view) => Ok(Some(view)),
                Err(ChannelError::Empty) => {
                    if (mask & TIER_BIT_SMALL) != 0 {
                        self.clear_tier_if_empty(&self.small, TIER_BIT_SMALL);
                    }
                    Ok(None)
                }
                Err(e) => Err(RingPairError::Channel(e)),
            };
        }

        let tiers = [
            (&self.small, TIER_BIT_SMALL),
            (&self.medium, TIER_BIT_MEDIUM),
            (&self.large, TIER_BIT_LARGE),
            (&self.xlarge, TIER_BIT_XLARGE),
        ];
        let start = self
            .next_tier
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for offset in 0..tiers.len() {
            let (ring, bit) = tiers[(start.wrapping_add(offset)) % tiers.len()];
            if bit != TIER_BIT_SMALL && (mask & bit) == 0 && !ring.has_data_acquire() {
                continue;
            }
            match ring.try_recv_view() {
                Ok(view) => return Ok(Some(view)),
                Err(ChannelError::Empty) => {
                    self.clear_tier_if_empty(ring, bit);
                }
                Err(e) => return Err(RingPairError::Channel(e)),
            }
        }
        Ok(None)
    }

    /// Drain up to `max_items` responses across active tiers in small-first order without
    /// allocating intermediate `Vec<u8>` or `Bytes` buffers.
    pub fn drain_with<F>(&self, max_items: usize, mut f: F) -> Result<usize, RingPairError>
    where
        F: FnMut(u64, u16, &[u8], &[u8]),
    {
        self.drain_frames_with(max_items, |frame| {
            f(
                frame.correlation_id,
                frame.status,
                frame.headers_raw,
                frame.body,
            )
        })
    }

    /// Drain unary and streaming frames without copying their borrowed payloads.
    pub fn drain_frames_with<F>(&self, max_items: usize, mut f: F) -> Result<usize, RingPairError>
    where
        F: FnMut(ResponseFrameView<'_>),
    {
        if max_items == 0 {
            return Ok(0);
        }

        let mask = self.active_tier_mask();
        let tiers = [
            (&self.small, TIER_BIT_SMALL),
            (&self.medium, TIER_BIT_MEDIUM),
            (&self.large, TIER_BIT_LARGE),
            (&self.xlarge, TIER_BIT_XLARGE),
        ];

        let mut total_drained = 0usize;
        let mut parse_err: Option<RingPairError> = None;

        for (ring, bit) in tiers {
            if total_drained >= max_items {
                break;
            }
            if bit != TIER_BIT_SMALL && (mask & bit) == 0 && !ring.has_data_acquire() {
                continue;
            }
            let remaining = max_items - total_drained;
            let count = ring
                .drain_with(remaining, |slice, _flags| {
                    if parse_err.is_some() {
                        return;
                    }
                    #[cfg(unix)]
                    if slice.len() >= 4
                        && u32::from_le_bytes(slice[0..4].try_into().unwrap())
                            == RESPONSE_OVERFLOW_MAGIC
                    {
                        if let Err(err) = ResponseData::with_overflow_frame(slice, &mut f) {
                            parse_err = Some(err);
                        }
                        return;
                    }
                    match ResponseData::parse_frame(slice) {
                        Ok(frame) => f(frame),
                        Err(err) => {
                            parse_err = Some(err);
                        }
                    }
                })
                .map_err(RingPairError::Channel)?;
            total_drained += count;
            self.clear_tier_if_empty(ring, bit);
            if let Some(err) = parse_err {
                return Err(err);
            }
        }

        Ok(total_drained)
    }

    /// Try to receive a response from any tier using round-robin rotation and zero-copy slot views.
    pub fn try_recv_response(&self) -> Result<Option<ResponseData>, RingPairError> {
        let mask = self.active_tier_mask();
        if (mask & !TIER_BIT_SMALL) == 0 && !self.has_non_small_data() {
            return match self.small.try_recv_view() {
                Ok(view) => {
                    let parsed = ResponseData::deserialize(view.as_slice());
                    drop(view);
                    self.clear_tier_if_empty(&self.small, TIER_BIT_SMALL);
                    parsed.map(Some)
                }
                Err(ChannelError::Empty) => {
                    if (mask & TIER_BIT_SMALL) != 0 {
                        self.clear_tier_if_empty(&self.small, TIER_BIT_SMALL);
                    }
                    Ok(None)
                }
                Err(e) => Err(RingPairError::Channel(e)),
            };
        }

        let tiers = [
            (&self.small, TIER_BIT_SMALL),
            (&self.medium, TIER_BIT_MEDIUM),
            (&self.large, TIER_BIT_LARGE),
            (&self.xlarge, TIER_BIT_XLARGE),
        ];
        let start = self
            .next_tier
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for offset in 0..tiers.len() {
            let (ring, bit) = tiers[(start.wrapping_add(offset)) % tiers.len()];
            if bit != TIER_BIT_SMALL && (mask & bit) == 0 && !ring.has_data_acquire() {
                continue;
            }
            match ring.try_recv_view() {
                Ok(view) => {
                    let parsed = ResponseData::deserialize(view.as_slice());
                    drop(view);
                    self.clear_tier_if_empty(ring, bit);
                    return parsed.map(Some);
                }
                Err(ChannelError::Empty) => {
                    self.clear_tier_if_empty(ring, bit);
                }
                Err(e) => return Err(RingPairError::Channel(e)),
            }
        }
        Ok(None)
    }

    #[inline]
    fn select_ring_with_bit(&self, response_size: usize) -> Option<(&Arc<SpscRing>, u32)> {
        if response_size <= self.small.payload_capacity() {
            Some((&self.small, TIER_BIT_SMALL))
        } else if response_size <= self.medium.payload_capacity() {
            Some((&self.medium, TIER_BIT_MEDIUM))
        } else if response_size <= self.large.payload_capacity() {
            Some((&self.large, TIER_BIT_LARGE))
        } else if response_size <= self.xlarge.payload_capacity() {
            Some((&self.xlarge, TIER_BIT_XLARGE))
        } else {
            None
        }
    }

    /// Select the ring for the given response size.
    pub fn select_ring(&self, response_size: usize) -> Option<&Arc<SpscRing>> {
        self.select_ring_with_bit(response_size)
            .map(|(ring, _)| ring)
    }

    /// Get the SHM handle (FD) for each tier ring, for FD inheritance.
    #[cfg(unix)]
    pub fn handles(&self) -> [Option<RawFd>; 4] {
        [
            shm_handle_from_ring(&self.small),
            shm_handle_from_ring(&self.medium),
            shm_handle_from_ring(&self.large),
            shm_handle_from_ring(&self.xlarge),
        ]
    }
}

/// Extract the SHM handle from a SpscRing's underlying region.
#[cfg(unix)]
fn shm_handle_from_ring(ring: &Arc<SpscRing>) -> Option<RawFd> {
    ring.region_handle()
}

/// Bidirectional ring pair for one supervisor ↔ worker channel.
///
/// Uses MPMC for the request path (multiple collectors → single worker)
/// and SPSC for response path (single worker → single supervisor reader).
pub struct RingPair {
    /// Request ring: supervisor writes (MPMC), worker reads.
    pub request_ring: Arc<MpmcRing>,
    /// Response rings: worker writes, supervisor reads (tiered by size, SPSC).
    pub response_rings: TieredRingSet,
    /// Notifier for request ring (supervisor signals worker).
    pub request_notifier: Notifier,
    /// Notifier for response rings (worker signals supervisor).
    pub response_notifier: Notifier,
    /// Configuration used to create this pair.
    pub config: RingPairConfig,
}

impl RingPair {
    /// Create a new ring pair with freshly allocated shared memory.
    pub fn create(config: RingPairConfig) -> Result<Self, RingPairError> {
        let req_layout = RingLayout::new(
            config.request_slot_size,
            config.request_slots,
            ChannelMode::Mpmc,
        );
        let req_region = Arc::new(ShmRegion::create(req_layout.total_size)?);
        let pid = std::process::id();
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // SAFETY: `req_region` was freshly created exclusively with `req_layout.total_size` bytes.
        let request_ring =
            Arc::new(unsafe { MpmcRing::initialize(req_region, req_layout, pid, epoch)? });

        let response_rings = TieredRingSet::create(&config.response_tiers)?;
        let request_notifier = Notifier::new().map_err(RingPairError::Io)?;
        let response_notifier = Notifier::new().map_err(RingPairError::Io)?;

        Ok(Self {
            request_ring,
            response_rings,
            request_notifier,
            response_notifier,
            config,
        })
    }

    /// Attach to an inherited set of shared-memory ring and notifier file descriptors.
    ///
    /// # Safety
    ///
    /// All passed file descriptors must be valid, open shared-memory or eventfd
    /// descriptors initialized with layouts matching `config`.
    #[cfg(unix)]
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn from_inherited(
        config: RingPairConfig,
        req_ring_fd: RawFd,
        resp_small_fd: RawFd,
        resp_medium_fd: RawFd,
        resp_large_fd: RawFd,
        resp_xlarge_fd: RawFd,
        req_notify_fd: RawFd,
        resp_notify_fd: RawFd,
        #[cfg(not(target_os = "linux"))] req_notify_write_fd: RawFd,
        #[cfg(not(target_os = "linux"))] resp_notify_write_fd: RawFd,
    ) -> Result<Self, RingPairError> {
        let req_layout = RingLayout::new(
            config.request_slot_size,
            config.request_slots,
            ChannelMode::Mpmc,
        );
        // SAFETY: caller guarantees `req_ring_fd` is a valid, open SHM FD of `req_layout.total_size`.
        let req_region = Arc::new(unsafe { ShmRegion::map(req_ring_fd, req_layout.total_size)? });
        // SAFETY: `req_region` was mapped with `req_layout.total_size` and initialized with `req_layout`.
        let request_ring = Arc::new(unsafe { MpmcRing::attach(req_region, req_layout)? });

        // SAFETY: caller guarantees all four response tier FDs are valid and match `config.response_tiers`.
        let response_rings = unsafe {
            TieredRingSet::from_inherited(
                &config.response_tiers,
                resp_small_fd,
                resp_medium_fd,
                resp_large_fd,
                resp_xlarge_fd,
            )?
        };

        // SAFETY: caller guarantees `req_notify_fd` is a valid inherited eventfd descriptor.
        #[cfg(target_os = "linux")]
        let request_notifier = unsafe { Notifier::from_raw_fd(req_notify_fd) };
        // SAFETY: caller guarantees `resp_notify_fd` is a valid inherited eventfd descriptor.
        #[cfg(target_os = "linux")]
        let response_notifier = unsafe { Notifier::from_raw_fd(resp_notify_fd) };

        // SAFETY: caller guarantees `req_notify_fd` and `req_notify_write_fd` are valid inherited pipe FDs.
        #[cfg(not(target_os = "linux"))]
        let request_notifier =
            unsafe { Notifier::from_raw_fds(req_notify_fd, req_notify_write_fd) };
        // SAFETY: caller guarantees `resp_notify_fd` and `resp_notify_write_fd` are valid inherited pipe FDs.
        #[cfg(not(target_os = "linux"))]
        let response_notifier =
            unsafe { Notifier::from_raw_fds(resp_notify_fd, resp_notify_write_fd) };

        Ok(Self {
            request_ring,
            response_rings,
            request_notifier,
            response_notifier,
            config,
        })
    }

    /// Reset all request/response rings and drain notifiers after a worker exits.
    ///
    /// # Safety
    ///
    /// The caller must ensure no concurrent worker process is reading from or
    /// writing to the underlying rings while they are being reset.
    pub unsafe fn reclaim(&self) {
        // SAFETY: Reset is safe here because we assume the worker is stopped.
        unsafe {
            self.request_ring.reset();
            self.response_rings.small.reset();
            self.response_rings.medium.reset();
            self.response_rings.large.reset();
            self.response_rings.xlarge.reset();
        }
        let _ = self.request_notifier.clear();
        let _ = self.response_notifier.clear();
    }

    #[cfg(unix)]
    pub fn request_ring_handle(&self) -> Option<RawFd> {
        self.request_ring.region_handle()
    }

    #[cfg(unix)]
    pub fn fd_set(&self) -> RingPairFds {
        let resp_handles = self.response_rings.handles();
        let req_layout = RingLayout::new(
            self.config.request_slot_size,
            self.config.request_slots,
            ChannelMode::Mpmc,
        );
        RingPairFds {
            req_ring_fd: self.request_ring.region_handle().unwrap_or(-1),
            req_ring_size: req_layout.total_size,
            resp_small_fd: resp_handles[0].unwrap_or(-1),
            resp_small_size: RingLayout::new(
                self.config.response_tiers.small.slot_size,
                self.config.response_tiers.small.slot_count,
                ChannelMode::Spsc,
            )
            .total_size,
            resp_medium_fd: resp_handles[1].unwrap_or(-1),
            resp_medium_size: RingLayout::new(
                self.config.response_tiers.medium.slot_size,
                self.config.response_tiers.medium.slot_count,
                ChannelMode::Spsc,
            )
            .total_size,
            resp_large_fd: resp_handles[2].unwrap_or(-1),
            resp_large_size: RingLayout::new(
                self.config.response_tiers.large.slot_size,
                self.config.response_tiers.large.slot_count,
                ChannelMode::Spsc,
            )
            .total_size,
            resp_xlarge_fd: resp_handles[3].unwrap_or(-1),
            resp_xlarge_size: RingLayout::new(
                self.config.response_tiers.xlarge.slot_size,
                self.config.response_tiers.xlarge.slot_count,
                ChannelMode::Spsc,
            )
            .total_size,
            req_notify_fd: self.request_notifier.as_raw_fd(),
            resp_notify_fd: self.response_notifier.as_raw_fd(),
            #[cfg(not(target_os = "linux"))]
            req_notify_write_fd: self.request_notifier.write_fd(),
            #[cfg(not(target_os = "linux"))]
            resp_notify_write_fd: self.response_notifier.write_fd(),
        }
    }

    #[cfg(unix)]
    pub fn from_env(mut config: RingPairConfig) -> Result<Self, RingPairError> {
        fn env_parse<T: std::str::FromStr>(key: &str) -> Result<T, RingPairError> {
            std::env::var(key)
                .map_err(|_| RingPairError::EnvVar(key.to_owned()))
                .and_then(|v| {
                    v.parse::<T>()
                        .map_err(|_| RingPairError::EnvVar(key.to_owned()))
                })
        }

        let req_ring_fd: RawFd = env_parse("VERTEBRA_REQ_RING_FD")?;
        let req_ring_size: usize = env_parse("VERTEBRA_REQ_RING_SIZE")?;
        if let Some(slots) = RingLayout::slots_from_total_size(
            config.request_slot_size,
            req_ring_size,
            ChannelMode::Mpmc,
        ) {
            config.request_slots = slots;
        }
        if let Ok(small_size) = env_parse::<usize>("VERTEBRA_RESP_RING_SMALL_SIZE")
            && let Some(slots) = RingLayout::slots_from_total_size(
                config.response_tiers.small.slot_size,
                small_size,
                ChannelMode::Spsc,
            )
        {
            config.response_tiers.small.slot_count = slots;
        }
        if let Ok(medium_size) = env_parse::<usize>("VERTEBRA_RESP_RING_MEDIUM_SIZE")
            && let Some(slots) = RingLayout::slots_from_total_size(
                config.response_tiers.medium.slot_size,
                medium_size,
                ChannelMode::Spsc,
            )
        {
            config.response_tiers.medium.slot_count = slots;
        }
        if let Ok(large_size) = env_parse::<usize>("VERTEBRA_RESP_RING_LARGE_SIZE")
            && let Some(slots) = RingLayout::slots_from_total_size(
                config.response_tiers.large.slot_size,
                large_size,
                ChannelMode::Spsc,
            )
        {
            config.response_tiers.large.slot_count = slots;
        }
        if let Ok(xlarge_size) = env_parse::<usize>("VERTEBRA_RESP_RING_XLARGE_SIZE")
            && let Some(slots) = RingLayout::slots_from_total_size(
                config.response_tiers.xlarge.slot_size,
                xlarge_size,
                ChannelMode::Spsc,
            )
        {
            config.response_tiers.xlarge.slot_count = slots;
        }
        let resp_small_fd: RawFd = env_parse("VERTEBRA_RESP_RING_SMALL_FD")?;
        let resp_medium_fd: RawFd = env_parse("VERTEBRA_RESP_RING_MEDIUM_FD")?;
        let resp_large_fd: RawFd = env_parse("VERTEBRA_RESP_RING_LARGE_FD")?;
        let resp_xlarge_fd: RawFd = env_parse("VERTEBRA_RESP_RING_XLARGE_FD")?;
        let req_notify_fd: RawFd = env_parse("VERTEBRA_REQ_NOTIFY_FD")?;
        let resp_notify_fd: RawFd = env_parse("VERTEBRA_RESP_NOTIFY_FD")?;
        #[cfg(not(target_os = "linux"))]
        let req_notify_write_fd: RawFd = env_parse("VERTEBRA_REQ_NOTIFY_WRITE_FD")?;
        #[cfg(not(target_os = "linux"))]
        let resp_notify_write_fd: RawFd = env_parse("VERTEBRA_RESP_NOTIFY_WRITE_FD")?;

        // SAFETY: child process inherited open ring and notifier FDs configured by the parent supervisor.
        unsafe {
            Self::from_inherited(
                config,
                req_ring_fd,
                resp_small_fd,
                resp_medium_fd,
                resp_large_fd,
                resp_xlarge_fd,
                req_notify_fd,
                resp_notify_fd,
                #[cfg(not(target_os = "linux"))]
                req_notify_write_fd,
                #[cfg(not(target_os = "linux"))]
                resp_notify_write_fd,
            )
        }
    }

    #[cfg(unix)]
    pub fn all_fds(&self) -> Vec<RawFd> {
        self.fd_set().all_fds()
    }

    /// Send one ordered response stream frame.
    pub fn send_stream_frame(
        &self,
        correlation_id: u64,
        status: u16,
        flags: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), RingPairError> {
        self.response_rings
            .send_stream_frame(correlation_id, status, flags, headers_raw, body)
    }

    /// Wait for room to send one ordered response stream frame.
    pub fn send_stream_frame_blocking(
        &self,
        correlation_id: u64,
        status: u16,
        flags: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), RingPairError> {
        self.response_rings.send_stream_frame_blocking(
            correlation_id,
            status,
            flags,
            headers_raw,
            body,
        )
    }

    pub fn max_stream_frame_payload(&self) -> usize {
        self.response_rings.max_stream_frame_payload()
    }

    pub fn drain_frames_with<F: FnMut(ResponseFrameView<'_>)>(
        &self,
        max_items: usize,
        f: F,
    ) -> Result<usize, RingPairError> {
        self.response_rings.drain_frames_with(max_items, f)
    }

    /// Send a pre-encoded raw response frame through the tiered response rings.
    pub fn send_response_bytes(&self, raw_frame: &[u8]) -> Result<(), RingPairError> {
        self.response_rings.send_response_bytes(raw_frame)
    }

    /// Arm Dekker sleep on the request MPMC ring (`true` when empty and safe to wait).
    #[inline]
    pub fn prepare_request_sleep(&self) -> bool {
        self.request_ring.prepare_sleep()
    }

    /// Cancel Dekker sleep on the request MPMC ring.
    #[inline]
    pub fn cancel_request_sleep(&self) {
        self.request_ring.cancel_sleep();
    }

    /// Notify `request_notifier` only if the worker consumer is sleeping.
    #[inline]
    pub fn notify_request_if_sleeping(&self) -> std::io::Result<bool> {
        self.request_ring.notify_if_sleeping(&self.request_notifier)
    }

    /// Arm Dekker sleep on the tiered response rings (`true` when all tiers are empty).
    #[inline]
    pub fn prepare_response_sleep(&self) -> bool {
        self.response_rings.prepare_sleep()
    }

    /// Cancel Dekker sleep on the tiered response rings.
    #[inline]
    pub fn cancel_response_sleep(&self) {
        self.response_rings.cancel_sleep();
    }

    /// Notify `response_notifier` only if the supervisor response reader is sleeping.
    #[inline]
    pub fn notify_response_if_sleeping(&self) -> std::io::Result<bool> {
        self.response_rings
            .notify_if_sleeping(&self.response_notifier)
    }
}

#[cfg(unix)]
#[derive(Debug, Clone)]
pub struct RingPairFds {
    pub req_ring_fd: RawFd,
    pub req_ring_size: usize,
    pub resp_small_fd: RawFd,
    pub resp_small_size: usize,
    pub resp_medium_fd: RawFd,
    pub resp_medium_size: usize,
    pub resp_large_fd: RawFd,
    pub resp_large_size: usize,
    pub resp_xlarge_fd: RawFd,
    pub resp_xlarge_size: usize,
    pub req_notify_fd: RawFd,
    pub resp_notify_fd: RawFd,
    #[cfg(not(target_os = "linux"))]
    pub req_notify_write_fd: RawFd,
    #[cfg(not(target_os = "linux"))]
    pub resp_notify_write_fd: RawFd,
}

#[cfg(unix)]
impl RingPairFds {
    pub fn to_env_vars(&self, worker_id: usize) -> Vec<(String, String)> {
        #[allow(unused_mut)]
        let mut vars = vec![
            ("VERTEBRA_TRANSPORT_MODE".into(), "shm".into()),
            ("VERTEBRA_WORKER_ID".into(), worker_id.to_string()),
            ("VERTEBRA_REQ_RING_FD".into(), self.req_ring_fd.to_string()),
            (
                "VERTEBRA_REQ_RING_SIZE".into(),
                self.req_ring_size.to_string(),
            ),
            (
                "VERTEBRA_RESP_RING_SMALL_FD".into(),
                self.resp_small_fd.to_string(),
            ),
            (
                "VERTEBRA_RESP_RING_SMALL_SIZE".into(),
                self.resp_small_size.to_string(),
            ),
            (
                "VERTEBRA_RESP_RING_MEDIUM_FD".into(),
                self.resp_medium_fd.to_string(),
            ),
            (
                "VERTEBRA_RESP_RING_MEDIUM_SIZE".into(),
                self.resp_medium_size.to_string(),
            ),
            (
                "VERTEBRA_RESP_RING_LARGE_FD".into(),
                self.resp_large_fd.to_string(),
            ),
            (
                "VERTEBRA_RESP_RING_LARGE_SIZE".into(),
                self.resp_large_size.to_string(),
            ),
            (
                "VERTEBRA_RESP_RING_XLARGE_FD".into(),
                self.resp_xlarge_fd.to_string(),
            ),
            (
                "VERTEBRA_RESP_RING_XLARGE_SIZE".into(),
                self.resp_xlarge_size.to_string(),
            ),
            (
                "VERTEBRA_REQ_NOTIFY_FD".into(),
                self.req_notify_fd.to_string(),
            ),
            (
                "VERTEBRA_RESP_NOTIFY_FD".into(),
                self.resp_notify_fd.to_string(),
            ),
        ];
        #[cfg(not(target_os = "linux"))]
        {
            vars.push((
                "VERTEBRA_REQ_NOTIFY_WRITE_FD".into(),
                self.req_notify_write_fd.to_string(),
            ));
            vars.push((
                "VERTEBRA_RESP_NOTIFY_WRITE_FD".into(),
                self.resp_notify_write_fd.to_string(),
            ));
        }
        vars
    }

    pub fn all_fds(&self) -> Vec<RawFd> {
        #[allow(unused_mut)]
        let mut fds = vec![
            self.req_ring_fd,
            self.resp_small_fd,
            self.resp_medium_fd,
            self.resp_large_fd,
            self.resp_xlarge_fd,
            self.req_notify_fd,
            self.resp_notify_fd,
        ];
        #[cfg(not(target_os = "linux"))]
        {
            fds.push(self.req_notify_write_fd);
            fds.push(self.resp_notify_write_fd);
        }
        fds
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RingPairError {
    #[error("shared memory error: {0}")]
    Shm(#[from] crate::shm::ShmError),
    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("I/O error: {0}")]
    Io(std::io::Error),
    #[error("response too large for any tier: {0} bytes")]
    ResponseTooLarge(usize),
    #[error("invalid response data: {0}")]
    InvalidResponse(&'static str),
    #[error("missing or invalid environment variable: {0}")]
    EnvVar(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiered_ring_set_response_round_trip() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create");

        let headers_raw = b"\x04host\x09\x00localhost";
        let body = b"hello world";
        set.send_response(42, 200, headers_raw, body)
            .expect("send_response");

        let resp = set
            .try_recv_response()
            .expect("try_recv")
            .expect("should have response");
        assert_eq!(resp.correlation_id, 42);
        assert_eq!(resp.status, 200);
        assert_eq!(resp.headers_raw, &headers_raw[..]);
        assert_eq!(resp.body, &body[..]);
    }

    #[test]
    fn tiered_ring_set_response_empty_body() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create");

        set.send_response(99, 204, b"", b"").expect("send");

        let resp = set
            .try_recv_response()
            .expect("recv")
            .expect("should have response");
        assert_eq!(resp.correlation_id, 99);
        assert_eq!(resp.status, 204);
        assert!(resp.headers_raw.is_empty());
        assert!(resp.body.is_empty());
    }

    #[test]
    fn tiered_ring_set_recv_none_when_empty() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create");
        assert!(set.try_recv_response().expect("recv").is_none());
    }

    #[test]
    fn tiered_ring_set_response_too_large() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create");

        #[cfg(unix)]
        {
            let overflow_body = vec![0xABu8; 3 * 1024 * 1024];
            set.send_response(77, 200, b"", &overflow_body)
                .expect("3 MiB response should spill into SHM overflow region");
            let received = set
                .try_recv_response()
                .expect("recv")
                .expect("should receive overflow response");
            assert_eq!(received.correlation_id, 77);
            assert_eq!(received.status, 200);
            assert_eq!(received.body.len(), overflow_body.len());

            let too_huge = vec![0u8; MAX_OVERFLOW_RESPONSE_SIZE + 1];
            let result = set.send_response(78, 200, b"", &too_huge);
            assert!(matches!(result, Err(RingPairError::ResponseTooLarge(_))));
        }

        #[cfg(not(unix))]
        {
            let huge_body = vec![0u8; 3 * 1024 * 1024];
            let result = set.send_response(1, 200, b"", &huge_body);
            assert!(matches!(result, Err(RingPairError::ResponseTooLarge(_))));
        }
    }

    #[test]
    fn tiered_ring_set_selects_next_tier_near_boundary() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create");

        // Small tier capacity is 4096 - SlotHeader::SIZE (8) = 4088 bytes
        // Frame size is RESPONSE_HEADER_SIZE (20) + headers_len + body_len
        // If total frame size is small.payload_capacity() + 1, it must select medium tier and succeed
        let small_cap = set.small.payload_capacity();
        let body_len = (small_cap + 1) - RESPONSE_HEADER_SIZE;
        let body = vec![7u8; body_len];
        set.send_response(123, 200, b"", &body)
            .expect("should route to medium tier and succeed");

        let resp = set
            .try_recv_response()
            .expect("recv")
            .expect("should have response");
        assert_eq!(resp.correlation_id, 123);
        assert_eq!(resp.body.len(), body_len);
    }

    #[test]
    fn write_response_slot_rejects_oversized_headers() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create");

        // Headers larger than 65535 bytes must be rejected with InvalidResponse
        let huge_headers = vec![0u8; 66_000];
        let err = set
            .send_response(1, 200, &huge_headers, b"")
            .expect_err("should reject oversized headers");
        assert!(matches!(err, RingPairError::InvalidResponse(_)));
    }

    #[test]
    fn tiered_ring_set_fair_drain_rotates_across_tiers() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create");

        // Enqueue 2 small responses and 1 medium response
        set.send_response(1, 200, b"", b"small-1")
            .expect("send small-1");
        set.send_response(2, 200, b"", b"small-2")
            .expect("send small-2");
        let medium_body = vec![b'm'; set.small.payload_capacity() + 64];
        set.send_response(3, 200, b"", &medium_body)
            .expect("send medium-3");

        let first = set.try_recv_response().unwrap().unwrap();
        let second = set.try_recv_response().unwrap().unwrap();
        let third = set.try_recv_response().unwrap().unwrap();

        // Because try_recv_response rotates the starting tier index on every call,
        // the medium response is drained on the second call instead of starving behind small-2.
        assert_eq!(first.correlation_id, 1);
        assert_eq!(second.correlation_id, 3);
        assert_eq!(third.correlation_id, 2);
    }

    mod prop_tests {
        use crate::transport::{BatchSnapshot, TransportNode};
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn batch_snapshot_serialize_arbitrary(
                slab_data in proptest::collection::vec(any::<u8>(), 0..1024),
                node_count in 0usize..32,
                flags in any::<u32>(),
            ) {
                let nodes: Vec<TransportNode> = (0..node_count)
                    .map(|i| TransportNode {
                        method_tag: (i % 9) as u8,
                        http_version_tag: 1,
                        _pad: [0; 2],
                        handler_id: i as u64,
                        correlation_id: i as u64 * 7,
                        path_offset: 0,
                        path_len: 0,
                        query_offset: 0,
                        query_len: 0,
                        headers_offset: 0,
                        headers_len: 0,
                        body_offset: 0,
                        body_len: 0,
                    })
                    .collect();

                let flags = flags & !(crate::protocol::flags::COMPRESSED | crate::protocol::flags::SHM_OVERFLOW);
                let snap = BatchSnapshot::new_with_flags(slab_data.clone(), nodes.clone(), flags);
                let mut buf = vec![0u8; snap.serialized_size()];
                let written = snap.serialize_into(&mut buf);
                prop_assert_eq!(written, snap.serialized_size());

                let deserialized = BatchSnapshot::deserialize_from(&buf[..written])
                    .expect("deserialize should succeed");
                prop_assert_eq!(deserialized.len(), node_count);
                prop_assert_eq!(deserialized.flags(), flags);
                for i in 0..node_count {
                    prop_assert_eq!(deserialized.node(i).handler_id, i as u64);
                    prop_assert_eq!(deserialized.node(i).correlation_id, i as u64 * 7);
                }
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn ring_pair_from_inherited_round_trips() {
        let config = RingPairConfig::default();
        let pair = RingPair::create(config.clone()).expect("create");

        // Get FDs for inheritance
        let req_fd = pair.request_ring_handle().expect("request ring handle");
        let resp_handles = pair.response_rings.handles();
        let req_notify_fd = pair.request_notifier.as_raw_fd();
        let resp_notify_fd = pair.response_notifier.as_raw_fd();

        // Simulate worker-side attach (same process, but tests the API)
        // Note: we can't truly test from_inherited in the same process because
        // the original RingPair still owns the FDs. Instead, verify the FDs are valid.
        assert!(req_fd >= 0);
        for h in &resp_handles {
            assert!(h.unwrap() >= 0);
        }
        assert!(req_notify_fd >= 0);
        assert!(resp_notify_fd >= 0);

        // Verify data round-trips through the request ring
        let data = b"inherited test";
        pair.request_ring.try_send(data, 0).expect("send");
        let mut buf = vec![0u8; 1024];
        let len = pair.request_ring.try_recv(&mut buf).expect("recv");
        assert_eq!(&buf[..len], data);
    }

    #[cfg(unix)]
    #[test]
    fn fd_set_returns_valid_fds() {
        let pair = RingPair::create(RingPairConfig::default()).expect("create");
        let fds = pair.fd_set();

        assert!(fds.req_ring_fd >= 0, "request ring FD should be valid");
        assert!(fds.resp_small_fd >= 0, "small response FD should be valid");
        assert!(fds.resp_medium_fd >= 0);
        assert!(fds.resp_large_fd >= 0);
        assert!(fds.resp_xlarge_fd >= 0);
        assert!(fds.req_notify_fd >= 0);
        assert!(fds.resp_notify_fd >= 0);
        #[cfg(not(target_os = "linux"))]
        {
            assert!(fds.req_notify_write_fd >= 0);
            assert!(fds.resp_notify_write_fd >= 0);
        }
        assert!(fds.req_ring_size > 0);
        assert!(fds.resp_small_size > 0);
    }

    #[cfg(unix)]
    #[test]
    fn env_vars_protocol_complete() {
        let pair = RingPair::create(RingPairConfig::default()).expect("create");
        let fds = pair.fd_set();
        let vars = fds.to_env_vars(3);

        // Should contain all expected vars
        let keys: Vec<&str> = vars.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"VERTEBRA_TRANSPORT_MODE"));
        assert!(keys.contains(&"VERTEBRA_WORKER_ID"));
        assert!(keys.contains(&"VERTEBRA_REQ_RING_FD"));
        assert!(keys.contains(&"VERTEBRA_REQ_RING_SIZE"));
        assert!(keys.contains(&"VERTEBRA_RESP_RING_SMALL_FD"));
        assert!(keys.contains(&"VERTEBRA_RESP_RING_MEDIUM_FD"));
        assert!(keys.contains(&"VERTEBRA_RESP_RING_LARGE_FD"));
        assert!(keys.contains(&"VERTEBRA_RESP_RING_XLARGE_FD"));
        assert!(keys.contains(&"VERTEBRA_REQ_NOTIFY_FD"));
        assert!(keys.contains(&"VERTEBRA_RESP_NOTIFY_FD"));
        #[cfg(not(target_os = "linux"))]
        {
            assert!(keys.contains(&"VERTEBRA_REQ_NOTIFY_WRITE_FD"));
            assert!(keys.contains(&"VERTEBRA_RESP_NOTIFY_WRITE_FD"));
        }

        // Worker ID should be 3
        let worker_id = vars
            .iter()
            .find(|(k, _)| k == "VERTEBRA_WORKER_ID")
            .unwrap();
        assert_eq!(worker_id.1, "3");

        // Transport mode should be shm
        let mode = vars
            .iter()
            .find(|(k, _)| k == "VERTEBRA_TRANSPORT_MODE")
            .unwrap();
        assert_eq!(mode.1, "shm");
    }

    #[cfg(unix)]
    #[test]
    fn all_fds_for_make_inheritable() {
        let pair = RingPair::create(RingPairConfig::default()).expect("create");
        let fds = pair.all_fds();
        #[cfg(target_os = "linux")]
        assert_eq!(fds.len(), 7); // req_ring + 4 resp tiers + 2 notifiers
        #[cfg(not(target_os = "linux"))]
        assert_eq!(fds.len(), 9); // req_ring + 4 resp tiers + 2 pipe pairs (read + write)
        for fd in &fds {
            assert!(*fd >= 0, "all FDs should be valid");
        }
    }

    #[test]
    fn ring_pair_compact_profile_and_slot_reconstruction() {
        let compact = RingPairConfig::compact();
        assert_eq!(compact.request_slots, 64);
        assert_eq!(compact.response_tiers.small.slot_count, 256);
        assert_eq!(compact.response_tiers.medium.slot_count, 32);
        assert_eq!(compact.response_tiers.large.slot_count, 8);
        assert_eq!(compact.response_tiers.xlarge.slot_count, 2);

        let req_layout = RingLayout::new(
            compact.request_slot_size,
            compact.request_slots,
            ChannelMode::Mpmc,
        );
        assert_eq!(
            RingLayout::slots_from_total_size(
                compact.request_slot_size,
                req_layout.total_size,
                ChannelMode::Mpmc,
            ),
            Some(64)
        );

        let small_layout = RingLayout::new(
            compact.response_tiers.small.slot_size,
            compact.response_tiers.small.slot_count,
            ChannelMode::Spsc,
        );
        assert_eq!(
            RingLayout::slots_from_total_size(
                compact.response_tiers.small.slot_size,
                small_layout.total_size,
                ChannelMode::Spsc,
            ),
            Some(256)
        );
    }

    #[cfg(unix)]
    #[test]
    fn file_backed_overflow_round_trip() {
        let mut region = crate::shm::ShmRegion::create_file_backed("/tmp/vtb-test-ovf", 8192)
            .expect("create file-backed overflow");
        region.disarm_unlink();
        // SAFETY: `region` was freshly created with 8192 bytes, and 18 bytes fit within it.
        unsafe {
            std::ptr::copy_nonoverlapping(b"hello-tmp-overflow".as_ptr(), region.as_ptr(), 18);
        }
        let opened =
            crate::shm::ShmRegion::open_named("/tmp/vtb-test-ovf", 8192).expect("open file-backed");
        // SAFETY: `opened` mapped 8192 bytes and is valid for an 18-byte immutable slice.
        let slice = unsafe { std::slice::from_raw_parts(opened.as_ptr(), 18) };
        assert_eq!(slice, b"hello-tmp-overflow");
        let _ = crate::shm::shm_unlink_named("/tmp/vtb-test-ovf");
    }
}
