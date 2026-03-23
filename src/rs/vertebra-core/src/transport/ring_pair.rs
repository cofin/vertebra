//! SPSC ring pair for multi-process transport.
//!
//! A `RingPair` encapsulates the bidirectional communication channel between
//! the supervisor and a single worker process:
//!
//! - **Request ring** (supervisor → worker): carries serialized `BatchSnapshot` data
//! - **Response rings** (worker → supervisor): tiered by response size for slot efficiency
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

use crate::buffer::{ChannelMode, RingLayout};
use crate::channel::{ChannelError, SpscRing};
use crate::notify::Notifier;
use crate::shm::ShmRegion;
use crate::tiered_ring::{RingTier, TieredRingConfig};

/// Magic marker for response wire format validation.
pub const RESPONSE_MAGIC: u32 = 0x56_52_45_53; // "VRES"

/// Fixed header size for serialized responses (magic + correlation_id + status + headers_len + body_len).
const RESPONSE_HEADER_SIZE: usize = 4 + 8 + 2 + 2 + 4; // 20 bytes

/// Deserialized response data from a ring slot.
#[derive(Debug, Clone)]
pub struct ResponseData {
    pub correlation_id: u64,
    pub status: u16,
    pub headers_raw: Vec<u8>,
    pub body: Vec<u8>,
}

impl ResponseData {
    fn deserialize(data: &[u8]) -> Result<Self, RingPairError> {
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

        let expected = RESPONSE_HEADER_SIZE + headers_len + body_len;
        if data.len() < expected {
            return Err(RingPairError::InvalidResponse("truncated"));
        }

        let headers_raw = data[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + headers_len].to_vec();
        let body = data
            [RESPONSE_HEADER_SIZE + headers_len..RESPONSE_HEADER_SIZE + headers_len + body_len]
            .to_vec();

        Ok(Self {
            correlation_id,
            status,
            headers_raw,
            body,
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
            // 64KB slots accommodate ~64 requests × 56-byte TransportNode + slab data
            request_slot_size: 64 * 1024,
            request_slots: 64,
            response_tiers: TieredRingConfig::default(),
        }
    }
}

/// A set of SPSC rings, one per response size tier.
pub struct TieredRingSet {
    pub small: Arc<SpscRing>,
    pub medium: Arc<SpscRing>,
    pub large: Arc<SpscRing>,
    pub xlarge: Arc<SpscRing>,
    pub config: TieredRingConfig,
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
        let small = Self::attach_tier_ring(&config.small, small_fd)?;
        let medium = Self::attach_tier_ring(&config.medium, medium_fd)?;
        let large = Self::attach_tier_ring(&config.large, large_fd)?;
        let xlarge = Self::attach_tier_ring(&config.xlarge, xlarge_fd)?;

        Ok(Self {
            small,
            medium,
            large,
            xlarge,
            config: config.clone(),
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
        let region = Arc::new(ShmRegion::map(fd, layout.total_size)?);
        let ring = SpscRing::attach(region, layout)?;
        Ok(Arc::new(ring))
    }

    /// Send a response through the appropriate tier ring.
    ///
    /// Wire format:
    /// ```text
    /// [magic: u32]            // RESPONSE_MAGIC for validation
    /// [correlation_id: u64]   // matches the original request
    /// [status: u16]           // HTTP status code
    /// [header_count: u16]     // number of headers
    /// [body_len: u32]         // length of body bytes
    /// [headers: encoded...]   // compact header format (name_len:u8, name, value_len:u16 LE, value)
    /// [body: u8 × body_len]  // response body
    /// ```
    pub fn send_response(
        &self,
        correlation_id: u64,
        status: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> Result<(), RingPairError> {
        let total_size = RESPONSE_HEADER_SIZE + headers_raw.len() + body.len();
        let ring = self
            .select_ring(total_size)
            .ok_or(RingPairError::ResponseTooLarge(total_size))?;

        let mut buf = vec![0u8; total_size];
        let mut offset = 0;

        // Magic
        buf[offset..offset + 4].copy_from_slice(&RESPONSE_MAGIC.to_le_bytes());
        offset += 4;
        // Correlation ID
        buf[offset..offset + 8].copy_from_slice(&correlation_id.to_le_bytes());
        offset += 8;
        // Status
        buf[offset..offset + 2].copy_from_slice(&status.to_le_bytes());
        offset += 2;
        // Header raw length (as u16)
        let headers_len = headers_raw.len() as u16;
        buf[offset..offset + 2].copy_from_slice(&headers_len.to_le_bytes());
        offset += 2;
        // Body length
        let body_len = body.len() as u32;
        buf[offset..offset + 4].copy_from_slice(&body_len.to_le_bytes());
        offset += 4;
        // Headers raw bytes
        buf[offset..offset + headers_raw.len()].copy_from_slice(headers_raw);
        offset += headers_raw.len();
        // Body
        buf[offset..offset + body.len()].copy_from_slice(body);

        ring.try_send(&buf, 0).map_err(RingPairError::Channel)
    }

    /// Try to receive a response from any tier (checks small → xlarge).
    ///
    /// Returns `None` if no response is available on any tier.
    pub fn try_recv_response(&self) -> Result<Option<ResponseData>, RingPairError> {
        // Check tiers in order: small first (most common), then up
        for ring in [&self.small, &self.medium, &self.large, &self.xlarge] {
            let mut buf = vec![0u8; ring.payload_capacity()];
            match ring.try_recv(&mut buf) {
                Ok(len) => {
                    return ResponseData::deserialize(&buf[..len]).map(Some);
                }
                Err(ChannelError::Empty) => continue,
                Err(e) => return Err(RingPairError::Channel(e)),
            }
        }
        Ok(None)
    }

    /// Select the ring for the given response size.
    pub fn select_ring(&self, response_size: usize) -> Option<&Arc<SpscRing>> {
        if response_size <= self.config.small.slot_size {
            Some(&self.small)
        } else if response_size <= self.config.medium.slot_size {
            Some(&self.medium)
        } else if response_size <= self.config.large.slot_size {
            Some(&self.large)
        } else if response_size <= self.config.xlarge.slot_size {
            Some(&self.xlarge)
        } else {
            None
        }
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

/// Bidirectional SPSC ring pair for one supervisor ↔ worker channel.
pub struct RingPair {
    /// Request ring: supervisor writes, worker reads.
    pub request_ring: Arc<SpscRing>,
    /// Response rings: worker writes, supervisor reads (tiered by size).
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
    ///
    /// Called by the supervisor before forking worker processes. The returned
    /// `RingPair` owns all SHM regions and notifier FDs. After fork, the child
    /// inherits the FDs and reconstructs via `from_inherited()`.
    pub fn create(config: RingPairConfig) -> Result<Self, RingPairError> {
        // Create request ring
        let req_layout = RingLayout::new(
            config.request_slot_size,
            config.request_slots,
            ChannelMode::Spsc,
        );
        let req_region = Arc::new(ShmRegion::create(req_layout.total_size)?);
        let pid = std::process::id();
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // SAFETY: we just created this region exclusively.
        let request_ring =
            Arc::new(unsafe { SpscRing::initialize(req_region, req_layout, pid, epoch)? });

        // Create tiered response rings
        let response_rings = TieredRingSet::create(&config.response_tiers)?;

        // Create notifiers (no CLOEXEC — inheritable across fork)
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

    /// Reconstruct a ring pair from inherited file descriptors.
    ///
    /// Called by worker processes after fork. The FDs were created by the
    /// supervisor's `RingPair::create()` and inherited across the spawn boundary.
    ///
    /// # Safety
    /// All FDs must be valid, open file descriptors that were created by a
    /// corresponding `RingPair::create()` call.
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
    ) -> Result<Self, RingPairError> {
        // Attach to request ring
        let req_layout = RingLayout::new(
            config.request_slot_size,
            config.request_slots,
            ChannelMode::Spsc,
        );
        let req_region = Arc::new(ShmRegion::map(req_ring_fd, req_layout.total_size)?);
        let request_ring = Arc::new(SpscRing::attach(req_region, req_layout)?);

        // Attach to response rings
        let response_rings = TieredRingSet::from_inherited(
            &config.response_tiers,
            resp_small_fd,
            resp_medium_fd,
            resp_large_fd,
            resp_xlarge_fd,
        )?;

        // Wrap inherited notifier FDs
        let request_notifier = Notifier::from_raw_fd(req_notify_fd);
        let response_notifier = Notifier::from_raw_fd(resp_notify_fd);

        Ok(Self {
            request_ring,
            response_rings,
            request_notifier,
            response_notifier,
            config,
        })
    }

    /// Reclaim this ring pair after a worker death.
    ///
    /// Resets all ring heads to zero and drains notifier counters so the
    /// ring pair can be reused by a replacement worker process.
    ///
    /// # Safety
    /// The dead worker must no longer be accessing these rings. Typically
    /// called after `waitpid` confirms the child has exited.
    pub unsafe fn reclaim(&self) {
        // Reset request ring
        self.request_ring.reset();
        // Reset all response tier rings
        self.response_rings.small.reset();
        self.response_rings.medium.reset();
        self.response_rings.large.reset();
        self.response_rings.xlarge.reset();
        // Drain notifier counters
        let _ = self.request_notifier.clear();
        let _ = self.response_notifier.clear();
    }

    /// Get the request ring's SHM handle for FD inheritance.
    #[cfg(unix)]
    pub fn request_ring_handle(&self) -> Option<RawFd> {
        self.request_ring.region_handle()
    }

    /// Collect all FDs needed for worker inheritance.
    #[cfg(unix)]
    pub fn fd_set(&self) -> RingPairFds {
        let resp_handles = self.response_rings.handles();
        let req_layout = RingLayout::new(
            self.config.request_slot_size,
            self.config.request_slots,
            ChannelMode::Spsc,
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
        }
    }

    /// Reconstruct a ring pair from environment variables set by the supervisor.
    ///
    /// Reads the `VERTEBRA_*` env var protocol and attaches to the inherited
    /// shared memory regions and notifier FDs.
    #[cfg(unix)]
    pub fn from_env(config: RingPairConfig) -> Result<Self, RingPairError> {
        fn env_parse<T: std::str::FromStr>(key: &str) -> Result<T, RingPairError> {
            std::env::var(key)
                .map_err(|_| RingPairError::EnvVar(key.to_owned()))
                .and_then(|v| {
                    v.parse::<T>()
                        .map_err(|_| RingPairError::EnvVar(key.to_owned()))
                })
        }

        let req_ring_fd: RawFd = env_parse("VERTEBRA_REQ_RING_FD")?;
        let _req_ring_size: usize = env_parse("VERTEBRA_REQ_RING_SIZE")?;
        let resp_small_fd: RawFd = env_parse("VERTEBRA_RESP_RING_SMALL_FD")?;
        let resp_medium_fd: RawFd = env_parse("VERTEBRA_RESP_RING_MEDIUM_FD")?;
        let resp_large_fd: RawFd = env_parse("VERTEBRA_RESP_RING_LARGE_FD")?;
        let resp_xlarge_fd: RawFd = env_parse("VERTEBRA_RESP_RING_XLARGE_FD")?;
        let req_notify_fd: RawFd = env_parse("VERTEBRA_REQ_NOTIFY_FD")?;
        let resp_notify_fd: RawFd = env_parse("VERTEBRA_RESP_NOTIFY_FD")?;

        // SAFETY: the env vars were set by the supervisor which created
        // valid ShmRegions and Notifier FDs before spawning this worker.
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
            )
        }
    }

    /// Get all raw FDs for `make_inheritable()`.
    #[cfg(unix)]
    pub fn all_fds(&self) -> Vec<RawFd> {
        let fds = self.fd_set();
        vec![
            fds.req_ring_fd,
            fds.resp_small_fd,
            fds.resp_medium_fd,
            fds.resp_large_fd,
            fds.resp_xlarge_fd,
            fds.req_notify_fd,
            fds.resp_notify_fd,
        ]
    }
}

/// All file descriptors needed to reconstruct a `RingPair` in a worker process.
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
}

#[cfg(unix)]
impl RingPairFds {
    /// Generate environment variables for the supervisor→worker FD protocol.
    pub fn to_env_vars(&self, worker_id: usize) -> Vec<(String, String)> {
        vec![
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
        ]
    }

    /// Collect all FDs into a slice for `make_inheritable()`.
    pub fn all_fds(&self) -> Vec<RawFd> {
        vec![
            self.req_ring_fd,
            self.resp_small_fd,
            self.resp_medium_fd,
            self.resp_large_fd,
            self.resp_xlarge_fd,
            self.req_notify_fd,
            self.resp_notify_fd,
        ]
    }
}

/// Errors from ring pair operations.
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
    fn default_config_is_sensible() {
        let config = RingPairConfig::default();
        assert_eq!(config.request_slot_size, 64 * 1024);
        assert_eq!(config.request_slots, 64);
        assert!(config.request_slots.is_power_of_two());
    }

    #[test]
    fn ring_pair_create_succeeds() {
        let pair = RingPair::create(RingPairConfig::default()).expect("create should succeed");
        // Request ring should be usable
        let data = b"hello ring pair";
        pair.request_ring
            .try_send(data, 0)
            .expect("send should succeed");
        let mut buf = vec![0u8; 1024];
        let len = pair
            .request_ring
            .try_recv(&mut buf)
            .expect("recv should succeed");
        assert_eq!(&buf[..len], data);
    }

    #[test]
    fn tiered_ring_set_create_and_select() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create should succeed");

        // Small response → small ring
        assert!(std::ptr::eq(
            set.select_ring(100).unwrap().as_ref(),
            set.small.as_ref()
        ));
        // Medium response → medium ring
        assert!(std::ptr::eq(
            set.select_ring(5000).unwrap().as_ref(),
            set.medium.as_ref()
        ));
        // Large response → large ring
        assert!(std::ptr::eq(
            set.select_ring(100_000).unwrap().as_ref(),
            set.large.as_ref()
        ));
        // XLarge response → xlarge ring
        assert!(std::ptr::eq(
            set.select_ring(1_000_000).unwrap().as_ref(),
            set.xlarge.as_ref()
        ));
        // Too large → None
        assert!(set.select_ring(3_000_000).is_none());
    }

    #[test]
    fn tiered_ring_set_send_recv_per_tier() {
        let config = TieredRingConfig::default();
        let set = TieredRingSet::create(&config).expect("create should succeed");

        // Send/recv through small tier
        let data = b"small response";
        set.small.try_send(data, 0).expect("send small");
        let mut buf = vec![0u8; 4096];
        let len = set.small.try_recv(&mut buf).expect("recv small");
        assert_eq!(&buf[..len], data);
    }

    #[test]
    fn ring_pair_notifiers_work() {
        let pair = RingPair::create(RingPairConfig::default()).expect("create");
        // Notify and clear should not error
        pair.request_notifier.notify().expect("notify request");
        pair.request_notifier.clear().expect("clear request");
        pair.response_notifier.notify().expect("notify response");
        pair.response_notifier.clear().expect("clear response");
    }

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
        assert_eq!(resp.headers_raw, headers_raw);
        assert_eq!(resp.body, body);
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

        let huge_body = vec![0u8; 3 * 1024 * 1024]; // 3MB > 2MB xlarge
        let result = set.send_response(1, 200, b"", &huge_body);
        assert!(matches!(result, Err(RingPairError::ResponseTooLarge(_))));
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
        assert_eq!(fds.len(), 7); // req_ring + 4 resp tiers + 2 notifiers
        for fd in &fds {
            assert!(*fd >= 0, "all FDs should be valid");
        }
    }
}
