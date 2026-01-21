//! Lock-free ring buffer channel (SPSC implementation).

use core::ptr;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::Backoff;

use crate::buffer::{
    Buffer, BufferError, BufferPool, ChannelMode, RegionState, RingLayout, ShmHeader, SlotHeader,
    ABI_VERSION, MAGIC, SLOT_ALIGN,
};
use crate::shm::{ShmError, ShmRegion};
use crate::sync::{self, SyncError};

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("ring layout is invalid")]
    InvalidLayout,
    #[error("region is not ready")]
    NotReady,
    #[error("channel is full")]
    Full,
    #[error("channel is empty")]
    Empty,
    #[error("payload too large: {len} > {capacity}")]
    PayloadTooLarge { len: usize, capacity: usize },
    #[error("buffer too small: {len} > {capacity}")]
    BufferTooSmall { len: usize, capacity: usize },
    #[error("buffer metadata invalid")]
    InvalidBufferMeta,
    #[error("buffer error: {0}")]
    Buffer(#[from] BufferError),
    #[error("shared memory error: {0}")]
    Shm(#[from] ShmError),
    #[error("sync failure: {0}")]
    Sync(#[from] SyncError),
}

/// SPSC ring channel backed by a shared memory region.
#[derive(Debug)]
pub struct SpscRing {
    #[allow(dead_code)]
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    data: *mut u8,
    layout: RingLayout,
    mask: u64,
}

unsafe impl Send for SpscRing {}
unsafe impl Sync for SpscRing {}

#[derive(Debug, Copy, Clone)]
pub struct RingStats {
    pub slot_size: usize,
    pub slots: usize,
    pub read_head: u64,
    pub write_head: u64,
}

impl SpscRing {
    /// Initialize a shared region in-place and return a ring handle.
    ///
    /// # Safety
    /// Caller must ensure exclusive initialization (single writer) and that
    /// the region is at least `layout.total_size` bytes.
    pub unsafe fn initialize(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        creator_pid: u32,
        creator_epoch: u64,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two() || layout.slot_size < core::mem::size_of::<SlotHeader>() {
            return Err(ChannelError::InvalidLayout);
        }
        if region.len() < layout.total_size {
            return Err(ChannelError::InvalidLayout);
        }
        // SAFETY: caller guarantees exclusive initialization and valid mapping.
        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &mut *header;
        header_ref.magic = MAGIC;
        header_ref.abi_version = ABI_VERSION;
        header_ref.flags = 0;
        header_ref.total_size = layout.total_size as u64;
        header_ref.ring_offset = layout.header_size as u64;
        header_ref.ring_size = layout.ring_size as u64;
        header_ref.generation.store(1, Ordering::Relaxed);
        header_ref
            .state
            .store(RegionState::Ready as u32, Ordering::Release);
        header_ref.creator_pid = creator_pid;
        header_ref.creator_epoch = creator_epoch;
        header_ref.read_head.0.store(0, Ordering::Relaxed);
        header_ref.write_head.0.store(0, Ordering::Relaxed);
        header_ref.wait_read.0.store(0, Ordering::Relaxed);
        header_ref.wait_write.0.store(0, Ordering::Relaxed);
        Self::from_region(region, layout)
    }

    /// Attach to an existing shared region and validate the header.
    ///
    /// # Safety
    /// Caller must ensure the region stays mapped for the lifetime of the ring.
    pub unsafe fn attach(region: Arc<ShmRegion>, layout: RingLayout) -> Result<Self, ChannelError> {
        // SAFETY: caller guarantees region is initialized and mapped.
        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &*header;
        if header_ref.magic != MAGIC || header_ref.abi_version != ABI_VERSION {
            return Err(ChannelError::NotReady);
        }
        if header_ref.state.load(Ordering::Acquire) != RegionState::Ready as u32 {
            return Err(ChannelError::NotReady);
        }
        Self::from_region(region, layout)
    }

    /// Create a ring without initializing the header.
    ///
    /// # Safety
    /// Caller must ensure the region is already initialized correctly.
    pub unsafe fn from_region(
        region: Arc<ShmRegion>,
        layout: RingLayout,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two() {
            return Err(ChannelError::InvalidLayout);
        }
        // SAFETY: region is a valid shared memory mapping.
        let header = region.as_ptr().cast::<ShmHeader>();
        let data = region.as_ptr().add(layout.header_size);
        Ok(Self {
            region,
            header,
            data,
            layout,
            mask: (layout.slots as u64).wrapping_sub(1),
        })
    }

    pub fn payload_capacity(&self) -> usize {
        self.layout.slot_size - core::mem::size_of::<SlotHeader>()
    }

    pub fn stats(&self) -> RingStats {
        RingStats {
            slot_size: self.layout.slot_size,
            slots: self.layout.slots,
            read_head: self.read_head().load(Ordering::Acquire),
            write_head: self.write_head().load(Ordering::Acquire),
        }
    }

    /// Split the ring into sender/receiver handles.
    ///
    /// The caller must ensure only one sender and one receiver are used.
    pub fn split(self) -> (SpscSender, SpscReceiver) {
        let ring = Arc::new(self);
        (
            SpscSender {
                ring: Arc::clone(&ring),
            },
            SpscReceiver { ring },
        )
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        if payload.len() > self.payload_capacity() {
            return Err(ChannelError::PayloadTooLarge {
                len: payload.len(),
                capacity: self.payload_capacity(),
            });
        }
        let head = self.read_head().load(Ordering::Acquire);
        let tail = self.write_head().load(Ordering::Acquire);
        let next = tail.wrapping_add(1) & self.mask;
        if next == head {
            return Err(ChannelError::Full);
        }
        unsafe {
            self.write_slot(tail, payload, flags);
        }
        self.write_head().store(next, Ordering::Release);
        self.wait_write_ptr().fetch_add(1, Ordering::Release);
        sync::wake_one(self.wait_write_ptr())?;
        Ok(())
    }

    pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(()),
                Err(ChannelError::Full) => {
                    let wait_word = self.wait_read_ptr();
                    let expected = wait_word.load(Ordering::Acquire);
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    let next = tail.wrapping_add(1) & self.mask;
                    if next != head {
                        continue;
                    }
                    sync::wait_on_address(wait_word, expected)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn send_timeout(
        &self,
        payload: &[u8],
        flags: u32,
        timeout: Duration,
    ) -> Result<bool, ChannelError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(true),
                Err(ChannelError::Full) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(false);
                    }
                    let remaining = deadline - now;
                    let wait_word = self.wait_read_ptr();
                    let expected = wait_word.load(Ordering::Acquire);
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    let next = tail.wrapping_add(1) & self.mask;
                    if next != head {
                        continue;
                    }
                    if !sync::wait_on_address_timeout(wait_word, expected, remaining)? {
                        return Ok(false);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        let head = self.read_head().load(Ordering::Acquire);
        let tail = self.write_head().load(Ordering::Acquire);
        if head == tail {
            return Err(ChannelError::Empty);
        }
        let len = unsafe { self.read_slot(head, out)? };
        let next = head.wrapping_add(1) & self.mask;
        self.read_head().store(next, Ordering::Release);
        self.wait_read_ptr().fetch_add(1, Ordering::Release);
        sync::wake_one(self.wait_read_ptr())?;
        Ok(len)
    }

    pub fn try_recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        let head = self.read_head().load(Ordering::Acquire);
        let tail = self.write_head().load(Ordering::Acquire);
        if head == tail {
            return Err(ChannelError::Empty);
        }
        let (len, flags) = unsafe { self.read_slot_with_flags(head, out)? };
        let next = head.wrapping_add(1) & self.mask;
        self.read_head().store(next, Ordering::Release);
        self.wait_read_ptr().fetch_add(1, Ordering::Release);
        sync::wake_one(self.wait_read_ptr())?;
        Ok((len, flags))
    }

    pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(len),
                Err(ChannelError::Empty) => {
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    if head != tail {
                        continue;
                    }
                    sync::wait_on_address(wait_word, expected)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn recv_timeout(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<usize>, ChannelError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(Some(len)),
                Err(ChannelError::Empty) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(None);
                    }
                    let remaining = deadline - now;
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    if head != tail {
                        continue;
                    }
                    if !sync::wait_on_address_timeout(wait_word, expected, remaining)? {
                        return Ok(None);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        loop {
            match self.try_recv_with_flags(out) {
                Ok(result) => return Ok(result),
                Err(ChannelError::Empty) => {
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    if head != tail {
                        continue;
                    }
                    sync::wait_on_address(wait_word, expected)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn recv_timeout_with_flags(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<(usize, u32)>, ChannelError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.try_recv_with_flags(out) {
                Ok(result) => return Ok(Some(result)),
                Err(ChannelError::Empty) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(None);
                    }
                    let remaining = deadline - now;
                    let wait_word = self.wait_write_ptr();
                    let expected = wait_word.load(Ordering::Acquire);
                    let head = self.read_head().load(Ordering::Acquire);
                    let tail = self.write_head().load(Ordering::Acquire);
                    if head != tail {
                        continue;
                    }
                    if !sync::wait_on_address_timeout(wait_word, expected, remaining)? {
                        return Ok(None);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn read_head(&self) -> &core::sync::atomic::AtomicU64 {
        unsafe { &(*self.header).read_head.0 }
    }

    fn write_head(&self) -> &core::sync::atomic::AtomicU64 {
        unsafe { &(*self.header).write_head.0 }
    }

    fn wait_read_ptr(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).wait_read.0 }
    }

    fn wait_write_ptr(&self) -> &core::sync::atomic::AtomicU32 {
        unsafe { &(*self.header).wait_write.0 }
    }

    unsafe fn slot_ptr(&self, index: u64) -> *mut u8 {
        let offset = (index as usize & self.mask as usize) * self.layout.slot_size;
        self.data.add(offset)
    }

    unsafe fn write_slot(&self, index: u64, payload: &[u8], flags: u32) {
        let slot = self.slot_ptr(index);
        let header = slot.cast::<SlotHeader>();
        (*header).len = payload.len() as u32;
        (*header).flags = flags;
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
        ptr::copy_nonoverlapping(payload.as_ptr(), payload_ptr, payload.len());
    }

    unsafe fn read_slot(&self, index: u64, out: &mut [u8]) -> Result<usize, ChannelError> {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
        if len > out.len() {
            return Err(ChannelError::BufferTooSmall {
                len,
                capacity: out.len(),
            });
        }
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
        ptr::copy_nonoverlapping(payload_ptr, out.as_mut_ptr(), len);
        Ok(len)
    }

    unsafe fn read_slot_with_flags(
        &self,
        index: u64,
        out: &mut [u8],
    ) -> Result<(usize, u32), ChannelError> {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
        if len > out.len() {
            return Err(ChannelError::BufferTooSmall {
                len,
                capacity: out.len(),
            });
        }
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
        ptr::copy_nonoverlapping(payload_ptr, out.as_mut_ptr(), len);
        Ok((len, header.flags))
    }
}

#[derive(Debug)]
pub struct SpscSender {
    ring: Arc<SpscRing>,
}

impl SpscSender {
    pub fn payload_capacity(&self) -> usize {
        self.ring.payload_capacity()
    }

    pub fn stats(&self) -> RingStats {
        self.ring.stats()
    }

    /// Blocking send. Uses wait/wake to apply backpressure when full.
    pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        self.ring.send(payload, flags)
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        self.ring.try_send(payload, flags)
    }

    pub fn send_timeout(
        &self,
        payload: &[u8],
        flags: u32,
        timeout: Duration,
    ) -> Result<bool, ChannelError> {
        self.ring.send_timeout(payload, flags, timeout)
    }
}

#[derive(Debug)]
pub struct SpscReceiver {
    ring: Arc<SpscRing>,
}

impl SpscReceiver {
    pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        self.ring.recv(out)
    }

    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        self.ring.try_recv(out)
    }

    pub fn stats(&self) -> RingStats {
        self.ring.stats()
    }

    pub fn recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        self.ring.recv_with_flags(out)
    }

    pub fn try_recv_with_flags(&self, out: &mut [u8]) -> Result<(usize, u32), ChannelError> {
        self.ring.try_recv_with_flags(out)
    }

    pub fn recv_timeout(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<usize>, ChannelError> {
        self.ring.recv_timeout(out, timeout)
    }

    pub fn recv_timeout_with_flags(
        &self,
        out: &mut [u8],
        timeout: Duration,
    ) -> Result<Option<(usize, u32)>, ChannelError> {
        self.ring.recv_timeout_with_flags(out, timeout)
    }
}

#[derive(Debug)]
pub struct BufferMessage {
    pub buffer: Buffer,
    pub len: usize,
    pub flags: u32,
}

#[repr(C)]
#[derive(Copy, Clone)]
struct BufferMeta {
    index: u32,
    len: u32,
    flags: u32,
    _reserved: u32,
}

const BUFFER_META_BYTES: usize = core::mem::size_of::<BufferMeta>();

fn encode_buffer_meta(meta: BufferMeta) -> [u8; BUFFER_META_BYTES] {
    let mut out = [0u8; BUFFER_META_BYTES];
    out[0..4].copy_from_slice(&meta.index.to_le_bytes());
    out[4..8].copy_from_slice(&meta.len.to_le_bytes());
    out[8..12].copy_from_slice(&meta.flags.to_le_bytes());
    out[12..16].copy_from_slice(&meta._reserved.to_le_bytes());
    out
}

fn decode_buffer_meta(bytes: &[u8]) -> Result<BufferMeta, ChannelError> {
    if bytes.len() < BUFFER_META_BYTES {
        return Err(ChannelError::InvalidBufferMeta);
    }
    let index = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let flags = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let reserved = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    Ok(BufferMeta {
        index,
        len,
        flags,
        _reserved: reserved,
    })
}

#[derive(Debug)]
pub struct BufferSender {
    ring: Arc<SpscRing>,
    #[allow(dead_code)]
    pool: Arc<BufferPool>,
}

#[derive(Debug)]
pub struct BufferReceiver {
    ring: Arc<SpscRing>,
    pool: Arc<BufferPool>,
}

impl BufferSender {
    pub fn send(&self, buffer: Buffer, len: usize, flags: u32) -> Result<(), ChannelError> {
        let payload_capacity = buffer.len();
        if len > payload_capacity {
            return Err(ChannelError::PayloadTooLarge {
                len,
                capacity: payload_capacity,
            });
        }
        let index = buffer.index();
        let index_u32 = u32::try_from(index).map_err(|_| ChannelError::InvalidBufferMeta)?;
        let len_u32 = u32::try_from(len).map_err(|_| ChannelError::InvalidBufferMeta)?;
        let meta = BufferMeta {
            index: index_u32,
            len: len_u32,
            flags,
            _reserved: 0,
        };
        let payload = encode_buffer_meta(meta);
        let mut buffer = Some(buffer);
        self.ring.send(&payload, 0)?;
        if let Some(buffer) = buffer.take() {
            let _ = buffer.into_raw_index();
        }
        Ok(())
    }

    pub fn try_send(&self, buffer: Buffer, len: usize, flags: u32) -> Result<(), ChannelError> {
        let payload_capacity = buffer.len();
        if len > payload_capacity {
            return Err(ChannelError::PayloadTooLarge {
                len,
                capacity: payload_capacity,
            });
        }
        let index = buffer.index();
        let index_u32 = u32::try_from(index).map_err(|_| ChannelError::InvalidBufferMeta)?;
        let len_u32 = u32::try_from(len).map_err(|_| ChannelError::InvalidBufferMeta)?;
        let meta = BufferMeta {
            index: index_u32,
            len: len_u32,
            flags,
            _reserved: 0,
        };
        let payload = encode_buffer_meta(meta);
        let mut buffer = Some(buffer);
        match self.ring.try_send(&payload, 0) {
            Ok(()) => {
                if let Some(buffer) = buffer.take() {
                    let _ = buffer.into_raw_index();
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

impl BufferReceiver {
    pub fn recv(&self) -> Result<BufferMessage, ChannelError> {
        let mut payload = [0u8; BUFFER_META_BYTES];
        let len = self.ring.recv(&mut payload)?;
        if len != BUFFER_META_BYTES {
            return Err(ChannelError::InvalidBufferMeta);
        }
        let meta = decode_buffer_meta(&payload)?;
        let buffer = unsafe { self.pool.from_raw(meta.index as usize) }?;
        Ok(BufferMessage {
            buffer,
            len: meta.len as usize,
            flags: meta.flags,
        })
    }

    pub fn try_recv(&self) -> Result<Option<BufferMessage>, ChannelError> {
        let mut payload = [0u8; BUFFER_META_BYTES];
        match self.ring.try_recv(&mut payload) {
            Ok(len) => {
                if len != BUFFER_META_BYTES {
                    return Err(ChannelError::InvalidBufferMeta);
                }
                let meta = decode_buffer_meta(&payload)?;
                let buffer = unsafe { self.pool.from_raw(meta.index as usize) }?;
                Ok(Some(BufferMessage {
                    buffer,
                    len: meta.len as usize,
                    flags: meta.flags,
                }))
            }
            Err(ChannelError::Empty) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[derive(Debug)]
pub struct BufferChannel;

impl BufferChannel {
    pub fn split(
        pool: Arc<BufferPool>,
        slots: usize,
    ) -> Result<(BufferSender, BufferReceiver), ChannelError> {
        let slot_payload = BUFFER_META_BYTES;
        let slot_size = align_up(
            core::mem::size_of::<SlotHeader>() + slot_payload,
            SLOT_ALIGN,
        );
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::create(layout.total_size)?);
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0)? };
        let ring = Arc::new(ring);
        Ok((
            BufferSender {
                ring: Arc::clone(&ring),
                pool: Arc::clone(&pool),
            },
            BufferReceiver { ring, pool },
        ))
    }
}

#[derive(Debug)]
pub struct MpmcRing {
    #[allow(dead_code)]
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    sequences: *mut AtomicU64,
    data: *mut u8,
    layout: RingLayout,
    mask: u64,
}

unsafe impl Send for MpmcRing {}
unsafe impl Sync for MpmcRing {}

impl MpmcRing {
    /// Initialize a shared region in-place and return a ring handle.
    pub unsafe fn initialize(
        region: Arc<ShmRegion>,
        layout: RingLayout,
        creator_pid: u32,
        creator_epoch: u64,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two()
            || layout.slot_size < core::mem::size_of::<SlotHeader>()
            || layout.mode != ChannelMode::Mpmc
        {
            return Err(ChannelError::InvalidLayout);
        }
        if region.len() < layout.total_size {
            return Err(ChannelError::InvalidLayout);
        }

        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &mut *header;
        header_ref.magic = MAGIC;
        header_ref.abi_version = ABI_VERSION;
        header_ref.flags = 0;
        header_ref.total_size = layout.total_size as u64;
        header_ref.ring_offset = layout.ring_offset() as u64;
        header_ref.ring_size = layout.ring_size as u64;
        header_ref.generation.store(1, Ordering::Relaxed);
        header_ref
            .state
            .store(RegionState::Ready as u32, Ordering::Release);
        header_ref.creator_pid = creator_pid;
        header_ref.creator_epoch = creator_epoch;
        header_ref.read_head.0.store(0, Ordering::Relaxed);
        header_ref.write_head.0.store(0, Ordering::Relaxed);
        header_ref.wait_read.0.store(0, Ordering::Relaxed);
        header_ref.wait_write.0.store(0, Ordering::Relaxed);

        // Initialize sequences
        let sequences = region.as_ptr().add(layout.seq_offset()).cast::<AtomicU64>();
        for i in 0..layout.slots {
            (*sequences.add(i)).store(i as u64, Ordering::Relaxed);
        }

        Self::from_region(region, layout)
    }

    /// Attach to an existing shared region.
    pub unsafe fn attach(region: Arc<ShmRegion>, layout: RingLayout) -> Result<Self, ChannelError> {
        let header = region.as_ptr().cast::<ShmHeader>();
        let header_ref = &*header;
        if header_ref.magic != MAGIC || header_ref.abi_version != ABI_VERSION {
            return Err(ChannelError::NotReady);
        }
        if header_ref.state.load(Ordering::Acquire) != RegionState::Ready as u32 {
            return Err(ChannelError::NotReady);
        }
        if layout.mode != ChannelMode::Mpmc {
            return Err(ChannelError::InvalidLayout);
        }
        Self::from_region(region, layout)
    }

    pub unsafe fn from_region(
        region: Arc<ShmRegion>,
        layout: RingLayout,
    ) -> Result<Self, ChannelError> {
        if !layout.is_power_of_two() {
            return Err(ChannelError::InvalidLayout);
        }
        let header = region.as_ptr().cast::<ShmHeader>();
        let sequences = region.as_ptr().add(layout.seq_offset()).cast::<AtomicU64>();
        let data = region.as_ptr().add(layout.ring_offset());
        Ok(Self {
            region,
            header,
            sequences,
            data,
            layout,
            mask: (layout.slots as u64).wrapping_sub(1),
        })
    }

    /// Create a new anonymous MPMC ring.
    pub fn new(slot_size: usize, slots: usize) -> Result<Self, ChannelError> {
        let layout = RingLayout::new(slot_size, slots, ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::create(layout.total_size)?);
        unsafe { Self::initialize(region, layout, 0, 0) }
    }

    pub fn payload_capacity(&self) -> usize {
        self.layout.slot_size - core::mem::size_of::<SlotHeader>()
    }

    fn head(&self) -> &AtomicU64 {
        unsafe { &(*self.header).read_head.0 }
    }

    fn tail(&self) -> &AtomicU64 {
        unsafe { &(*self.header).write_head.0 }
    }

    fn sequence(&self, index: usize) -> &AtomicU64 {
        unsafe { &*self.sequences.add(index) }
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        if payload.len() > self.payload_capacity() {
            return Err(ChannelError::PayloadTooLarge {
                len: payload.len(),
                capacity: self.payload_capacity(),
            });
        }

        let backoff = Backoff::new();
        loop {
            let tail = self.tail().load(Ordering::Relaxed);
            let index = (tail & self.mask) as usize;
            let seq = self.sequence(index).load(Ordering::Acquire);
            let diff = seq as i64 - tail as i64;

            if diff == 0 {
                if self
                    .tail()
                    .compare_exchange(tail, tail + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        self.write_slot(index, payload, flags);
                    }
                    self.sequence(index).store(tail + 1, Ordering::Release);
                    return Ok(());
                }
            } else if diff < 0 {
                return Err(ChannelError::Full);
            } else {
                backoff.snooze();
            }
        }
    }

    pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        let backoff = Backoff::new();
        loop {
            match self.try_send(payload, flags) {
                Ok(()) => return Ok(()),
                Err(ChannelError::Full) => {
                    backoff.snooze();
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn try_recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        let backoff = Backoff::new();
        loop {
            let head = self.head().load(Ordering::Relaxed);
            let index = (head & self.mask) as usize;
            let seq = self.sequence(index).load(Ordering::Acquire);
            let diff = seq as i64 - (head + 1) as i64;

            if diff == 0 {
                if self
                    .head()
                    .compare_exchange(head, head + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    let len = unsafe { self.read_slot(index, out)? };
                    self.sequence(index)
                        .store(head + self.mask + 1, Ordering::Release);
                    return Ok(len);
                }
            } else if diff < 0 {
                return Err(ChannelError::Empty);
            } else {
                backoff.snooze();
            }
        }
    }

    pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
        let backoff = Backoff::new();
        loop {
            match self.try_recv(out) {
                Ok(len) => return Ok(len),
                Err(ChannelError::Empty) => backoff.snooze(),
                Err(err) => return Err(err),
            }
        }
    }

    unsafe fn slot_ptr(&self, index: usize) -> *mut u8 {
        let offset = index * self.layout.slot_size;
        self.data.add(offset)
    }

    unsafe fn write_slot(&self, index: usize, payload: &[u8], flags: u32) {
        let slot = self.slot_ptr(index);
        let header = slot.cast::<SlotHeader>();
        (*header).len = payload.len() as u32;
        (*header).flags = flags;
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
        ptr::copy_nonoverlapping(payload.as_ptr(), payload_ptr, payload.len());
    }

    unsafe fn read_slot(&self, index: usize, out: &mut [u8]) -> Result<usize, ChannelError> {
        let slot = self.slot_ptr(index);
        let header = &*slot.cast::<SlotHeader>();
        let len = header.len as usize;
        if len > out.len() {
            return Err(ChannelError::BufferTooSmall {
                len,
                capacity: out.len(),
            });
        }
        let payload_ptr = slot.add(core::mem::size_of::<SlotHeader>());
        ptr::copy_nonoverlapping(payload_ptr, out.as_mut_ptr(), len);
        Ok(len)
    }
}

fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::RingLayout;
    use proptest::prelude::*;
    use std::sync::Arc;

    #[test]
    fn spsc_round_trip() {
        let layout = RingLayout::new(256, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
        let payload = b"hello";
        ring.send(payload, 0).unwrap();
        let mut out = [0u8; 32];
        let len = ring.recv(&mut out).unwrap();
        assert_eq!(&out[..len], payload);
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_timeouts() {
        let layout = RingLayout::new(256, 2, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
        let payload = [1u8; 8];
        ring.send(&payload, 0).unwrap();
        let sent = ring
            .send_timeout(&payload, 0, std::time::Duration::from_millis(1))
            .unwrap();
        assert!(!sent);
        let mut out = [0u8; 8];
        ring.recv(&mut out).unwrap();
        let recv = ring
            .recv_timeout(&mut out, std::time::Duration::from_millis(1))
            .unwrap();
        assert!(recv.is_none());
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_full_empty_edges() {
        let layout = RingLayout::new(128, 2, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
        let payload = [1u8; 8];
        ring.try_send(&payload, 0).unwrap();
        assert!(matches!(
            ring.try_send(&payload, 0),
            Err(ChannelError::Full)
        ));
        let mut out = [0u8; 8];
        ring.try_recv(&mut out).unwrap();
        assert!(matches!(ring.try_recv(&mut out), Err(ChannelError::Empty)));
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn spsc_multi_threaded() {
        let layout = RingLayout::new(128, 8, ChannelMode::Spsc);
        let alloc_layout =
            std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                .unwrap();
        let ptr = unsafe { std::alloc::alloc(alloc_layout) };
        assert!(!ptr.is_null());
        let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
        let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
        let (tx, rx) = ring.split();
        std::thread::scope(|s| {
            s.spawn(|| {
                for i in 0u8..20 {
                    tx.send(&[i], 0).unwrap();
                }
            });
            s.spawn(|| {
                let mut out = [0u8; 8];
                for i in 0u8..20 {
                    let len = rx.recv(&mut out).unwrap();
                    assert_eq!(len, 1);
                    assert_eq!(out[0], i);
                }
            });
        });
        unsafe {
            std::alloc::dealloc(ptr, alloc_layout);
        }
    }

    #[test]
    fn buffer_channel_round_trip() {
        let pool = BufferPool::new(64, 2);
        let (tx, rx) = BufferChannel::split(Arc::clone(&pool), 8).unwrap();
        let mut buffer = pool.acquire().unwrap();
        buffer.as_mut_slice()[0..4].copy_from_slice(b"ping");
        tx.send(buffer, 4, 7).unwrap();
        let msg = rx.recv().unwrap();
        assert_eq!(msg.len, 4);
        assert_eq!(msg.flags, 7);
        assert_eq!(&msg.buffer.as_slice()[..4], b"ping");
    }

    #[test]
    fn mpmc_multi_threaded() {
        let ring = Arc::new(MpmcRing::new(64, 64).unwrap());
        let total = 200u32;
        let received = Arc::new(std::sync::Mutex::new(vec![false; total as usize]));
        std::thread::scope(|s| {
            for producer in 0..2u32 {
                let ring = Arc::clone(&ring);
                s.spawn(move || {
                    for i in 0..(total / 2) {
                        let value = producer * (total / 2) + i;
                        let payload = value.to_le_bytes();
                        ring.send(&payload, 0).unwrap();
                    }
                });
            }
            for _ in 0..2 {
                let ring = Arc::clone(&ring);
                let received = Arc::clone(&received);
                s.spawn(move || {
                    let mut out = [0u8; 8];
                    for _ in 0..(total / 2) {
                        let len = ring.recv(&mut out).unwrap();
                        assert_eq!(len, 4);
                        let value = u32::from_le_bytes(out[0..4].try_into().unwrap());
                        received.lock().unwrap()[value as usize] = true;
                    }
                });
            }
        });
        let received = received.lock().unwrap();
        assert!(received.iter().all(|&seen| seen));
    }

    proptest! {
        #[test]
        fn spsc_fifo_order(values in proptest::collection::vec(0u8..=255, 1..32)) {
            let layout = RingLayout::new(64, 64, ChannelMode::Spsc);
            let alloc_layout =
                std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                    .unwrap();
            let ptr = unsafe { std::alloc::alloc(alloc_layout) };
            let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
            let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
            for value in &values {
                ring.send(&[*value], 0).unwrap();
            }
            let mut out = [0u8; 8];
            for value in values {
                let len = ring.recv(&mut out).unwrap();
                prop_assert_eq!(len, 1);
                prop_assert_eq!(out[0], value);
            }
            unsafe {
                std::alloc::dealloc(ptr, alloc_layout);
            }
        }

        #[test]
        fn spsc_lossless(values in proptest::collection::vec(0u8..=255, 1..32)) {
            let layout = RingLayout::new(64, 64, ChannelMode::Spsc);
            let alloc_layout =
                std::alloc::Layout::from_size_align(layout.total_size, crate::buffer::CACHELINE_BYTES)
                    .unwrap();
            let ptr = unsafe { std::alloc::alloc(alloc_layout) };
            let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
            let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
            for value in &values {
                ring.send(&[*value], 0).unwrap();
            }
            let mut out = [0u8; 8];
            let mut received = Vec::new();
            for _ in 0..values.len() {
                let len = ring.recv(&mut out).unwrap();
                prop_assert_eq!(len, 1);
                received.push(out[0]);
            }
            prop_assert_eq!(received, values);
            unsafe {
                std::alloc::dealloc(ptr, alloc_layout);
            }
        }
    }
}
