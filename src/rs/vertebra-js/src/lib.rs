//! napi-rs bindings for Vertebra (Bun/Node.js).

use std::sync::Arc;

use napi::bindgen_prelude::{Buffer, Reference};
use napi::JsObject;
use napi::{Env, Error, Result};
use napi_derive::napi;

use vertebra_core::buffer::BufferPool;
use vertebra_core::buffer::RingLayout;
use vertebra_core::channel::{BufferChannel, BufferMessage, MpmcRing, SpscRing};
use vertebra_core::router::{RouteId, Router};
#[cfg(unix)]
use vertebra_core::shm::shm_unlink_named;
use vertebra_core::shm::ShmRegion;

fn map_err(err: impl std::fmt::Display) -> Error {
    Error::from_reason(err.to_string())
}

#[napi]
pub struct RouterHandle {
    inner: Router,
}

#[napi]
impl RouterHandle {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Router::new(),
        }
    }

    #[napi]
    pub fn insert(&mut self, path: String, id: u32) -> Result<()> {
        self.inner
            .insert(&path, RouteId(id as u64))
            .map_err(|e| map_err(e.to_string()))
    }

    #[napi]
    pub fn match_path(&self, path: String) -> Option<MatchResult> {
        self.inner
            .match_path(&path)
            .map(|(id, params)| MatchResult {
                id: id.0 as u32,
                params: params
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            })
    }
}

#[napi(object)]
pub struct MatchResult {
    pub id: u32,
    pub params: std::collections::HashMap<String, String>,
}

#[napi]
pub fn get_version() -> String {
    vertebra_core::VERSION.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_matches_core() {
        assert_eq!(get_version(), vertebra_core::VERSION.to_string());
    }

    #[test]
    fn shm_region_read_write() {
        let mut region = ShmRegionHandle::new(64).expect("create shm");
        let payload = Buffer::from(vec![1u8, 2, 3, 4]);
        region.write(0, payload).expect("write");
        let read = region.read(0, 4).expect("read");
        assert_eq!(read.as_ref(), &[1, 2, 3, 4]);
    }

    #[test]
    fn buffer_pool_roundtrip() {
        let pool = BufferPoolHandle::new(16, 1);
        let mut buffer = pool.acquire().expect("acquire").expect("buffer");
        buffer.write(Buffer::from(vec![9u8, 8, 7])).expect("write");
        let data = buffer.read().expect("read");
        assert_eq!(data.as_ref(), &[9, 8, 7]);
    }

    #[test]
    fn spsc_send_recv() {
        let ring = SpscRingHandle::new(64, 8).expect("ring");
        ring.send(Buffer::from(vec![5u8, 6, 7]), 0).expect("send");
        let recv = ring.recv(16).expect("recv");
        assert_eq!(recv.as_ref(), &[5, 6, 7]);
    }

    #[test]
    fn mpmc_send_recv() {
        let ring = MpmcRingHandle::new(64, 8).expect("ring");
        ring.send(Buffer::from(vec![1u8, 2]), 0).expect("send");
        let recv = ring.recv(16).expect("recv");
        assert_eq!(recv.as_ref(), &[1, 2]);
    }

    // Note: ChannelHandle tests require JS runtime (Env), tested via integration tests
}

#[napi]
pub struct ShmRegionHandle {
    inner: ShmRegion,
}

#[napi]
impl ShmRegionHandle {
    #[napi(constructor)]
    pub fn new(size: u32) -> Result<Self> {
        let inner = ShmRegion::create(size as usize).map_err(map_err)?;
        Ok(Self { inner })
    }

    #[napi]
    pub fn len(&self) -> u32 {
        self.inner.len() as u32
    }

    #[napi]
    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    #[napi]
    pub fn read(&self, offset: u32, len: u32) -> Result<Buffer> {
        let slice = unsafe { self.inner.as_slice() };
        let end = (offset as usize)
            .checked_add(len as usize)
            .ok_or_else(|| map_err("invalid read range"))?;
        if end > slice.len() {
            return Err(map_err("read out of bounds"));
        }
        Ok(Buffer::from(&slice[offset as usize..end]))
    }

    #[napi]
    pub fn write(&mut self, offset: u32, data: Buffer) -> Result<()> {
        let slice = unsafe { self.inner.as_mut_slice() };
        let end = (offset as usize)
            .checked_add(data.len())
            .ok_or_else(|| map_err("invalid write range"))?;
        if end > slice.len() {
            return Err(map_err("write out of bounds"));
        }
        slice[offset as usize..end].copy_from_slice(&data);
        Ok(())
    }
}

#[napi]
pub struct PoolBuffer {
    inner: Option<vertebra_core::buffer::Buffer>,
}

impl PoolBuffer {
    fn inner_ref(&self) -> Result<&vertebra_core::buffer::Buffer> {
        self.inner
            .as_ref()
            .ok_or_else(|| map_err("buffer is closed"))
    }

    fn inner_mut(&mut self) -> Result<&mut vertebra_core::buffer::Buffer> {
        self.inner
            .as_mut()
            .ok_or_else(|| map_err("buffer is closed"))
    }

    fn take_inner(&mut self) -> Result<vertebra_core::buffer::Buffer> {
        self.inner.take().ok_or_else(|| map_err("buffer is closed"))
    }
}

#[napi]
impl PoolBuffer {
    #[napi]
    pub fn len(&self) -> Result<u32> {
        Ok(self.inner_ref()?.len() as u32)
    }

    #[napi]
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.inner_ref()?.len() == 0)
    }

    #[napi]
    pub fn index(&self) -> Result<u32> {
        Ok(self.inner_ref()?.index() as u32)
    }

    #[napi]
    pub fn read(&self) -> Result<Buffer> {
        Ok(Buffer::from(self.inner_ref()?.as_slice()))
    }

    #[napi]
    pub fn write(&mut self, data: Buffer) -> Result<()> {
        let slice = self.inner_mut()?.as_mut_slice();
        if data.len() > slice.len() {
            return Err(map_err("data too large"));
        }
        slice[..data.len()].copy_from_slice(&data);
        Ok(())
    }
}

#[napi]
pub struct BufferPoolHandle {
    inner: Arc<BufferPool>,
}

#[napi]
impl BufferPoolHandle {
    #[napi(constructor)]
    pub fn new(buffer_size: u32, count: u32) -> Self {
        Self {
            inner: BufferPool::new(buffer_size as usize, count as usize),
        }
    }

    #[napi]
    pub fn acquire(&self) -> Result<Option<PoolBuffer>> {
        match self.inner.acquire() {
            Ok(buffer) => Ok(Some(PoolBuffer {
                inner: Some(buffer),
            })),
            Err(_) => Ok(None),
        }
    }

    #[napi]
    pub fn available(&self) -> u32 {
        self.inner.available() as u32
    }
}

#[napi]
pub struct SpscRingHandle {
    inner: SpscRing,
}

#[napi]
impl SpscRingHandle {
    #[napi(constructor)]
    pub fn new(slot_size: u32, slots: u32) -> Result<Self> {
        let layout = RingLayout::new(
            slot_size as usize,
            slots as usize,
            vertebra_core::buffer::ChannelMode::Spsc,
        );
        let region = Arc::new(ShmRegion::create(layout.total_size).map_err(map_err)?);
        let ring =
            unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0) }.map_err(map_err)?;
        Ok(Self { inner: ring })
    }

    #[napi]
    pub fn send(&self, data: Buffer, flags: u32) -> Result<()> {
        self.inner.send(&data, flags).map_err(map_err)
    }

    #[napi]
    pub fn try_send(&self, data: Buffer, flags: u32) -> Result<()> {
        self.inner.try_send(&data, flags).map_err(map_err)
    }

    #[napi]
    pub fn recv(&self, max_len: u32) -> Result<Buffer> {
        let mut buffer = vec![0u8; max_len as usize];
        let len = self.inner.recv(&mut buffer).map_err(map_err)?;
        buffer.truncate(len);
        Ok(Buffer::from(buffer))
    }
}

#[napi]
pub struct MpmcRingHandle {
    inner: Arc<MpmcRing>,
}

#[napi]
impl MpmcRingHandle {
    #[napi(constructor)]
    pub fn new(slot_size: u32, slots: u32) -> Result<Self> {
        let inner = MpmcRing::new(slot_size as usize, slots as usize).map_err(map_err)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[napi]
    pub fn send(&self, data: Buffer, flags: u32) -> Result<()> {
        self.inner.send(&data, flags).map_err(map_err)
    }

    #[napi]
    pub fn recv(&self, max_len: u32) -> Result<Buffer> {
        let mut buffer = vec![0u8; max_len as usize];
        let len = self.inner.recv(&mut buffer).map_err(map_err)?;
        buffer.truncate(len);
        Ok(Buffer::from(buffer))
    }
}

/// Role in a bidirectional channel.
#[napi]
pub enum ChannelRole {
    Client,
    Server,
}

/// Bidirectional channel with paired send/recv rings for request/response patterns.
///
/// Use `createChannelPair()` to get two endpoints where one's send goes to the other's recv.
#[napi]
pub struct ChannelHandle {
    send_ring: SpscRing,
    recv_ring: SpscRing,
    /// Name for named channels (used for unlink).
    #[cfg(unix)]
    channel_name: Option<String>,
}

/// Create a pair of connected channels. Returns [client, server].
/// What client sends, server receives, and vice versa.
#[napi]
pub fn create_channel_pair(env: Env, slot_size: u32, slots: u32) -> Result<JsObject> {
    let layout = RingLayout::new(
        slot_size as usize,
        slots as usize,
        vertebra_core::buffer::ChannelMode::Spsc,
    );
    let req_region = Arc::new(ShmRegion::create(layout.total_size).map_err(map_err)?);
    let resp_region = Arc::new(ShmRegion::create(layout.total_size).map_err(map_err)?);

    let req_client =
        unsafe { SpscRing::initialize(Arc::clone(&req_region), layout, 0, 0) }.map_err(map_err)?;
    let req_server =
        unsafe { SpscRing::attach(Arc::clone(&req_region), layout) }.map_err(map_err)?;
    let resp_client =
        unsafe { SpscRing::initialize(Arc::clone(&resp_region), layout, 0, 0) }.map_err(map_err)?;
    let resp_server =
        unsafe { SpscRing::attach(Arc::clone(&resp_region), layout) }.map_err(map_err)?;

    let client = ChannelHandle {
        send_ring: req_client,
        recv_ring: resp_client,
        #[cfg(unix)]
        channel_name: None,
    };
    let server = ChannelHandle {
        send_ring: resp_server,
        recv_ring: req_server,
        #[cfg(unix)]
        channel_name: None,
    };

    let mut obj = env.create_object()?;
    obj.set("client", client)?;
    obj.set("server", server)?;
    Ok(obj)
}

#[napi]
impl ChannelHandle {
    /// Create a named channel (Unix only).
    #[cfg(unix)]
    #[napi(factory)]
    pub fn create_named(
        name: String,
        slot_size: u32,
        slots: u32,
        role: ChannelRole,
    ) -> Result<Self> {
        let layout = RingLayout::new(
            slot_size as usize,
            slots as usize,
            vertebra_core::buffer::ChannelMode::Spsc,
        );
        let req_name = format!("{name}.req");
        let resp_name = format!("{name}.resp");
        let req_region =
            Arc::new(ShmRegion::create_named(&req_name, layout.total_size).map_err(map_err)?);
        let resp_region =
            Arc::new(ShmRegion::create_named(&resp_name, layout.total_size).map_err(map_err)?);
        let req_ring = unsafe { SpscRing::initialize(Arc::clone(&req_region), layout, 0, 0) }
            .map_err(map_err)?;
        let resp_ring = unsafe { SpscRing::initialize(Arc::clone(&resp_region), layout, 0, 0) }
            .map_err(map_err)?;

        match role {
            ChannelRole::Client => Ok(Self {
                send_ring: req_ring,
                recv_ring: resp_ring,
                channel_name: Some(name),
            }),
            ChannelRole::Server => Ok(Self {
                send_ring: resp_ring,
                recv_ring: req_ring,
                channel_name: Some(name),
            }),
        }
    }

    /// Open an existing named channel (Unix only).
    #[cfg(unix)]
    #[napi(factory)]
    pub fn open_named(name: String, slot_size: u32, slots: u32, role: ChannelRole) -> Result<Self> {
        let layout = RingLayout::new(
            slot_size as usize,
            slots as usize,
            vertebra_core::buffer::ChannelMode::Spsc,
        );
        let req_name = format!("{name}.req");
        let resp_name = format!("{name}.resp");
        let req_region =
            Arc::new(ShmRegion::open_named(&req_name, layout.total_size).map_err(map_err)?);
        let resp_region =
            Arc::new(ShmRegion::open_named(&resp_name, layout.total_size).map_err(map_err)?);
        let req_ring =
            unsafe { SpscRing::attach(Arc::clone(&req_region), layout) }.map_err(map_err)?;
        let resp_ring =
            unsafe { SpscRing::attach(Arc::clone(&resp_region), layout) }.map_err(map_err)?;

        match role {
            ChannelRole::Client => Ok(Self {
                send_ring: req_ring,
                recv_ring: resp_ring,
                channel_name: Some(name),
            }),
            ChannelRole::Server => Ok(Self {
                send_ring: resp_ring,
                recv_ring: req_ring,
                channel_name: Some(name),
            }),
        }
    }

    /// Get the maximum payload size per message.
    #[napi]
    pub fn payload_capacity(&self) -> u32 {
        self.send_ring.payload_capacity() as u32
    }

    /// Send data (blocking).
    #[napi]
    pub fn send(&self, data: Buffer, flags: u32) -> Result<()> {
        self.send_ring.send(&data, flags).map_err(map_err)
    }

    /// Try to send data without blocking. Returns true if sent.
    #[napi]
    pub fn try_send(&self, data: Buffer, flags: u32) -> Result<bool> {
        match self.send_ring.try_send(&data, flags) {
            Ok(()) => Ok(true),
            Err(vertebra_core::channel::ChannelError::Full) => Ok(false),
            Err(err) => Err(map_err(err)),
        }
    }

    /// Send with timeout in milliseconds. Returns true if sent.
    #[napi]
    pub fn send_timeout(&self, data: Buffer, flags: u32, timeout_ms: u32) -> Result<bool> {
        self.send_ring
            .send_timeout(
                &data,
                flags,
                std::time::Duration::from_millis(timeout_ms as u64),
            )
            .map_err(map_err)
    }

    /// Receive data (blocking).
    #[napi]
    pub fn recv(&self, max_len: u32) -> Result<Buffer> {
        let mut buffer = vec![0u8; max_len as usize];
        let len = self.recv_ring.recv(&mut buffer).map_err(map_err)?;
        buffer.truncate(len);
        Ok(Buffer::from(buffer))
    }

    /// Try to receive data without blocking. Returns null if empty.
    #[napi]
    pub fn try_recv(&self, max_len: u32) -> Result<Option<Buffer>> {
        let mut buffer = vec![0u8; max_len as usize];
        match self.recv_ring.try_recv(&mut buffer) {
            Ok(len) => {
                buffer.truncate(len);
                Ok(Some(Buffer::from(buffer)))
            }
            Err(vertebra_core::channel::ChannelError::Empty) => Ok(None),
            Err(err) => Err(map_err(err)),
        }
    }

    /// Receive with timeout in milliseconds. Returns null if timeout.
    #[napi]
    pub fn recv_timeout(&self, max_len: u32, timeout_ms: u32) -> Result<Option<Buffer>> {
        let mut buffer = vec![0u8; max_len as usize];
        match self.recv_ring.recv_timeout(
            &mut buffer,
            std::time::Duration::from_millis(timeout_ms as u64),
        ) {
            Ok(Some(len)) => {
                buffer.truncate(len);
                Ok(Some(Buffer::from(buffer)))
            }
            Ok(None) => Ok(None), // timeout
            Err(err) => Err(map_err(err)),
        }
    }

    /// Get the channel name (if named).
    #[cfg(unix)]
    #[napi(getter)]
    pub fn name(&self) -> Option<String> {
        self.channel_name.clone()
    }

    /// Unlink the named shared memory regions (Unix only).
    ///
    /// This removes the names from the filesystem so no new processes can open them.
    /// Existing mappings remain valid. Only call this once, typically from the creator.
    #[cfg(unix)]
    #[napi]
    pub fn unlink(&self) -> Result<()> {
        if let Some(ref name) = self.channel_name {
            let req_name = format!("{name}.req");
            let resp_name = format!("{name}.resp");
            // Unlink both regions, ignoring ENOENT if already unlinked
            let _ = shm_unlink_named(&req_name);
            let _ = shm_unlink_named(&resp_name);
        }
        Ok(())
    }
}

#[napi]
pub struct BufferChannelHandle {
    pool: Arc<BufferPool>,
    sender: vertebra_core::channel::BufferSender,
    receiver: vertebra_core::channel::BufferReceiver,
}

#[napi]
impl BufferChannelHandle {
    #[napi(constructor)]
    pub fn new(buffer_size: u32, count: u32, slots: u32) -> Result<Self> {
        let pool = BufferPool::new(buffer_size as usize, count as usize);
        let (sender, receiver) =
            BufferChannel::split(Arc::clone(&pool), slots as usize).map_err(map_err)?;
        Ok(Self {
            pool,
            sender,
            receiver,
        })
    }

    #[napi]
    pub fn acquire(&self) -> Result<Option<PoolBuffer>> {
        match self.pool.acquire() {
            Ok(buffer) => Ok(Some(PoolBuffer {
                inner: Some(buffer),
            })),
            Err(_) => Ok(None),
        }
    }

    #[napi]
    pub fn send(&self, mut buffer: Reference<PoolBuffer>, len: u32, flags: u32) -> Result<()> {
        let inner = buffer.take_inner()?;
        self.sender
            .send(inner, len as usize, flags)
            .map_err(map_err)
    }

    #[napi]
    pub fn recv(&self, env: Env) -> Result<JsObject> {
        let BufferMessage { buffer, len, flags } = self.receiver.recv().map_err(map_err)?;
        let mut obj = env.create_object()?;
        obj.set(
            "buffer",
            PoolBuffer {
                inner: Some(buffer),
            },
        )?;
        obj.set("len", len as u32)?;
        obj.set("flags", flags)?;
        Ok(obj)
    }
}
