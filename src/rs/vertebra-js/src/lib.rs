//! napi-rs bindings for Vertebra (Bun/Node.js).

use std::sync::Arc;

use napi::bindgen_prelude::{Buffer, Reference};
use napi::JsObject;
use napi::{Env, Error, Result};
use napi_derive::napi;

use vertebra_core::buffer::BufferPool;
use vertebra_core::buffer::RingLayout;
use vertebra_core::channel::{BufferChannel, BufferMessage, MpmcRing, SpscRing};
use vertebra_core::shm::ShmRegion;

fn map_err(err: impl std::fmt::Display) -> Error {
    Error::from_reason(err.to_string())
}

#[napi]
pub fn get_version() -> String {
    vertebra_core::VERSION.to_string()
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
        let layout = RingLayout::new(slot_size as usize, slots as usize);
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
