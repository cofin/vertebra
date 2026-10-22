//! Cache-line layout and alignment primitives.

#[cfg(target_arch = "aarch64")]
pub const CACHELINE_BYTES: usize = 128;
#[cfg(not(target_arch = "aarch64"))]
pub const CACHELINE_BYTES: usize = 64;

/// Cache-line-aligned wrapper to prevent false sharing across CPU cores.
#[repr(C, align(64))]
#[cfg_attr(target_arch = "aarch64", repr(align(128)))]
#[derive(Debug, Default)]
pub struct CacheLine<T>(pub T);

impl<T> CacheLine<T> {
    pub const fn new(val: T) -> Self {
        Self(val)
    }
}

impl<T> core::ops::Deref for CacheLine<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> core::ops::DerefMut for CacheLine<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
