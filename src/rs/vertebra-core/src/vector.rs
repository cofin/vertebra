//! Shared-memory vector slab, seqlock vector cache, SIMD kernels, Gemma prompt splicer,
//! static token embedder, and compact Protobuf v3 gRPC embedding wire helpers.

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::shm::ShmRegion;

pub const VECTOR_SLAB_MAGIC: [u8; 8] = *b"VTBVSLAB";
pub const VECTOR_SLAB_VERSION: u32 = 1;
pub const VECTOR_SLAB_HEADER_BYTES: usize = 64;
pub const VECTOR_FLAG_L2_NORMALIZED: u8 = 1 << 0;

pub const VECTOR_CACHE_MAGIC: u64 = u64::from_le_bytes(*b"VTBVCACH");
pub const VECTOR_CACHE_VERSION: u32 = 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorDtype {
    Float32 = 1,
    Float64 = 2,
    Float16 = 3,
    Int8 = 4,
    Binary = 5,
}

impl VectorDtype {
    #[inline]
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            1 => Some(Self::Float32),
            2 => Some(Self::Float64),
            3 => Some(Self::Float16),
            4 => Some(Self::Int8),
            5 => Some(Self::Binary),
            _ => None,
        }
    }

    #[inline]
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "float32" | "f32" | "float" => Some(Self::Float32),
            "float64" | "f64" | "double" => Some(Self::Float64),
            "float16" | "f16" | "half" => Some(Self::Float16),
            "int8" | "i8" => Some(Self::Int8),
            "binary" | "bit" | "ubinary" | "u1" => Some(Self::Binary),
            _ => None,
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Float16 => "float16",
            Self::Int8 => "int8",
            Self::Binary => "binary",
        }
    }

    #[inline]
    pub const fn elem_bytes(self) -> usize {
        match self {
            Self::Float32 => 4,
            Self::Float64 => 8,
            Self::Float16 => 2,
            Self::Int8 => 1,
            Self::Binary => 1,
        }
    }

    #[inline]
    pub const fn packed_row_bytes(self, dimensions: usize) -> usize {
        match self {
            Self::Binary => dimensions.div_ceil(8),
            _ => dimensions.saturating_mul(self.elem_bytes()),
        }
    }
}

#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct VectorSlabHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub dtype: u8,
    pub flags: u8,
    pub reserved: u16,
    pub count: u32,
    pub dimensions: u32,
    pub row_stride_bytes: u32,
    pub scales_offset: u64,
    pub data_offset: u64,
    pub total_bytes: u64,
    pub _pad: [u8; 8],
}

const _: () = assert!(std::mem::size_of::<VectorSlabHeader>() == 64);
const _: () = assert!(std::mem::align_of::<VectorSlabHeader>() == 64);

#[derive(Debug)]
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: `AlignedBuffer` owns its heap allocation exclusively via `NonNull<u8>`
// and only exposes shared/mutable access through `&self` / `&mut self` (or atomic
// seqlock operations in `VectorCache`), making cross-thread transfer and sharing safe.
unsafe impl Send for AlignedBuffer {}
// SAFETY: Shared references to `AlignedBuffer` only allow read access or atomic
// seqlock access to the underlying aligned byte allocation.
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    pub fn new_zeroed(size: usize) -> Result<Self, String> {
        let alloc_size = size.max(64).next_multiple_of(64);
        let layout = Layout::from_size_align(alloc_size, 64)
            .map_err(|e| format!("invalid aligned buffer layout: {e}"))?;
        // SAFETY: `alloc_size >= 64` is non-zero and a multiple of alignment `64`.
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr =
            NonNull::new(raw).ok_or_else(|| "aligned buffer allocation failed".to_string())?;
        Ok(Self { ptr, layout })
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.layout.size()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.layout.size() == 0
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` was allocated with `alloc_zeroed(self.layout)` in
        // `AlignedBuffer::new_zeroed` and is deallocated exactly once on drop.
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

#[derive(Debug)]
enum SlabBacking {
    Memory(AlignedBuffer),
    Shm {
        region: ShmRegion,
        name: Option<String>,
    },
}

#[derive(Debug)]
pub struct VectorSlab {
    backing: SlabBacking,
    count: usize,
    dimensions: usize,
    dtype: VectorDtype,
    row_stride_bytes: usize,
    packed_row_bytes: usize,
    data_offset: usize,
    scales_offset: usize,
    total_bytes: usize,
}

impl VectorSlab {
    pub fn compute_layout(
        count: usize,
        dimensions: usize,
        dtype: VectorDtype,
        padded_rows: bool,
    ) -> Result<(usize, usize, usize, usize, usize), String> {
        if dimensions == 0 {
            return Err("vector dimensions must be > 0".to_string());
        }
        let packed_row_bytes = match dtype {
            VectorDtype::Binary => dimensions.div_ceil(8),
            _ => dimensions
                .checked_mul(dtype.elem_bytes())
                .ok_or_else(|| "vector row byte size overflow".to_string())?,
        };
        let row_stride_bytes = if padded_rows {
            packed_row_bytes
                .checked_next_multiple_of(64)
                .ok_or_else(|| "vector row stride overflow".to_string())?
        } else {
            packed_row_bytes
        };
        let data_offset = VECTOR_SLAB_HEADER_BYTES;
        let data_bytes = count
            .checked_mul(row_stride_bytes)
            .ok_or_else(|| "vector slab byte size overflow".to_string())?;
        let data_end = data_offset
            .checked_add(data_bytes)
            .ok_or_else(|| "vector slab data size overflow".to_string())?;
        let scales_offset = if dtype == VectorDtype::Int8 && count > 0 {
            data_end
                .checked_next_multiple_of(64)
                .ok_or_else(|| "vector slab scales offset overflow".to_string())?
        } else {
            0
        };
        let total_bytes = if scales_offset > 0 {
            let scales_bytes = count
                .checked_mul(4)
                .ok_or_else(|| "vector slab scales overflow".to_string())?;
            scales_offset
                .checked_add(scales_bytes)
                .ok_or_else(|| "vector slab scales overflow".to_string())?
                .checked_next_multiple_of(64)
                .ok_or_else(|| "vector slab total size overflow".to_string())?
        } else {
            data_end
                .max(64)
                .checked_next_multiple_of(64)
                .ok_or_else(|| "vector slab total size overflow".to_string())?
        };
        Ok((
            packed_row_bytes,
            row_stride_bytes,
            data_offset,
            scales_offset,
            total_bytes,
        ))
    }

    pub fn new_in_memory(
        count: usize,
        dimensions: usize,
        dtype: VectorDtype,
        padded_rows: bool,
    ) -> Result<Self, String> {
        let (packed_row_bytes, row_stride_bytes, data_offset, scales_offset, total_bytes) =
            Self::compute_layout(count, dimensions, dtype, padded_rows)?;
        let buf = AlignedBuffer::new_zeroed(total_bytes)?;
        let mut slab = Self {
            backing: SlabBacking::Memory(buf),
            count,
            dimensions,
            dtype,
            row_stride_bytes,
            packed_row_bytes,
            data_offset,
            scales_offset,
            total_bytes,
        };
        slab.write_header(0);
        Ok(slab)
    }

    pub fn create_shm(
        name: Option<&str>,
        count: usize,
        dimensions: usize,
        dtype: VectorDtype,
        padded_rows: bool,
    ) -> Result<Self, String> {
        let (packed_row_bytes, row_stride_bytes, data_offset, scales_offset, total_bytes) =
            Self::compute_layout(count, dimensions, dtype, padded_rows)?;
        let (region, resolved_name) = match name {
            #[cfg(unix)]
            Some(explicit_name) => {
                let mut reg = ShmRegion::create_named(explicit_name, total_bytes).map_err(|e| {
                    format!("failed to create named SHM slab '{explicit_name}': {e}")
                })?;
                reg.disarm_unlink();
                (reg, Some(explicit_name.to_string()))
            }
            #[cfg(not(unix))]
            Some(explicit_name) => {
                let reg = ShmRegion::create(total_bytes)
                    .map_err(|e| format!("failed to create SHM slab: {e}"))?;
                (reg, Some(explicit_name.to_string()))
            }
            None => {
                #[cfg(unix)]
                {
                    let (auto_name, reg) = ShmRegion::create_overflow_region("vslab", total_bytes)
                        .map_err(|e| format!("failed to create SHM vector slab: {e}"))?;
                    (reg, Some(auto_name))
                }
                #[cfg(not(unix))]
                {
                    let reg = ShmRegion::create(total_bytes)
                        .map_err(|e| format!("failed to create SHM vector slab: {e}"))?;
                    (reg, None)
                }
            }
        };
        // SAFETY: `region` was newly created with at least `total_bytes` mapped writable bytes.
        unsafe {
            std::ptr::write_bytes(region.as_ptr(), 0, total_bytes);
        }
        let mut slab = Self {
            backing: SlabBacking::Shm {
                region,
                name: resolved_name,
            },
            count,
            dimensions,
            dtype,
            row_stride_bytes,
            packed_row_bytes,
            data_offset,
            scales_offset,
            total_bytes,
        };
        slab.write_header(0);
        Ok(slab)
    }

    #[cfg(unix)]
    pub fn open_shm(name: &str, min_size: usize) -> Result<Self, String> {
        let open_size = min_size.max(4096);
        let region = ShmRegion::open_named(name, open_size)
            .map_err(|e| format!("failed to open SHM vector slab '{name}': {e}"))?;
        // SAFETY: `open_size >= 4096 >= size_of::<VectorSlabHeader>()` (64 bytes) and page-aligned.
        let hdr = unsafe { &*(region.as_ptr() as *const VectorSlabHeader) };
        if hdr.magic != VECTOR_SLAB_MAGIC {
            return Err("invalid VectorSlab magic header".to_string());
        }
        if hdr.version != VECTOR_SLAB_VERSION {
            return Err(format!("unsupported VectorSlab version {}", hdr.version));
        }
        let dtype = VectorDtype::from_u8(hdr.dtype)
            .ok_or_else(|| format!("unknown VectorDtype code {}", hdr.dtype))?;
        let count = hdr.count as usize;
        let dimensions = hdr.dimensions as usize;
        let row_stride_bytes = hdr.row_stride_bytes as usize;
        let packed_row_bytes = dtype.packed_row_bytes(dimensions);
        let data_offset = hdr.data_offset as usize;
        let scales_offset = hdr.scales_offset as usize;
        let total_bytes = hdr.total_bytes as usize;
        if dimensions == 0
            || total_bytes < VECTOR_SLAB_HEADER_BYTES
            || data_offset < VECTOR_SLAB_HEADER_BYTES
            || row_stride_bytes < packed_row_bytes
        {
            return Err("invalid VectorSlab header dimensions or offsets".to_string());
        }
        let data_bytes = count
            .checked_mul(row_stride_bytes)
            .ok_or_else(|| "VectorSlab header data size overflow".to_string())?;
        let data_end = data_offset
            .checked_add(data_bytes)
            .ok_or_else(|| "VectorSlab header data end overflow".to_string())?;
        if data_end > total_bytes {
            return Err("VectorSlab header data exceeds total_bytes".to_string());
        }
        if dtype == VectorDtype::Float32
            && (!data_offset.is_multiple_of(4) || !row_stride_bytes.is_multiple_of(4))
        {
            return Err("VectorSlab Float32 data offset or stride is unaligned".to_string());
        }
        if scales_offset > 0 {
            let scales_bytes = count
                .checked_mul(4)
                .ok_or_else(|| "VectorSlab header scales size overflow".to_string())?;
            let scales_end = scales_offset
                .checked_add(scales_bytes)
                .ok_or_else(|| "VectorSlab header scales end overflow".to_string())?;
            if scales_offset < VECTOR_SLAB_HEADER_BYTES
                || !scales_offset.is_multiple_of(4)
                || scales_end > total_bytes
            {
                return Err("VectorSlab header scales region is invalid".to_string());
            }
        }
        let final_region = if total_bytes > region.len() {
            drop(region);
            let reopened = ShmRegion::open_named(name, total_bytes)
                .map_err(|e| format!("failed to reopen full SHM vector slab '{name}': {e}"))?;
            if reopened.len() < total_bytes {
                return Err("reopened SHM vector slab shorter than total_bytes".to_string());
            }
            reopened
        } else {
            region
        };
        Ok(Self {
            backing: SlabBacking::Shm {
                region: final_region,
                name: Some(name.to_string()),
            },
            count,
            dimensions,
            dtype,
            row_stride_bytes,
            packed_row_bytes,
            data_offset,
            scales_offset,
            total_bytes,
        })
    }

    pub fn from_f32_slice(
        data: &[f32],
        count: usize,
        dimensions: usize,
        padded_rows: bool,
    ) -> Result<Self, String> {
        let expected = count
            .checked_mul(dimensions)
            .ok_or_else(|| "vector dimensions overflow".to_string())?;
        if data.len() != expected {
            return Err(format!(
                "expected {expected} f32 values for ({count}, {dimensions}), got {}",
                data.len()
            ));
        }
        let mut slab = Self::new_in_memory(count, dimensions, VectorDtype::Float32, padded_rows)?;
        for i in 0..count {
            let src = &data[i * dimensions..(i + 1) * dimensions];
            slab.row_f32_mut(i)?.copy_from_slice(src);
        }
        Ok(slab)
    }

    fn write_header(&mut self, flags: u8) {
        let hdr = VectorSlabHeader {
            magic: VECTOR_SLAB_MAGIC,
            version: VECTOR_SLAB_VERSION,
            dtype: self.dtype as u8,
            flags,
            reserved: 0,
            count: self.count as u32,
            dimensions: self.dimensions as u32,
            row_stride_bytes: self.row_stride_bytes as u32,
            scales_offset: self.scales_offset as u64,
            data_offset: self.data_offset as u64,
            total_bytes: self.total_bytes as u64,
            _pad: [0u8; 8],
        };
        // SAFETY: `self.base_mut_ptr()` is 64-byte aligned and points to at least
        // `VECTOR_SLAB_HEADER_BYTES` (64 bytes) of writable backing storage.
        unsafe {
            std::ptr::write(self.base_mut_ptr() as *mut VectorSlabHeader, hdr);
        }
    }

    #[inline]
    pub fn base_ptr(&self) -> *const u8 {
        match &self.backing {
            SlabBacking::Memory(buf) => buf.as_ptr(),
            SlabBacking::Shm { region, .. } => region.as_ptr(),
        }
    }

    #[inline]
    pub fn base_mut_ptr(&mut self) -> *mut u8 {
        match &mut self.backing {
            SlabBacking::Memory(buf) => buf.as_mut_ptr(),
            SlabBacking::Shm { region, .. } => region.as_ptr(),
        }
    }

    #[inline]
    pub fn data_ptr(&self) -> *const u8 {
        // SAFETY: `self.data_offset <= self.total_bytes` within the slab backing allocation.
        unsafe { self.base_ptr().add(self.data_offset) }
    }

    #[inline]
    pub fn data_mut_ptr(&mut self) -> *mut u8 {
        // SAFETY: `self.data_offset <= self.total_bytes` within the mutable slab backing allocation.
        unsafe { self.base_mut_ptr().add(self.data_offset) }
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    #[inline]
    pub fn dtype(&self) -> VectorDtype {
        self.dtype
    }

    #[inline]
    pub fn row_stride_bytes(&self) -> usize {
        self.row_stride_bytes
    }

    #[inline]
    pub fn packed_row_bytes(&self) -> usize {
        self.packed_row_bytes
    }

    #[inline]
    pub fn data_byte_len(&self) -> usize {
        if self.count == 0 {
            0
        } else {
            (self.count - 1) * self.row_stride_bytes + self.packed_row_bytes
        }
    }

    #[inline]
    pub fn shm_name(&self) -> Option<&str> {
        match &self.backing {
            SlabBacking::Memory(_) => None,
            SlabBacking::Shm { name, .. } => name.as_deref(),
        }
    }

    #[inline]
    pub fn row_ptr(&self, row: usize) -> Result<*const u8, String> {
        if row >= self.count {
            return Err(format!(
                "row index {row} out of bounds for VectorSlab of count {}",
                self.count
            ));
        }
        // SAFETY: `row < self.count`, so `row * self.row_stride_bytes` is within the data region.
        Ok(unsafe { self.data_ptr().add(row * self.row_stride_bytes) })
    }

    #[inline]
    pub fn row_bytes(&self, row: usize) -> Result<&[u8], String> {
        let ptr = self.row_ptr(row)?;
        // SAFETY: `ptr` points to a valid row of `self.packed_row_bytes` initialized bytes.
        Ok(unsafe { std::slice::from_raw_parts(ptr, self.packed_row_bytes) })
    }

    #[inline]
    pub fn row_bytes_mut(&mut self, row: usize) -> Result<&mut [u8], String> {
        if row >= self.count {
            return Err(format!(
                "row index {row} out of bounds for VectorSlab of count {}",
                self.count
            ));
        }
        // SAFETY: `row < self.count`, so the row offset and `self.packed_row_bytes` length
        // lie within the exclusively borrowed slab data region.
        let ptr = unsafe { self.data_mut_ptr().add(row * self.row_stride_bytes) };
        // SAFETY: `ptr` is valid for `self.packed_row_bytes` mutable bytes tied to `&mut self`.
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr, self.packed_row_bytes) })
    }

    #[inline]
    pub fn row_f32(&self, row: usize) -> Result<&[f32], String> {
        if self.dtype != VectorDtype::Float32 {
            return Err(format!(
                "cannot view VectorSlab of dtype {} as f32",
                self.dtype.as_str()
            ));
        }
        let ptr = self.row_ptr(row)? as *const f32;
        // SAFETY: For `VectorDtype::Float32`, `row_stride_bytes` is a multiple of 4 and
        // `ptr` is 4-byte aligned and valid for `self.dimensions` `f32` elements.
        Ok(unsafe { std::slice::from_raw_parts(ptr, self.dimensions) })
    }

    #[inline]
    pub fn row_f32_mut(&mut self, row: usize) -> Result<&mut [f32], String> {
        if self.dtype != VectorDtype::Float32 {
            return Err(format!(
                "cannot view VectorSlab of dtype {} as mutable f32",
                self.dtype.as_str()
            ));
        }
        if row >= self.count {
            return Err(format!(
                "row index {row} out of bounds for VectorSlab of count {}",
                self.count
            ));
        }
        // SAFETY: `row < self.count` and `Float32` rows are 4-byte aligned within `data_mut_ptr()`.
        let ptr = unsafe { self.data_mut_ptr().add(row * self.row_stride_bytes) as *mut f32 };
        // SAFETY: `ptr` is valid for `self.dimensions` mutable `f32` elements tied to `&mut self`.
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr, self.dimensions) })
    }

    #[inline]
    pub fn row_i8(&self, row: usize) -> Result<&[i8], String> {
        if self.dtype != VectorDtype::Int8 {
            return Err(format!(
                "cannot view VectorSlab of dtype {} as i8",
                self.dtype.as_str()
            ));
        }
        let ptr = self.row_ptr(row)? as *const i8;
        // SAFETY: `ptr` is valid for `self.dimensions` `i8` elements in `VectorDtype::Int8`.
        Ok(unsafe { std::slice::from_raw_parts(ptr, self.dimensions) })
    }

    #[inline]
    pub fn row_i8_mut(&mut self, row: usize) -> Result<&mut [i8], String> {
        if self.dtype != VectorDtype::Int8 {
            return Err(format!(
                "cannot view VectorSlab of dtype {} as mutable i8",
                self.dtype.as_str()
            ));
        }
        if row >= self.count {
            return Err(format!(
                "row index {row} out of bounds for VectorSlab of count {}",
                self.count
            ));
        }
        // SAFETY: `row < self.count` within the mutable `Int8` data region.
        let ptr = unsafe { self.data_mut_ptr().add(row * self.row_stride_bytes) as *mut i8 };
        // SAFETY: `ptr` is valid for `self.dimensions` mutable `i8` elements tied to `&mut self`.
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr, self.dimensions) })
    }

    #[inline]
    pub fn scale_at(&self, row: usize) -> f32 {
        if self.scales_offset == 0 || row >= self.count {
            return 1.0;
        }
        // SAFETY: `scales_offset` is 64-byte aligned and `scales_offset + count * 4 <= total_bytes`.
        unsafe {
            let scales_ptr = self.base_ptr().add(self.scales_offset) as *const f32;
            *scales_ptr.add(row)
        }
    }

    #[inline]
    pub fn set_scale_at(&mut self, row: usize, scale: f32) {
        if self.scales_offset == 0 || row >= self.count {
            return;
        }
        // SAFETY: `scales_offset` is 64-byte aligned and `row < self.count` within `total_bytes`.
        unsafe {
            let scales_ptr = self.base_mut_ptr().add(self.scales_offset) as *mut f32;
            *scales_ptr.add(row) = scale;
        }
    }
}

/// Compute L2 norm of an `f32` slice using an 8-lane unrolled accumulator.
#[inline]
pub fn l2_norm_f32(slice: &[f32]) -> f32 {
    let mut s0 = 0.0f32;
    let mut s1 = 0.0f32;
    let mut s2 = 0.0f32;
    let mut s3 = 0.0f32;
    let mut s4 = 0.0f32;
    let mut s5 = 0.0f32;
    let mut s6 = 0.0f32;
    let mut s7 = 0.0f32;

    let (chunks, rem) = slice.as_chunks::<8>();
    for c in chunks {
        s0 = c[0].mul_add(c[0], s0);
        s1 = c[1].mul_add(c[1], s1);
        s2 = c[2].mul_add(c[2], s2);
        s3 = c[3].mul_add(c[3], s3);
        s4 = c[4].mul_add(c[4], s4);
        s5 = c[5].mul_add(c[5], s5);
        s6 = c[6].mul_add(c[6], s6);
        s7 = c[7].mul_add(c[7], s7);
    }
    let mut sum = (s0 + s1) + (s2 + s3) + (s4 + s5) + (s6 + s7);
    for &x in rem {
        sum = x.mul_add(x, sum);
    }
    sum.sqrt()
}

/// Normalize an `f32` slice in place to unit L2 length and return its original norm.
#[inline]
pub fn l2_normalize_in_place(slice: &mut [f32]) -> f32 {
    let norm = l2_norm_f32(slice);
    if norm > 1e-12 {
        let inv = 1.0 / norm;
        for x in slice.iter_mut() {
            *x *= inv;
        }
    }
    norm
}

/// Matryoshka Representation Learning (MRL) prefix truncation + L2 renormalization.
#[inline]
pub fn mrl_truncate_and_normalize(
    src: &[f32],
    target_dim: usize,
    dst: &mut [f32],
) -> Result<f32, String> {
    if target_dim == 0 || target_dim > src.len() {
        return Err(format!(
            "target_dim {target_dim} must be in 1..={}",
            src.len()
        ));
    }
    if dst.len() != target_dim {
        return Err(format!(
            "destination slice length {} does not match target_dim {target_dim}",
            dst.len()
        ));
    }
    dst.copy_from_slice(&src[..target_dim]);
    Ok(l2_normalize_in_place(dst))
}

/// Symmetric scalar quantization from `f32` to `i8`, returning the dequantization scale (`max_abs / 127.0`).
#[inline]
pub fn quantize_f32_to_i8(src: &[f32], dst: &mut [i8]) -> Result<f32, String> {
    if src.len() != dst.len() {
        return Err(format!(
            "src length {} does not match dst length {}",
            src.len(),
            dst.len()
        ));
    }
    let max_abs = src.iter().fold(0.0f32, |m, &x| m.max(x.abs()));
    let scale = if max_abs > 1e-12 {
        max_abs / 127.0
    } else {
        1.0
    };
    let inv_scale = 1.0 / scale;
    for (s, d) in src.iter().zip(dst.iter_mut()) {
        *d = (s * inv_scale).round().clamp(-127.0, 127.0) as i8;
    }
    Ok(scale)
}

/// Pack `f32` sign bits (`x > 0.0 -> 1`, else `0`) into MSB-first packed bytes (`src.len().div_ceil(8)`).
#[inline]
pub fn quantize_f32_to_binary(src: &[f32], dst: &mut [u8]) -> Result<(), String> {
    let expected = src.len().div_ceil(8);
    if dst.len() != expected {
        return Err(format!(
            "binary destination length {} does not match expected {expected} bytes",
            dst.len()
        ));
    }
    dst.fill(0);
    for (i, &val) in src.iter().enumerate() {
        if val > 0.0 {
            dst[i >> 3] |= 1u8 << (7 - (i & 7));
        }
    }
    Ok(())
}

/// 8-lane unrolled dot product over `f32` slices.
#[inline]
pub fn dot_product_f32(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    let a = &a[..n];
    let b = &b[..n];
    let mut s0 = 0.0f32;
    let mut s1 = 0.0f32;
    let mut s2 = 0.0f32;
    let mut s3 = 0.0f32;
    let mut s4 = 0.0f32;
    let mut s5 = 0.0f32;
    let mut s6 = 0.0f32;
    let mut s7 = 0.0f32;

    let (a_chunks, a_rem) = a.as_chunks::<8>();
    let (b_chunks, b_rem) = b.as_chunks::<8>();
    for (ac, bc) in a_chunks.iter().zip(b_chunks.iter()) {
        s0 = ac[0].mul_add(bc[0], s0);
        s1 = ac[1].mul_add(bc[1], s1);
        s2 = ac[2].mul_add(bc[2], s2);
        s3 = ac[3].mul_add(bc[3], s3);
        s4 = ac[4].mul_add(bc[4], s4);
        s5 = ac[5].mul_add(bc[5], s5);
        s6 = ac[6].mul_add(bc[6], s6);
        s7 = ac[7].mul_add(bc[7], s7);
    }
    let mut sum = (s0 + s1) + (s2 + s3) + (s4 + s5) + (s6 + s7);
    for (&x, &y) in a_rem.iter().zip(b_rem.iter()) {
        sum = x.mul_add(y, sum);
    }
    sum
}

/// Cosine similarity between two `f32` slices.
#[inline]
pub fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    let a = &a[..n];
    let b = &b[..n];
    let dot = dot_product_f32(a, b);
    let na = l2_norm_f32(a);
    let nb = l2_norm_f32(b);
    let denom = na * nb;
    if denom > 1e-12 { dot / denom } else { 0.0 }
}

/// Integer dot product over `i8` slices.
#[inline]
pub fn dot_product_i8(a: &[i8], b: &[i8]) -> i32 {
    let n = a.len().min(b.len());
    a[..n]
        .iter()
        .zip(b[..n].iter())
        .map(|(&x, &y)| (x as i32) * (y as i32))
        .sum()
}

/// Hardware `POPCNT` Hamming distance between packed binary vectors.
#[inline]
pub fn hamming_distance_binary(a: &[u8], b: &[u8]) -> u32 {
    let n = a.len().min(b.len());
    let a = &a[..n];
    let b = &b[..n];
    let mut dist = 0u32;
    let (a_chunks, a_rem) = a.as_chunks::<8>();
    let (b_chunks, b_rem) = b.as_chunks::<8>();
    for (ac, bc) in a_chunks.iter().zip(b_chunks.iter()) {
        let wa = u64::from_ne_bytes(*ac);
        let wb = u64::from_ne_bytes(*bc);
        dist += (wa ^ wb).count_ones();
    }
    for (&x, &y) in a_rem.iter().zip(b_rem.iter()) {
        dist += (x ^ y).count_ones();
    }
    dist
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimilarityMetric {
    Cosine,
    Dot,
    Hamming,
}

impl SimilarityMetric {
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "cosine" | "cos" => Some(Self::Cosine),
            "dot" | "ip" | "inner_product" => Some(Self::Dot),
            "hamming" | "bit_hamming" => Some(Self::Hamming),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimilarityMatch {
    pub index: usize,
    pub score: f32,
}

#[derive(Clone, Copy)]
struct MinScoreEntry {
    index: usize,
    score: f32,
}

impl PartialEq for MinScoreEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.index == other.index
    }
}

impl Eq for MinScoreEntry {}

impl PartialOrd for MinScoreEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinScoreEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.index.cmp(&other.index))
    }
}

/// Search `slab` for top-`k` rows matching `query_bytes` according to `metric`.
pub fn top_k_search(
    query_bytes: &[u8],
    slab: &VectorSlab,
    metric: SimilarityMetric,
    k: usize,
) -> Result<Vec<SimilarityMatch>, String> {
    if k == 0 || slab.count() == 0 {
        return Ok(Vec::new());
    }
    if query_bytes.len() != slab.packed_row_bytes() {
        return Err(format!(
            "query byte length {} does not match slab row length {}",
            query_bytes.len(),
            slab.packed_row_bytes()
        ));
    }

    enum Float32Query<'a> {
        Borrowed(&'a [f32]),
        Owned(Vec<f32>),
        None,
    }
    impl Float32Query<'_> {
        #[inline]
        fn as_slice(&self) -> &[f32] {
            match self {
                Self::Borrowed(s) => s,
                Self::Owned(v) => v,
                Self::None => &[],
            }
        }
    }
    let f32_query = if slab.dtype() == VectorDtype::Float32 {
        let ptr = query_bytes.as_ptr() as *const f32;
        if ptr.is_aligned() {
            // SAFETY: `query_bytes.len() == slab.packed_row_bytes() == slab.dimensions() * 4`
            // and `ptr.is_aligned()` verified 4-byte alignment for `f32`.
            Float32Query::Borrowed(unsafe { std::slice::from_raw_parts(ptr, slab.dimensions()) })
        } else {
            Float32Query::Owned(
                query_bytes
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| f32::from_ne_bytes([c[0], c[1], c[2], c[3]]))
                    .collect(),
            )
        }
    } else {
        Float32Query::None
    };

    let mut heap: BinaryHeap<MinScoreEntry> = BinaryHeap::with_capacity(k.min(slab.count()) + 1);
    for idx in 0..slab.count() {
        let score = match (slab.dtype(), metric) {
            (VectorDtype::Float32, SimilarityMetric::Cosine) => {
                let row = slab.row_f32(idx)?;
                cosine_similarity_f32(f32_query.as_slice(), row)
            }
            (VectorDtype::Float32, SimilarityMetric::Dot) => {
                let row = slab.row_f32(idx)?;
                dot_product_f32(f32_query.as_slice(), row)
            }
            (VectorDtype::Int8, SimilarityMetric::Dot | SimilarityMetric::Cosine) => {
                // SAFETY: `query_bytes.len() == slab.dimensions()` and `i8` has alignment 1
                // with identical size and representation to `u8`.
                let q = unsafe {
                    std::slice::from_raw_parts(query_bytes.as_ptr() as *const i8, slab.dimensions())
                };
                let row = slab.row_i8(idx)?;
                (dot_product_i8(q, row) as f32) * slab.scale_at(idx)
            }
            (VectorDtype::Binary, SimilarityMetric::Hamming) => {
                let row = slab.row_bytes(idx)?;
                -(hamming_distance_binary(query_bytes, row) as f32)
            }
            (dtype, _) => {
                return Err(format!(
                    "unsupported metric {metric:?} for VectorSlab dtype {}",
                    dtype.as_str()
                ));
            }
        };

        if heap.len() < k {
            heap.push(MinScoreEntry { index: idx, score });
        } else if let Some(&worst) = heap.peek()
            && score > worst.score
        {
            let _ = heap.pop();
            heap.push(MinScoreEntry { index: idx, score });
        }
    }

    let mut out: Vec<SimilarityMatch> = heap
        .into_iter()
        .map(|e| SimilarityMatch {
            index: e.index,
            score: e.score,
        })
        .collect();
    out.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.index.cmp(&b.index))
    });
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingTask {
    SearchQuery,
    SearchDocument,
    QuestionAnswering,
    FactVerification,
    Classification,
    Clustering,
    SemanticSimilarity,
    CodeRetrieval,
}

impl EmbeddingTask {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "query" | "search_query" | "retrieval_query" => Some(Self::SearchQuery),
            "document" | "search_document" | "retrieval_document" => Some(Self::SearchDocument),
            "qa" | "question_answering" => Some(Self::QuestionAnswering),
            "fact_verification" | "fact" => Some(Self::FactVerification),
            "classification" => Some(Self::Classification),
            "clustering" => Some(Self::Clustering),
            "semantic_similarity" | "similarity" => Some(Self::SemanticSimilarity),
            "code_retrieval" | "code" => Some(Self::CodeRetrieval),
            _ => None,
        }
    }

    pub const fn gemma_task_prefix(self) -> &'static str {
        match self {
            Self::SearchQuery => "task: search result | query: ",
            Self::SearchDocument => "title: none | text: ",
            Self::QuestionAnswering => "task: question answering | query: ",
            Self::FactVerification => "task: fact checking | query: ",
            Self::Classification => "task: classification | query: ",
            Self::Clustering => "task: clustering | query: ",
            Self::SemanticSimilarity => "task: sentence similarity | query: ",
            Self::CodeRetrieval => "task: code retrieval | query: ",
        }
    }

    /// Protobuf `TaskType` enum value used by Google Vertex AI / Gemini `EmbedContentRequest`.
    pub const fn gemini_proto_task_type(self) -> u32 {
        match self {
            Self::SearchQuery => 1,
            Self::SearchDocument => 2,
            Self::SemanticSimilarity => 3,
            Self::Classification => 4,
            Self::Clustering => 5,
            Self::QuestionAnswering => 6,
            Self::FactVerification => 7,
            Self::CodeRetrieval => 8,
        }
    }
}

/// Format an EmbeddingGemma prompt string with zero intermediate allocations.
pub fn format_gemma_prompt(task: EmbeddingTask, text: &str, title: Option<&str>) -> String {
    match task {
        EmbeddingTask::SearchDocument => {
            let resolved_title = title
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .unwrap_or("none");
            let mut out = String::with_capacity(16 + resolved_title.len() + text.len());
            out.push_str("title: ");
            out.push_str(resolved_title);
            out.push_str(" | text: ");
            out.push_str(text);
            out
        }
        other => {
            let prefix = other.gemma_task_prefix();
            let mut out = String::with_capacity(prefix.len() + text.len());
            out.push_str(prefix);
            out.push_str(text);
            out
        }
    }
}

/// Splice pre-tokenized task-prefix IDs and document token IDs directly into `out` bounded by `max_seq_len`.
pub fn splice_prefix_tokens(
    prefix_ids: &[u32],
    doc_ids: &[u32],
    eos_id: Option<u32>,
    max_seq_len: usize,
    out: &mut Vec<u32>,
) {
    out.clear();
    if max_seq_len == 0 {
        return;
    }
    let reserve_eos = usize::from(eos_id.is_some());
    let budget = max_seq_len.saturating_sub(reserve_eos);
    let prefix_take = prefix_ids.len().min(budget);
    let doc_take = doc_ids.len().min(budget.saturating_sub(prefix_take));
    out.reserve(prefix_take + doc_take + reserve_eos);
    out.extend_from_slice(&prefix_ids[..prefix_take]);
    out.extend_from_slice(&doc_ids[..doc_take]);
    if let Some(eos) = eos_id
        && out.len() < max_seq_len
    {
        out.push(eos);
    }
}

/// Compute 64-bit FNV-1a hash for vector cache keys.
#[inline]
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    if hash == 0 { 1 } else { hash }
}

/// Compute a composite cache key over `(model_id, task_tag, dimensions, dtype, text)`.
#[inline]
pub fn compute_vector_cache_key(
    model_id: &str,
    task_tag: &str,
    dimensions: usize,
    dtype: VectorDtype,
    text: &str,
) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for chunk in [
        model_id.as_bytes(),
        b"\x1f",
        task_tag.as_bytes(),
        b"\x1f",
        &dimensions.to_le_bytes(),
        &[dtype as u8],
        b"\x1f",
        text.as_bytes(),
    ] {
        for &b in chunk {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    if hash == 0 { 1 } else { hash }
}

#[repr(C, align(64))]
struct VectorCacheSlotHeader {
    seq: AtomicU64,
    key_hash: AtomicU64,
    updated_tick: AtomicU64,
    _pad: [u8; 40],
}

const _: () = assert!(std::mem::size_of::<VectorCacheSlotHeader>() == 64);

/// Lock-free seqlock-guarded vector embedding cache in shared memory or aligned heap memory.
#[derive(Debug)]
pub struct VectorCache {
    backing: SlabBacking,
    capacity: usize,
    dimensions: usize,
    slot_stride_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    tick: AtomicU64,
}

impl VectorCache {
    fn compute_layout(capacity: usize, dimensions: usize) -> Result<(usize, usize), String> {
        if capacity == 0 || dimensions == 0 {
            return Err("VectorCache capacity and dimensions must be > 0".to_string());
        }
        let vec_bytes = dimensions
            .checked_mul(4)
            .ok_or_else(|| "VectorCache dimensions byte size overflow".to_string())?;
        let slot_stride_bytes = 64usize
            .checked_add(vec_bytes)
            .ok_or_else(|| "VectorCache slot stride overflow".to_string())?
            .checked_next_multiple_of(64)
            .ok_or_else(|| "VectorCache slot stride overflow".to_string())?;
        let slots_bytes = capacity
            .checked_mul(slot_stride_bytes)
            .ok_or_else(|| "VectorCache total size overflow".to_string())?;
        let total_bytes = 64usize
            .checked_add(slots_bytes)
            .ok_or_else(|| "VectorCache total size overflow".to_string())?;
        Ok((slot_stride_bytes, total_bytes))
    }

    pub fn new_in_memory(capacity: usize, dimensions: usize) -> Result<Self, String> {
        let (slot_stride_bytes, total_bytes) = Self::compute_layout(capacity, dimensions)?;
        let buf = AlignedBuffer::new_zeroed(total_bytes)?;
        Ok(Self {
            backing: SlabBacking::Memory(buf),
            capacity,
            dimensions,
            slot_stride_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            tick: AtomicU64::new(1),
        })
    }

    pub fn create_shm(
        name: Option<&str>,
        capacity: usize,
        dimensions: usize,
    ) -> Result<Self, String> {
        let (slot_stride_bytes, total_bytes) = Self::compute_layout(capacity, dimensions)?;
        let (region, resolved_name) = match name {
            #[cfg(unix)]
            Some(explicit) => {
                let mut reg = ShmRegion::create_named(explicit, total_bytes)
                    .map_err(|e| format!("failed to create SHM VectorCache '{explicit}': {e}"))?;
                reg.disarm_unlink();
                (reg, Some(explicit.to_string()))
            }
            _ => {
                #[cfg(unix)]
                {
                    let (auto_name, reg) = ShmRegion::create_overflow_region("vcache", total_bytes)
                        .map_err(|e| format!("failed to create SHM VectorCache: {e}"))?;
                    (reg, Some(auto_name))
                }
                #[cfg(not(unix))]
                {
                    let reg = ShmRegion::create(total_bytes)
                        .map_err(|e| format!("failed to create SHM VectorCache: {e}"))?;
                    (reg, None)
                }
            }
        };
        // SAFETY: `region` was newly created with at least `total_bytes` mapped writable bytes.
        unsafe {
            std::ptr::write_bytes(region.as_ptr(), 0, total_bytes);
        }
        Ok(Self {
            backing: SlabBacking::Shm {
                region,
                name: resolved_name,
            },
            capacity,
            dimensions,
            slot_stride_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            tick: AtomicU64::new(1),
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    #[inline]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    #[inline]
    fn base_ptr(&self) -> *const u8 {
        match &self.backing {
            SlabBacking::Memory(buf) => buf.as_ptr(),
            SlabBacking::Shm { region, .. } => region.as_ptr(),
        }
    }

    #[inline]
    fn slot_ptr(&self, idx: usize) -> *const u8 {
        // SAFETY: Callers pass `idx < self.capacity`, so `64 + idx * self.slot_stride_bytes`
        // is 64-byte aligned and within the cache allocation.
        unsafe { self.base_ptr().add(64 + idx * self.slot_stride_bytes) }
    }

    /// Optimistic seqlock lookup (< 200 ns) over a 4-way set-associative window.
    pub fn get(&self, key_hash: u64) -> Option<Vec<f32>> {
        let mut out = vec![0.0f32; self.dimensions];
        if self.get_into(key_hash, &mut out) {
            Some(out)
        } else {
            None
        }
    }

    pub fn get_into(&self, key_hash: u64, dst: &mut [f32]) -> bool {
        if dst.len() != self.dimensions {
            return false;
        }
        let ways = 4usize.min(self.capacity);
        let base_idx = (key_hash as usize) % self.capacity;

        for w in 0..ways {
            let idx = (base_idx + w) % self.capacity;
            let ptr = self.slot_ptr(idx);
            // SAFETY: `ptr` is 64-byte aligned and points to a valid `VectorCacheSlotHeader`.
            let hdr = unsafe { &*(ptr as *const VectorCacheSlotHeader) };
            // SAFETY: `ptr.add(64)` points to `self.dimensions` `f32` elements inside the slot.
            let vec_ptr = unsafe { ptr.add(64) as *const f32 };

            for _attempt in 0..4 {
                let seq1 = hdr.seq.load(Ordering::Acquire);
                if seq1 == 0 || (seq1 & 1) == 1 {
                    break;
                }
                let slot_key = hdr.key_hash.load(Ordering::Relaxed);
                if slot_key != key_hash {
                    break;
                }
                for (i, item) in dst.iter_mut().enumerate() {
                    // SAFETY: `i < self.dimensions`; volatile read pairs with the seqlock fence below.
                    *item = unsafe { std::ptr::read_volatile(vec_ptr.add(i)) };
                }
                std::sync::atomic::fence(Ordering::Acquire);
                let seq2 = hdr.seq.load(Ordering::Relaxed);
                if seq1 == seq2 {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        false
    }

    pub fn put(&self, key_hash: u64, values: &[f32]) -> Result<(), String> {
        if values.len() != self.dimensions {
            return Err(format!(
                "vector length {} does not match cache dimensions {}",
                values.len(),
                self.dimensions
            ));
        }
        let ways = 4usize.min(self.capacity);
        let base_idx = (key_hash as usize) % self.capacity;
        let mut target_idx = base_idx;
        let mut oldest_tick = u64::MAX;

        for w in 0..ways {
            let idx = (base_idx + w) % self.capacity;
            let ptr = self.slot_ptr(idx);
            // SAFETY: `ptr` is 64-byte aligned and points to a valid `VectorCacheSlotHeader`.
            let hdr = unsafe { &*(ptr as *const VectorCacheSlotHeader) };
            let slot_key = hdr.key_hash.load(Ordering::Relaxed);
            if slot_key == 0 || slot_key == key_hash {
                target_idx = idx;
                break;
            }
            let tick = hdr.updated_tick.load(Ordering::Relaxed);
            if tick < oldest_tick {
                oldest_tick = tick;
                target_idx = idx;
            }
        }

        let ptr = self.slot_ptr(target_idx) as *mut u8;
        // SAFETY: `ptr` is 64-byte aligned and points to the target `VectorCacheSlotHeader`.
        let hdr = unsafe { &*(ptr as *const VectorCacheSlotHeader) };
        // SAFETY: `ptr.add(64)` points to `self.dimensions` `f32` elements inside the target slot.
        let vec_ptr = unsafe { ptr.add(64) as *mut f32 };

        loop {
            let cur = hdr.seq.load(Ordering::Acquire);
            if (cur & 1) == 1 {
                std::hint::spin_loop();
                continue;
            }
            if hdr
                .seq
                .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                hdr.key_hash.store(key_hash, Ordering::Relaxed);
                let now_tick = self.tick.fetch_add(1, Ordering::Relaxed);
                hdr.updated_tick.store(now_tick, Ordering::Relaxed);
                for (i, &val) in values.iter().enumerate() {
                    // SAFETY: Exclusive writer lock (`seq` is odd) guarantees no concurrent writer
                    // on `vec_ptr`, and `i < self.dimensions`.
                    unsafe {
                        std::ptr::write_volatile(vec_ptr.add(i), val);
                    }
                }
                hdr.seq.store(cur + 2, Ordering::Release);
                break;
            }
        }
        Ok(())
    }

    pub fn similarity_search(
        &self,
        query: &[f32],
        k: usize,
        metric: SimilarityMetric,
    ) -> Result<Vec<SimilarityMatch>, String> {
        if query.len() != self.dimensions {
            return Err(format!(
                "query dimension {} does not match cache dimension {}",
                query.len(),
                self.dimensions
            ));
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut heap: BinaryHeap<MinScoreEntry> = BinaryHeap::with_capacity(k + 1);
        let mut row_buf = vec![0.0f32; self.dimensions];

        for idx in 0..self.capacity {
            let ptr = self.slot_ptr(idx);
            // SAFETY: `ptr` is 64-byte aligned and points to a valid `VectorCacheSlotHeader`.
            let hdr = unsafe { &*(ptr as *const VectorCacheSlotHeader) };
            // SAFETY: `ptr.add(64)` points to `self.dimensions` `f32` elements inside the slot.
            let vec_ptr = unsafe { ptr.add(64) as *const f32 };

            let seq1 = hdr.seq.load(Ordering::Acquire);
            if seq1 == 0 || (seq1 & 1) == 1 {
                continue;
            }
            let key_hash = hdr.key_hash.load(Ordering::Relaxed);
            if key_hash == 0 {
                continue;
            }
            for (i, dst) in row_buf.iter_mut().enumerate() {
                // SAFETY: `i < self.dimensions`; volatile read pairs with the seqlock fence below.
                *dst = unsafe { std::ptr::read_volatile(vec_ptr.add(i)) };
            }
            std::sync::atomic::fence(Ordering::Acquire);
            let seq2 = hdr.seq.load(Ordering::Relaxed);
            if seq1 != seq2 {
                continue;
            }

            let score = match metric {
                SimilarityMetric::Cosine => cosine_similarity_f32(query, &row_buf),
                SimilarityMetric::Dot => dot_product_f32(query, &row_buf),
                SimilarityMetric::Hamming => {
                    return Err("Hamming metric requires binary vectors".to_string());
                }
            };

            if heap.len() < k {
                heap.push(MinScoreEntry { index: idx, score });
            } else if let Some(&worst) = heap.peek()
                && score > worst.score
            {
                let _ = heap.pop();
                heap.push(MinScoreEntry { index: idx, score });
            }
        }

        let mut out: Vec<SimilarityMatch> = heap
            .into_iter()
            .map(|e| SimilarityMatch {
                index: e.index,
                score: e.score,
            })
            .collect();
        out.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.index.cmp(&b.index))
        });
        Ok(out)
    }
}

/// Pure-Rust static token embedding table (`Model2Vec` / static hash projection) executing in `< 10 us`.
#[derive(Debug)]
pub struct StaticEmbedderCore {
    vocab: Option<HashMap<String, usize>>,
    weights: Arc<VectorSlab>,
    dimensions: usize,
}

impl StaticEmbedderCore {
    pub fn new(vocab_size: usize, dimensions: usize, seed: u64) -> Result<Self, String> {
        if vocab_size == 0 || dimensions == 0 {
            return Err("vocab_size and dimensions must be > 0".to_string());
        }
        let mut slab =
            VectorSlab::new_in_memory(vocab_size, dimensions, VectorDtype::Float32, true)?;
        let mut state = seed ^ 0x9E3779B97F4A7C15;
        for row_idx in 0..vocab_size {
            let row = slab.row_f32_mut(row_idx)?;
            for elem in row.iter_mut() {
                state ^= state >> 12;
                state ^= state << 25;
                state ^= state >> 27;
                let word = state.wrapping_mul(0x2545F4914F6CDD1D);
                let unit = ((word >> 40) as f32) / ((1u32 << 24) as f32);
                *elem = unit * 2.0 - 1.0;
            }
            l2_normalize_in_place(row);
        }
        Ok(Self {
            vocab: None,
            weights: Arc::new(slab),
            dimensions,
        })
    }

    pub fn from_vocab_and_weights(
        vocab: HashMap<String, usize>,
        weights: &[Vec<f32>],
    ) -> Result<Self, String> {
        if weights.is_empty() {
            return Err("weights matrix must not be empty".to_string());
        }
        let dimensions = weights[0].len();
        if dimensions == 0 {
            return Err("embedding dimensions must be > 0".to_string());
        }
        let mut slab =
            VectorSlab::new_in_memory(weights.len(), dimensions, VectorDtype::Float32, false)?;
        for (i, row_vec) in weights.iter().enumerate() {
            if row_vec.len() != dimensions {
                return Err(format!(
                    "row {i} has length {} != expected {dimensions}",
                    row_vec.len()
                ));
            }
            slab.row_f32_mut(i)?.copy_from_slice(row_vec);
        }
        let normalized_vocab = vocab
            .into_iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect();
        Ok(Self {
            vocab: Some(normalized_vocab),
            weights: Arc::new(slab),
            dimensions,
        })
    }

    pub fn from_slab(
        weights: Arc<VectorSlab>,
        vocab: Option<HashMap<String, usize>>,
    ) -> Result<Self, String> {
        if weights.dtype() != VectorDtype::Float32 {
            return Err("StaticEmbedderCore requires Float32 VectorSlab".to_string());
        }
        let dimensions = weights.dimensions();
        Ok(Self {
            vocab,
            weights,
            dimensions,
        })
    }

    #[inline]
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    #[inline]
    pub fn vocab_size(&self) -> usize {
        self.weights.count()
    }

    pub fn embed_text_into(
        &self,
        text: &str,
        target_dim: usize,
        out: &mut [f32],
    ) -> Result<(), String> {
        if target_dim == 0 || target_dim > self.dimensions || out.len() != target_dim {
            return Err(format!(
                "invalid target_dim {target_dim} for StaticEmbedderCore of dimensions {}",
                self.dimensions
            ));
        }
        out.fill(0.0);
        let vocab_len = self.weights.count();
        let mut count = 0usize;

        for token in text.split(|c: char| !c.is_alphanumeric()) {
            if token.is_empty() {
                continue;
            }
            let lower = token.to_ascii_lowercase();
            let row_idx = if let Some(ref map) = self.vocab {
                if let Some(&idx) = map.get(&lower)
                    && idx < vocab_len
                {
                    idx
                } else {
                    (fnv1a_64(lower.as_bytes()) as usize) % vocab_len
                }
            } else {
                (fnv1a_64(lower.as_bytes()) as usize) % vocab_len
            };
            let row = self.weights.row_f32(row_idx)?;
            for (dst_val, &src_val) in out.iter_mut().zip(row[..target_dim].iter()) {
                *dst_val += src_val;
            }
            count += 1;
        }

        if count > 0 {
            let inv = 1.0 / (count as f32);
            for v in out.iter_mut() {
                *v *= inv;
            }
            l2_normalize_in_place(out);
        }
        Ok(())
    }

    pub fn embed_batch(&self, texts: &[String], target_dim: usize) -> Result<VectorSlab, String> {
        let resolved_dim = if target_dim == 0 {
            self.dimensions
        } else {
            target_dim
        };
        let mut slab =
            VectorSlab::new_in_memory(texts.len(), resolved_dim, VectorDtype::Float32, false)?;
        for (i, text) in texts.iter().enumerate() {
            let row = slab.row_f32_mut(i)?;
            self.embed_text_into(text, resolved_dim, row)?;
        }
        Ok(slab)
    }
}

// ============================================================================
// Compact Protobuf v3 + 5-Byte gRPC LPM Wire Codec for Google Embeddings
// ============================================================================

use crate::proto::{
    WIRE_TYPE_LENGTH_DELIMITED, put_bytes_field, put_varint_field, read_length_delimited, read_tag,
    skip_field,
};

/// Single content part inside a Gemini `Content` message (`google.ai.generativelanguage.v1beta.Part`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeminiContentPart<'a> {
    Text(&'a str),
    InlineData {
        mime_type: &'a str,
        data: &'a [u8],
    },
    FileData {
        mime_type: &'a str,
        file_uri: &'a str,
    },
}

fn encode_batch_embed_contents_unchecked(
    model: &str,
    contents: &[Vec<GeminiContentPart<'_>>],
    task_type: Option<u32>,
    titles: Option<&[Option<String>]>,
    output_dimensionality: Option<u32>,
) -> Vec<u8> {
    let normalized_model = if model.starts_with("models/") || model.starts_with("projects/") {
        model.to_string()
    } else {
        format!("models/{model}")
    };

    let mut body = Vec::with_capacity(64 + contents.len() * 64);
    put_bytes_field(&mut body, 1, normalized_model.as_bytes());

    let mut blob_buf = Vec::new();
    let mut part_buf = Vec::new();
    let mut content_buf = Vec::new();
    let mut req_buf = Vec::new();

    for (idx, parts) in contents.iter().enumerate() {
        content_buf.clear();
        for part in parts {
            part_buf.clear();
            match part {
                GeminiContentPart::Text(text) => {
                    put_bytes_field(&mut part_buf, 2, text.as_bytes());
                }
                GeminiContentPart::InlineData { mime_type, data } => {
                    blob_buf.clear();
                    put_bytes_field(&mut blob_buf, 1, mime_type.as_bytes());
                    put_bytes_field(&mut blob_buf, 2, data);
                    put_bytes_field(&mut part_buf, 3, &blob_buf);
                }
                GeminiContentPart::FileData {
                    mime_type,
                    file_uri,
                } => {
                    blob_buf.clear();
                    put_bytes_field(&mut blob_buf, 1, mime_type.as_bytes());
                    put_bytes_field(&mut blob_buf, 2, file_uri.as_bytes());
                    put_bytes_field(&mut part_buf, 6, &blob_buf);
                }
            }
            put_bytes_field(&mut content_buf, 1, &part_buf);
        }

        req_buf.clear();
        put_bytes_field(&mut req_buf, 1, normalized_model.as_bytes());
        put_bytes_field(&mut req_buf, 2, &content_buf);
        if let Some(tt) = task_type
            && tt > 0
        {
            put_varint_field(&mut req_buf, 3, tt as u64);
        }
        if let Some(title_list) = titles
            && let Some(Some(title)) = title_list.get(idx)
            && !title.is_empty()
        {
            put_bytes_field(&mut req_buf, 4, title.as_bytes());
        }
        if let Some(dim) = output_dimensionality
            && dim > 0
        {
            put_varint_field(&mut req_buf, 5, dim as u64);
        }

        put_bytes_field(&mut body, 2, &req_buf);
    }

    let mut frame = Vec::with_capacity(5 + body.len());
    frame.push(0u8);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Encode multimodal `BatchEmbedContentsRequest` parts (`text=2`, `inline_data=3`, `file_data=6`) wrapped in a 5-byte gRPC LPM frame.
pub fn encode_batch_embed_contents_grpc_request(
    model: &str,
    contents: &[Vec<GeminiContentPart<'_>>],
    task_type: Option<u32>,
    titles: Option<&[Option<String>]>,
    output_dimensionality: Option<u32>,
) -> Result<Vec<u8>, String> {
    if contents.is_empty() {
        return Err("contents must not be empty".to_string());
    }
    if let Some(title_list) = titles
        && title_list.len() != contents.len()
    {
        return Err(format!(
            "titles length ({}) must match contents length ({})",
            title_list.len(),
            contents.len()
        ));
    }
    for (idx, parts) in contents.iter().enumerate() {
        if parts.is_empty() {
            return Err(format!("content at index {idx} must not be empty"));
        }
        for part in parts {
            match part {
                GeminiContentPart::Text(_) => {}
                GeminiContentPart::InlineData { mime_type, .. } => {
                    if mime_type.trim().is_empty() {
                        return Err("inline_data mime_type must not be empty".to_string());
                    }
                }
                GeminiContentPart::FileData {
                    mime_type,
                    file_uri,
                } => {
                    if mime_type.trim().is_empty() {
                        return Err("file_data mime_type must not be empty".to_string());
                    }
                    if file_uri.trim().is_empty() {
                        return Err("file_data file_uri must not be empty".to_string());
                    }
                }
            }
        }
    }
    Ok(encode_batch_embed_contents_unchecked(
        model,
        contents,
        task_type,
        titles,
        output_dimensionality,
    ))
}

/// Encode a `BatchEmbedContentsRequest` wrapped in a 5-byte gRPC Length-Prefixed Message frame.
pub fn encode_batch_embed_grpc_request(
    model: &str,
    texts: &[String],
    task_type: Option<u32>,
    titles: Option<&[Option<String>]>,
    output_dimensionality: Option<u32>,
) -> Result<Vec<u8>, String> {
    let contents: Vec<Vec<GeminiContentPart<'_>>> = texts
        .iter()
        .map(|text| vec![GeminiContentPart::Text(text.as_str())])
        .collect();
    encode_batch_embed_contents_grpc_request(
        model,
        &contents,
        task_type,
        titles,
        output_dimensionality,
    )
}

/// Encode a `BatchEmbedContentsResponse` wrapped in a 5-byte gRPC LPM frame (used by `RemoteEmbedder` servers and tests).
pub fn encode_batch_embed_grpc_response_from_slab(slab: &VectorSlab) -> Result<Vec<u8>, String> {
    if slab.dtype() != VectorDtype::Float32 {
        return Err("gRPC packed float encoder requires Float32 VectorSlab".to_string());
    }
    let count = slab.count();
    let dims = slab.dimensions();
    let mut body = Vec::with_capacity(count * (dims * 4 + 8));
    let mut emb_buf = Vec::with_capacity(dims * 4 + 8);

    for i in 0..count {
        let row_bytes = slab.row_bytes(i)?;
        emb_buf.clear();
        put_bytes_field(&mut emb_buf, 1, row_bytes);
        put_bytes_field(&mut body, 1, &emb_buf);
    }

    let mut frame = Vec::with_capacity(5 + body.len());
    frame.push(0u8);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Decode a 5-byte gRPC LPM `BatchEmbedContentsResponse` directly into a `VectorSlab` with MRL truncation and L2 normalization.
pub fn decode_batch_embed_grpc_response_into_slab(
    frame: &[u8],
    target_dim: Option<usize>,
    normalize: bool,
) -> Result<VectorSlab, String> {
    let payload = if frame.len() >= 5 && (frame[0] == 0 || frame[0] == 1) {
        let msg_len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        if 5 + msg_len <= frame.len() {
            &frame[5..5 + msg_len]
        } else {
            frame
        }
    } else {
        frame
    };

    let mut raw_slices: Vec<&[u8]> = Vec::new();
    let mut pos = 0usize;
    while pos < payload.len() {
        let (field_number, wire_type) = read_tag(payload, &mut pos)?;
        if field_number == 1 && wire_type == WIRE_TYPE_LENGTH_DELIMITED {
            let emb_msg = read_length_delimited(payload, &mut pos)
                .map_err(|_| "truncated ContentEmbedding message in gRPC response".to_string())?;

            let mut epos = 0usize;
            while epos < emb_msg.len() {
                let (efield, ewire) = read_tag(emb_msg, &mut epos)?;
                if efield == 1 && ewire == WIRE_TYPE_LENGTH_DELIMITED {
                    let slice = read_length_delimited(emb_msg, &mut epos).map_err(|_| {
                        "invalid packed float slice in ContentEmbedding".to_string()
                    })?;
                    if !slice.len().is_multiple_of(4) {
                        return Err("invalid packed float slice in ContentEmbedding".to_string());
                    }
                    raw_slices.push(slice);
                } else {
                    skip_field(emb_msg, &mut epos, ewire)?;
                }
            }
        } else {
            skip_field(payload, &mut pos, wire_type)?;
        }
    }

    if raw_slices.is_empty() {
        return Err("no embeddings found in gRPC response payload".to_string());
    }

    let source_dim = raw_slices[0].len() / 4;
    let resolved_dim = match target_dim {
        Some(d) if d > 0 && d <= source_dim => d,
        Some(d) => {
            return Err(format!(
                "requested target_dim {d} exceeds response dimension {source_dim}"
            ));
        }
        None => source_dim,
    };

    let mut slab =
        VectorSlab::new_in_memory(raw_slices.len(), resolved_dim, VectorDtype::Float32, false)?;
    for (row_idx, raw) in raw_slices.into_iter().enumerate() {
        if raw.len() / 4 < resolved_dim {
            return Err(format!(
                "embedding row {row_idx} has dimension {} < {resolved_dim}",
                raw.len() / 4
            ));
        }
        let copy_bytes = resolved_dim * 4;
        slab.row_bytes_mut(row_idx)?
            .copy_from_slice(&raw[..copy_bytes]);
        if normalize {
            let row_f32 = slab.row_f32_mut(row_idx)?;
            l2_normalize_in_place(row_f32);
        }
    }
    Ok(slab)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_slab_shm_and_mrl_quantization() {
        let mut slab =
            VectorSlab::create_shm(None, 4, 8, VectorDtype::Float32, false).expect("create_shm");
        for r in 0..4 {
            let row = slab.row_f32_mut(r).expect("row_f32_mut");
            for (c, val) in row.iter_mut().enumerate() {
                *val = ((r + 1) * (c + 1)) as f32;
            }
            l2_normalize_in_place(row);
        }

        #[cfg(unix)]
        if let Some(name) = slab.shm_name() {
            let reopened = VectorSlab::open_shm(name, slab.total_bytes).expect("open_shm");
            assert_eq!(reopened.count(), 4);
            assert_eq!(reopened.dimensions(), 8);
            assert_eq!(
                reopened.row_f32(0).expect("row0"),
                slab.row_f32(0).expect("row0")
            );
            let _ = crate::shm::shm_unlink_named(name);
        }

        let mut trunc = [0.0f32; 4];
        mrl_truncate_and_normalize(slab.row_f32(0).expect("row0"), 4, &mut trunc).expect("mrl");
        let norm = l2_norm_f32(&trunc);
        assert!((norm - 1.0).abs() < 1e-6);

        let mut q_i8 = [0i8; 4];
        let scale = quantize_f32_to_i8(&trunc, &mut q_i8).expect("quantize i8");
        assert!(scale > 0.0);

        let mut q_bin = [0u8; 1];
        quantize_f32_to_binary(&[-1.0, 1.0, -0.5, 0.5, 1.0, -1.0, 0.0, 2.0], &mut q_bin)
            .expect("quantize bin");
        assert_eq!(q_bin[0], 0b0101_1001);
        assert_eq!(hamming_distance_binary(&q_bin, &[0b0101_0001]), 1);
    }

    #[test]
    fn test_vector_cache_seqlock_and_grpc_codec() {
        let cache = VectorCache::new_in_memory(64, 4).expect("cache");
        let key = compute_vector_cache_key(
            "gemini-embedding-001",
            "query",
            4,
            VectorDtype::Float32,
            "hello",
        );
        assert!(cache.get(key).is_none());
        cache.put(key, &[0.5, 0.5, 0.5, 0.5]).expect("put");
        assert_eq!(cache.get(key), Some(vec![0.5, 0.5, 0.5, 0.5]));
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);

        let slab =
            VectorSlab::from_f32_slice(&[3.0, 4.0, 0.0, 0.0, 0.0, 0.0, 5.0, 12.0], 2, 4, false)
                .expect("from_f32_slice");
        let frame = encode_batch_embed_grpc_response_from_slab(&slab).expect("encode_resp");
        let decoded =
            decode_batch_embed_grpc_response_into_slab(&frame, Some(2), true).expect("decode_resp");
        assert_eq!(decoded.count(), 2);
        assert_eq!(decoded.dimensions(), 2);
        let row0 = decoded.row_f32(0).expect("row0");
        assert!((row0[0] - 0.6).abs() < 1e-5);
        assert!((row0[1] - 0.8).abs() < 1e-5);
    }

    #[test]
    fn test_gemini_batch_embed_contents_wire_tags_and_validation() {
        let text_frame = encode_batch_embed_grpc_request(
            "gemini-embedding-001",
            &["hi".to_string()],
            None,
            None,
            None,
        )
        .expect("text encode");
        assert!(text_frame.windows(4).any(|w| w == [0x12, 0x02, b'h', b'i']));
        assert!(
            encode_batch_embed_grpc_request("gemini-embedding-001", &[], None, None, None,)
                .is_err()
        );
        assert!(
            encode_batch_embed_grpc_request(
                "gemini-embedding-001",
                &["hi".to_string()],
                None,
                Some(&[None, None]),
                None,
            )
            .is_err()
        );

        let media_contents = vec![
            vec![
                GeminiContentPart::InlineData {
                    mime_type: "image/png",
                    data: b"\x89P",
                },
                GeminiContentPart::Text("cap"),
            ],
            vec![GeminiContentPart::FileData {
                mime_type: "video/mp4",
                file_uri: "https://example.com/v.mp4",
            }],
        ];
        let media_frame = encode_batch_embed_contents_grpc_request(
            "gemini-embedding-2",
            &media_contents,
            Some(9),
            Some(&[Some("T1".to_string()), None]),
            Some(256),
        )
        .expect("multimodal encode");
        assert!(
            media_frame
                .windows(17)
                .any(|w| w.starts_with(&[0x1a, 0x0f, 0x0a, 0x09])
                    && w.ends_with(&[0x12, 0x02, 0x89, b'P']))
        );
        assert!(
            media_frame
                .windows(5)
                .any(|w| w == [0x12, 0x03, b'c', b'a', b'p'])
        );
        assert!(media_frame.contains(&0x32));

        assert!(encode_batch_embed_contents_grpc_request("m", &[], None, None, None).is_err());
        assert!(
            encode_batch_embed_contents_grpc_request("m", &[vec![]], None, None, None).is_err()
        );
        assert!(
            encode_batch_embed_contents_grpc_request(
                "m",
                &[vec![GeminiContentPart::InlineData {
                    mime_type: "",
                    data: b"x"
                }]],
                None,
                None,
                None,
            )
            .is_err()
        );
        assert!(
            encode_batch_embed_contents_grpc_request(
                "m",
                &[vec![GeminiContentPart::FileData {
                    mime_type: "video/mp4",
                    file_uri: ""
                }]],
                None,
                None,
                None,
            )
            .is_err()
        );
        assert!(
            encode_batch_embed_contents_grpc_request(
                "m",
                &[vec![GeminiContentPart::Text("a")]],
                None,
                Some(&[None, None]),
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn test_top_k_search_unaligned_query_and_checked_layout_bounds() {
        #[repr(align(4))]
        struct AlignedBytes([u8; 9]);

        let slab =
            VectorSlab::from_f32_slice(&[1.0, 0.0, 0.0, 1.0], 2, 2, true).expect("from_f32_slice");
        let mut raw = AlignedBytes([0u8; 9]);
        raw.0[1..5].copy_from_slice(&0.0f32.to_ne_bytes());
        raw.0[5..9].copy_from_slice(&1.0f32.to_ne_bytes());
        let matches = top_k_search(&raw.0[1..9], &slab, SimilarityMetric::Dot, 1)
            .expect("unaligned query search");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].index, 1);

        assert!(VectorSlab::compute_layout(usize::MAX, 4, VectorDtype::Float32, false).is_err());
        assert!(VectorSlab::compute_layout(1, usize::MAX, VectorDtype::Float32, false).is_err());
        assert!(VectorCache::new_in_memory(usize::MAX, 4).is_err());
        assert!(VectorCache::new_in_memory(1, usize::MAX).is_err());
    }
}
