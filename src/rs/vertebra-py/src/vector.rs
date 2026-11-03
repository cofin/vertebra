//! PyO3 bindings for `vertebra-core::vector` (`VectorView`, `VectorBatchView`, `SimilarityMatch`,
//! `VectorCache`, `StaticEmbedderCore`, Gemma prompt splicer, and gRPC packed-float wire helpers).

use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::{c_int, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::exceptions::{PyIndexError, PyRuntimeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use vertebra_core::vector::{
    EmbeddingTask, SimilarityMetric, StaticEmbedderCore as RsStaticEmbedderCore,
    VectorCache as RsVectorCache, VectorDtype, VectorSlab, compute_vector_cache_key,
    cosine_similarity_f32, decode_batch_embed_grpc_response_into_slab, dot_product_f32,
    dot_product_i8, encode_batch_embed_grpc_request, encode_batch_embed_grpc_response_from_slab,
    format_gemma_prompt as rs_format_gemma_prompt, hamming_distance_binary, l2_norm_f32,
    l2_normalize_in_place, mrl_truncate_and_normalize, quantize_f32_to_binary, quantize_f32_to_i8,
    splice_prefix_tokens as rs_splice_prefix_tokens, top_k_search,
};
use vertebra_db::arrow_shm::write_shm_arrow_region;
use vertebra_db::oracle::{
    encode_oracle_vector_binary as rs_encode_oracle_vector_binary,
    encode_oracle_vector_f32 as rs_encode_oracle_vector_f32,
    encode_oracle_vector_i8 as rs_encode_oracle_vector_i8,
};
use vertebra_db::postgres::{
    encode_pgvector_bit as rs_encode_pgvector_bit, encode_pgvector_f32 as rs_encode_pgvector_f32,
};

use crate::db::ArrowCursor;

static ARROW_SHM_SEQ: AtomicU64 = AtomicU64::new(1);

#[inline]
const fn pep3118_format_cstr(dtype: VectorDtype) -> &'static CStr {
    match dtype {
        VectorDtype::Float32 => c"f",
        VectorDtype::Float64 => c"d",
        VectorDtype::Float16 => c"e",
        VectorDtype::Int8 => c"b",
        VectorDtype::Binary => c"B",
    }
}

/// Top-k similarity search match containing `(index, score)`.
#[pyclass(module = "vertebra._vertebra", frozen, immutable_type, from_py_object)]
#[derive(Clone, Copy, Debug)]
pub struct SimilarityMatch {
    #[pyo3(get)]
    pub index: usize,
    #[pyo3(get)]
    pub score: f32,
}

#[pymethods]
impl SimilarityMatch {
    #[new]
    pub fn new(index: usize, score: f32) -> Self {
        Self { index, score }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "SimilarityMatch(index={}, score={:.6})",
            self.index, self.score
        )
    }
}

/// Zero-copy 1D PEP 3118 vector view backed by a `VectorSlab` row.
#[pyclass(module = "vertebra._vertebra", frozen, immutable_type)]
pub struct VectorView {
    pub(crate) slab: Arc<VectorSlab>,
    pub(crate) row_index: usize,
    shape_storage: [ffi::Py_ssize_t; 1],
    stride_storage: [ffi::Py_ssize_t; 1],
}

impl VectorView {
    pub fn from_slab_row(slab: Arc<VectorSlab>, row_index: usize) -> PyResult<Self> {
        if row_index >= slab.count() {
            return Err(PyIndexError::new_err(format!(
                "row_index {row_index} out of range for VectorSlab of count {}",
                slab.count()
            )));
        }
        let dtype = slab.dtype();
        let (shape0, stride0) = match dtype {
            VectorDtype::Binary => (
                slab.packed_row_bytes() as ffi::Py_ssize_t,
                1 as ffi::Py_ssize_t,
            ),
            other => (
                slab.dimensions() as ffi::Py_ssize_t,
                other.elem_bytes() as ffi::Py_ssize_t,
            ),
        };
        Ok(Self {
            slab,
            row_index,
            shape_storage: [shape0],
            stride_storage: [stride0],
        })
    }

    #[inline]
    pub fn row_bytes(&self) -> PyResult<&[u8]> {
        self.slab
            .row_bytes(self.row_index)
            .map_err(PyRuntimeError::new_err)
    }

    #[inline]
    pub fn row_f32(&self) -> PyResult<&[f32]> {
        self.slab
            .row_f32(self.row_index)
            .map_err(PyValueError::new_err)
    }

    #[inline]
    pub fn row_i8(&self) -> PyResult<&[i8]> {
        self.slab
            .row_i8(self.row_index)
            .map_err(PyValueError::new_err)
    }
}

#[pymethods]
impl VectorView {
    #[staticmethod]
    #[pyo3(signature = (values, normalize=false))]
    pub fn from_f32(values: Vec<f32>, normalize: bool) -> PyResult<Self> {
        if values.is_empty() {
            return Err(PyValueError::new_err("vector values must not be empty"));
        }
        let dims = values.len();
        let mut slab =
            VectorSlab::from_f32_slice(&values, 1, dims, false).map_err(PyValueError::new_err)?;
        if normalize {
            let row = slab.row_f32_mut(0).map_err(PyValueError::new_err)?;
            l2_normalize_in_place(row);
        }
        Self::from_slab_row(Arc::new(slab), 0)
    }

    #[staticmethod]
    #[pyo3(signature = (data, dimensions, dtype="float32", scale=1.0))]
    pub fn from_bytes(data: &[u8], dimensions: usize, dtype: &str, scale: f32) -> PyResult<Self> {
        let parsed_dtype = VectorDtype::parse(dtype)
            .ok_or_else(|| PyValueError::new_err(format!("unsupported vector dtype '{dtype}'")))?;
        let mut slab = VectorSlab::new_in_memory(1, dimensions, parsed_dtype, false)
            .map_err(PyValueError::new_err)?;
        if data.len() != slab.packed_row_bytes() {
            return Err(PyValueError::new_err(format!(
                "expected {} bytes for ({dimensions}, {}), got {}",
                slab.packed_row_bytes(),
                parsed_dtype.as_str(),
                data.len()
            )));
        }
        slab.row_bytes_mut(0)
            .map_err(PyValueError::new_err)?
            .copy_from_slice(data);
        if parsed_dtype == VectorDtype::Int8 {
            slab.set_scale_at(0, scale);
        }
        Self::from_slab_row(Arc::new(slab), 0)
    }

    #[getter]
    pub fn dimensions(&self) -> usize {
        self.slab.dimensions()
    }

    #[getter]
    pub fn dtype(&self) -> &'static str {
        self.slab.dtype().as_str()
    }

    #[getter]
    pub fn nbytes(&self) -> usize {
        self.slab.packed_row_bytes()
    }

    #[getter]
    pub fn scale(&self) -> f32 {
        self.slab.scale_at(self.row_index)
    }

    pub fn __len__(&self) -> usize {
        self.slab.dimensions()
    }

    pub fn norm(&self) -> PyResult<f32> {
        let slice = self.row_f32()?;
        Ok(l2_norm_f32(slice))
    }

    #[pyo3(signature = (dimensions, normalize=true))]
    pub fn truncate(&self, dimensions: usize, normalize: bool) -> PyResult<Self> {
        let src = self.row_f32()?;
        if dimensions == 0 || dimensions > src.len() {
            return Err(PyValueError::new_err(format!(
                "target dimensions {dimensions} must be in 1..={}",
                src.len()
            )));
        }
        let mut dst_slab = VectorSlab::new_in_memory(1, dimensions, VectorDtype::Float32, false)
            .map_err(PyValueError::new_err)?;
        let dst = dst_slab.row_f32_mut(0).map_err(PyValueError::new_err)?;
        if normalize {
            mrl_truncate_and_normalize(src, dimensions, dst).map_err(PyValueError::new_err)?;
        } else {
            dst.copy_from_slice(&src[..dimensions]);
        }
        Self::from_slab_row(Arc::new(dst_slab), 0)
    }

    pub fn quantize_int8(&self) -> PyResult<Self> {
        let src = self.row_f32()?;
        let dims = src.len();
        let mut dst_slab = VectorSlab::new_in_memory(1, dims, VectorDtype::Int8, false)
            .map_err(PyValueError::new_err)?;
        let dst = dst_slab.row_i8_mut(0).map_err(PyValueError::new_err)?;
        let scale = quantize_f32_to_i8(src, dst).map_err(PyValueError::new_err)?;
        dst_slab.set_scale_at(0, scale);
        Self::from_slab_row(Arc::new(dst_slab), 0)
    }

    pub fn quantize_binary(&self) -> PyResult<Self> {
        let src = self.row_f32()?;
        let dims = src.len();
        let mut dst_slab = VectorSlab::new_in_memory(1, dims, VectorDtype::Binary, false)
            .map_err(PyValueError::new_err)?;
        let dst = dst_slab.row_bytes_mut(0).map_err(PyValueError::new_err)?;
        quantize_f32_to_binary(src, dst).map_err(PyValueError::new_err)?;
        Self::from_slab_row(Arc::new(dst_slab), 0)
    }

    pub fn cosine_similarity(&self, other: &VectorView) -> PyResult<f32> {
        if self.slab.dimensions() != other.slab.dimensions() {
            return Err(PyValueError::new_err("vector dimensions must match"));
        }
        let a = self.row_f32()?;
        let b = other.row_f32()?;
        Ok(cosine_similarity_f32(a, b))
    }

    pub fn dot(&self, other: &VectorView) -> PyResult<f32> {
        if self.slab.dimensions() != other.slab.dimensions() {
            return Err(PyValueError::new_err("vector dimensions must match"));
        }
        match (self.slab.dtype(), other.slab.dtype()) {
            (VectorDtype::Float32, VectorDtype::Float32) => {
                Ok(dot_product_f32(self.row_f32()?, other.row_f32()?))
            }
            (VectorDtype::Int8, VectorDtype::Int8) => {
                let raw = dot_product_i8(self.row_i8()?, other.row_i8()?) as f32;
                Ok(raw * self.scale() * other.scale())
            }
            _ => Err(PyValueError::new_err(
                "dot requires matching float32 or int8 VectorViews",
            )),
        }
    }

    pub fn hamming_distance(&self, other: &VectorView) -> PyResult<u32> {
        if self.slab.dtype() != VectorDtype::Binary || other.slab.dtype() != VectorDtype::Binary {
            return Err(PyValueError::new_err(
                "hamming_distance requires binary VectorViews",
            ));
        }
        if self.slab.dimensions() != other.slab.dimensions() {
            return Err(PyValueError::new_err("vector dimensions must match"));
        }
        Ok(hamming_distance_binary(
            self.row_bytes()?,
            other.row_bytes()?,
        ))
    }

    pub fn to_list(&self) -> PyResult<Vec<f32>> {
        match self.slab.dtype() {
            VectorDtype::Float32 => Ok(self.row_f32()?.to_vec()),
            VectorDtype::Int8 => {
                let scale = self.scale();
                Ok(self.row_i8()?.iter().map(|&v| (v as f32) * scale).collect())
            }
            other => Err(PyValueError::new_err(format!(
                "to_list is not supported for vector dtype {}",
                other.as_str()
            ))),
        }
    }

    pub fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.row_bytes()?;
        PyBytes::new_with(py, bytes.len(), |buf| {
            buf.copy_from_slice(bytes);
            Ok(())
        })
    }

    pub fn to_pgvector_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let encoded = match self.slab.dtype() {
            VectorDtype::Float32 => {
                rs_encode_pgvector_f32(self.row_f32()?).map_err(PyValueError::new_err)?
            }
            VectorDtype::Binary => {
                rs_encode_pgvector_bit(self.row_bytes()?, self.slab.dimensions())
                    .map_err(PyValueError::new_err)?
            }
            other => {
                return Err(PyValueError::new_err(format!(
                    "to_pgvector_bytes is not supported for vector dtype {}",
                    other.as_str()
                )));
            }
        };
        PyBytes::new_with(py, encoded.len(), |buf| {
            buf.copy_from_slice(&encoded);
            Ok(())
        })
    }

    pub fn to_oracle_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let encoded = match self.slab.dtype() {
            VectorDtype::Float32 => rs_encode_oracle_vector_f32(self.row_f32()?),
            VectorDtype::Int8 => rs_encode_oracle_vector_i8(self.row_i8()?),
            VectorDtype::Binary => rs_encode_oracle_vector_binary(self.row_bytes()?),
            other => {
                return Err(PyValueError::new_err(format!(
                    "to_oracle_bytes is not supported for vector dtype {}",
                    other.as_str()
                )));
            }
        };
        PyBytes::new_with(py, encoded.len(), |buf| {
            buf.copy_from_slice(&encoded);
            Ok(())
        })
    }

    pub fn to_sqlite_blob<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.to_bytes(py)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        let slice = this.row_bytes()?;
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr(),
            slice.as_ptr() as *mut c_void,
            slice.len() as ffi::Py_ssize_t,
            1,
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(py));
        }
        let dtype = this.slab.dtype();
        (*view).itemsize = dtype.elem_bytes() as ffi::Py_ssize_t;
        if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            (*view).format = pep3118_format_cstr(dtype).as_ptr().cast_mut();
        }
        (*view).ndim = 1;
        if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            (*view).shape = (&raw const this.shape_storage[0]).cast_mut();
        }
        if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            (*view).strides = (&raw const this.stride_storage[0]).cast_mut();
        }
        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}

    pub fn __repr__(&self) -> String {
        format!(
            "VectorView(dimensions={}, dtype={:?})",
            self.slab.dimensions(),
            self.slab.dtype().as_str()
        )
    }
}

/// Zero-copy 2D PEP 3118 vector batch view backed by a shared-memory or 64-byte-aligned `VectorSlab`.
#[pyclass(module = "vertebra._vertebra", frozen, immutable_type)]
pub struct VectorBatchView {
    pub(crate) slab: Arc<VectorSlab>,
    shape_storage: [ffi::Py_ssize_t; 2],
    stride_storage: [ffi::Py_ssize_t; 2],
}

impl VectorBatchView {
    pub fn from_slab(slab: Arc<VectorSlab>) -> Self {
        let dtype = slab.dtype();
        let count = slab.count() as ffi::Py_ssize_t;
        let (dim_shape, col_stride) = match dtype {
            VectorDtype::Binary => (
                slab.packed_row_bytes() as ffi::Py_ssize_t,
                1 as ffi::Py_ssize_t,
            ),
            other => (
                slab.dimensions() as ffi::Py_ssize_t,
                other.elem_bytes() as ffi::Py_ssize_t,
            ),
        };
        let row_stride = slab.row_stride_bytes() as ffi::Py_ssize_t;
        Self {
            slab,
            shape_storage: [count, dim_shape],
            stride_storage: [row_stride, col_stride],
        }
    }
}

#[pymethods]
impl VectorBatchView {
    #[staticmethod]
    #[pyo3(signature = (rows, normalize=false, shm=false))]
    pub fn from_f32_rows(
        py: Python<'_>,
        rows: Vec<Vec<f32>>,
        normalize: bool,
        shm: bool,
    ) -> PyResult<Self> {
        if rows.is_empty() {
            return Err(PyValueError::new_err("rows must not be empty"));
        }
        let dims = rows[0].len();
        if dims == 0 {
            return Err(PyValueError::new_err("vector dimensions must be > 0"));
        }
        let count = rows.len();
        let slab = py
            .detach(|| -> Result<VectorSlab, String> {
                let mut s = if shm {
                    VectorSlab::create_shm(None, count, dims, VectorDtype::Float32, false)?
                } else {
                    VectorSlab::new_in_memory(count, dims, VectorDtype::Float32, false)?
                };
                for (i, row) in rows.iter().enumerate() {
                    if row.len() != dims {
                        return Err(format!(
                            "row {i} has dimension {} != expected {dims}",
                            row.len()
                        ));
                    }
                    let dst = s.row_f32_mut(i)?;
                    dst.copy_from_slice(row);
                    if normalize {
                        l2_normalize_in_place(dst);
                    }
                }
                Ok(s)
            })
            .map_err(PyValueError::new_err)?;
        Ok(Self::from_slab(Arc::new(slab)))
    }

    #[staticmethod]
    #[pyo3(signature = (values, count, dimensions, normalize=false, shm=false))]
    pub fn from_flat_f32(
        py: Python<'_>,
        values: Vec<f32>,
        count: usize,
        dimensions: usize,
        normalize: bool,
        shm: bool,
    ) -> PyResult<Self> {
        let slab = py
            .detach(|| -> Result<VectorSlab, String> {
                let mut s = if shm {
                    let mut created = VectorSlab::create_shm(
                        None,
                        count,
                        dimensions,
                        VectorDtype::Float32,
                        false,
                    )?;
                    for i in 0..count {
                        created
                            .row_f32_mut(i)?
                            .copy_from_slice(&values[i * dimensions..(i + 1) * dimensions]);
                    }
                    created
                } else {
                    VectorSlab::from_f32_slice(&values, count, dimensions, false)?
                };
                if normalize {
                    for i in 0..count {
                        l2_normalize_in_place(s.row_f32_mut(i)?);
                    }
                }
                Ok(s)
            })
            .map_err(PyValueError::new_err)?;
        Ok(Self::from_slab(Arc::new(slab)))
    }

    #[staticmethod]
    #[pyo3(signature = (name, rows, normalize=false))]
    pub fn create_shm(
        py: Python<'_>,
        name: &str,
        rows: Vec<Vec<f32>>,
        normalize: bool,
    ) -> PyResult<Self> {
        if rows.is_empty() {
            return Err(PyValueError::new_err("rows must not be empty"));
        }
        let count = rows.len();
        let dims = rows[0].len();
        let name_owned = name.to_string();
        let slab = py
            .detach(move || -> Result<VectorSlab, String> {
                let mut s = VectorSlab::create_shm(
                    Some(&name_owned),
                    count,
                    dims,
                    VectorDtype::Float32,
                    false,
                )?;
                for (i, row) in rows.iter().enumerate() {
                    if row.len() != dims {
                        return Err(format!(
                            "row {i} has dimension {} != expected {dims}",
                            row.len()
                        ));
                    }
                    let dst = s.row_f32_mut(i)?;
                    dst.copy_from_slice(row);
                    if normalize {
                        l2_normalize_in_place(dst);
                    }
                }
                Ok(s)
            })
            .map_err(PyRuntimeError::new_err)?;
        Ok(Self::from_slab(Arc::new(slab)))
    }

    #[staticmethod]
    #[pyo3(signature = (name, min_size=4096))]
    pub fn open_shm(py: Python<'_>, name: &str, min_size: usize) -> PyResult<Self> {
        #[cfg(unix)]
        {
            let name_owned = name.to_string();
            let slab = py
                .detach(move || VectorSlab::open_shm(&name_owned, min_size))
                .map_err(PyRuntimeError::new_err)?;
            Ok(Self::from_slab(Arc::new(slab)))
        }
        #[cfg(not(unix))]
        {
            let _ = (py, name, min_size);
            Err(PyRuntimeError::new_err(
                "open_shm is only supported on Unix platforms",
            ))
        }
    }

    #[getter]
    pub fn count(&self) -> usize {
        self.slab.count()
    }

    #[getter]
    pub fn dimensions(&self) -> usize {
        self.slab.dimensions()
    }

    #[getter]
    pub fn dtype(&self) -> &'static str {
        self.slab.dtype().as_str()
    }

    #[getter]
    pub fn nbytes(&self) -> usize {
        self.slab.data_byte_len()
    }

    #[getter]
    pub fn shm_name(&self) -> Option<String> {
        self.slab.shm_name().map(str::to_string)
    }

    pub fn __len__(&self) -> usize {
        self.slab.count()
    }

    pub fn __getitem__(&self, index: isize) -> PyResult<VectorView> {
        let len = self.slab.count() as isize;
        let resolved = if index < 0 { len + index } else { index };
        if resolved < 0 || resolved >= len {
            return Err(PyIndexError::new_err(format!(
                "batch index {index} out of range for count {len}"
            )));
        }
        VectorView::from_slab_row(Arc::clone(&self.slab), resolved as usize)
    }

    #[pyo3(signature = (dimensions, normalize=true))]
    pub fn truncate(&self, py: Python<'_>, dimensions: usize, normalize: bool) -> PyResult<Self> {
        let slab_ref = Arc::clone(&self.slab);
        let new_slab = py
            .detach(move || -> Result<VectorSlab, String> {
                if slab_ref.dtype() != VectorDtype::Float32 {
                    return Err("truncate requires Float32 VectorBatchView".to_string());
                }
                if dimensions == 0 || dimensions > slab_ref.dimensions() {
                    return Err(format!(
                        "target dimensions {dimensions} must be in 1..={}",
                        slab_ref.dimensions()
                    ));
                }
                let mut dst_slab = VectorSlab::new_in_memory(
                    slab_ref.count(),
                    dimensions,
                    VectorDtype::Float32,
                    false,
                )?;
                for i in 0..slab_ref.count() {
                    let src = slab_ref.row_f32(i)?;
                    let dst = dst_slab.row_f32_mut(i)?;
                    if normalize {
                        mrl_truncate_and_normalize(src, dimensions, dst)?;
                    } else {
                        dst.copy_from_slice(&src[..dimensions]);
                    }
                }
                Ok(dst_slab)
            })
            .map_err(PyValueError::new_err)?;
        Ok(Self::from_slab(Arc::new(new_slab)))
    }

    pub fn quantize_int8(&self, py: Python<'_>) -> PyResult<Self> {
        let slab_ref = Arc::clone(&self.slab);
        let new_slab = py
            .detach(move || -> Result<VectorSlab, String> {
                if slab_ref.dtype() != VectorDtype::Float32 {
                    return Err("quantize_int8 requires Float32 VectorBatchView".to_string());
                }
                let mut dst_slab = VectorSlab::new_in_memory(
                    slab_ref.count(),
                    slab_ref.dimensions(),
                    VectorDtype::Int8,
                    false,
                )?;
                for i in 0..slab_ref.count() {
                    let src = slab_ref.row_f32(i)?;
                    let dst = dst_slab.row_i8_mut(i)?;
                    let scale = quantize_f32_to_i8(src, dst)?;
                    dst_slab.set_scale_at(i, scale);
                }
                Ok(dst_slab)
            })
            .map_err(PyValueError::new_err)?;
        Ok(Self::from_slab(Arc::new(new_slab)))
    }

    pub fn quantize_binary(&self, py: Python<'_>) -> PyResult<Self> {
        let slab_ref = Arc::clone(&self.slab);
        let new_slab = py
            .detach(move || -> Result<VectorSlab, String> {
                if slab_ref.dtype() != VectorDtype::Float32 {
                    return Err("quantize_binary requires Float32 VectorBatchView".to_string());
                }
                let mut dst_slab = VectorSlab::new_in_memory(
                    slab_ref.count(),
                    slab_ref.dimensions(),
                    VectorDtype::Binary,
                    false,
                )?;
                for i in 0..slab_ref.count() {
                    let src = slab_ref.row_f32(i)?;
                    let dst = dst_slab.row_bytes_mut(i)?;
                    quantize_f32_to_binary(src, dst)?;
                }
                Ok(dst_slab)
            })
            .map_err(PyValueError::new_err)?;
        Ok(Self::from_slab(Arc::new(new_slab)))
    }

    #[pyo3(signature = (query, k=10, metric="cosine"))]
    pub fn top_k(
        &self,
        py: Python<'_>,
        query: &VectorView,
        k: usize,
        metric: &str,
    ) -> PyResult<Vec<SimilarityMatch>> {
        let parsed_metric = SimilarityMetric::parse(metric)
            .ok_or_else(|| PyValueError::new_err(format!("unsupported metric '{metric}'")))?;
        let query_bytes = query.row_bytes()?.to_vec();
        let slab_ref = Arc::clone(&self.slab);
        let matches = py
            .detach(move || top_k_search(&query_bytes, &slab_ref, parsed_metric, k))
            .map_err(PyValueError::new_err)?;
        Ok(matches
            .into_iter()
            .map(|m| SimilarityMatch::new(m.index, m.score))
            .collect())
    }

    pub fn to_list(&self) -> PyResult<Vec<Vec<f32>>> {
        let mut out = Vec::with_capacity(self.slab.count());
        for i in 0..self.slab.count() {
            let v = VectorView::from_slab_row(Arc::clone(&self.slab), i)?;
            out.push(v.to_list()?);
        }
        Ok(out)
    }

    pub fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let len = self.slab.data_byte_len();
        let ptr = self.slab.data_ptr();
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        PyBytes::new_with(py, len, |buf| {
            buf.copy_from_slice(slice);
            Ok(())
        })
    }

    #[pyo3(signature = (column_name="embedding", *, shm_name=None))]
    pub fn to_arrow_shm(
        &self,
        py: Python<'_>,
        column_name: &str,
        shm_name: Option<&str>,
    ) -> PyResult<ArrowCursor> {
        let resolved_col = column_name;
        let resolved_shm_name = match shm_name {
            Some(explicit) => explicit.to_string(),
            None => {
                let seq = ARROW_SHM_SEQ.fetch_add(1, Ordering::Relaxed);
                format!("/vtb-vec-arrow-{}-{seq}", std::process::id())
            }
        };
        if self.slab.dtype() == VectorDtype::Float32
            && let Ok(pa) = py.import("pyarrow")
        {
            let count = self.slab.count();
            let dims = self.slab.dimensions();
            let flat_byte_len = count * dims * 4;
            let flat_pybytes = PyBytes::new_with(py, flat_byte_len, |buf| {
                for i in 0..count {
                    let row = self.slab.row_bytes(i).map_err(PyValueError::new_err)?;
                    buf[i * dims * 4..(i + 1) * dims * 4].copy_from_slice(&row[..dims * 4]);
                }
                Ok(())
            })?;
            let f32_type = pa.call_method0("float32")?;
            let pa_buf = pa.call_method1("py_buffer", (flat_pybytes,))?;
            let buffers = pyo3::types::PyList::new(py, [py.None(), pa_buf.unbind()])?;
            let array_cls = pa.getattr("Array")?;
            let flat_arr =
                array_cls.call_method1("from_buffers", (f32_type, count * dims, buffers))?;
            let fsl_cls = pa.getattr("FixedSizeListArray")?;
            let fsl_arr = fsl_cls.call_method1("from_arrays", (flat_arr, dims))?;
            let rb_cls = pa.getattr("RecordBatch")?;
            let batch = rb_cls.call_method1("from_arrays", ([fsl_arr], [resolved_col]))?;
            let sink = pa.call_method0("BufferOutputStream")?;
            let ipc_mod = pa.getattr("ipc")?;
            let schema = batch.getattr("schema")?;
            let writer = ipc_mod.call_method1("new_stream", (&sink, schema))?;
            writer.call_method1("write_batch", (&batch,))?;
            writer.call_method0("close")?;
            let pa_out_buf = sink.call_method0("getvalue")?;
            let ipc_pybytes: Bound<'_, PyBytes> =
                pa_out_buf.call_method0("to_pybytes")?.cast_into()?;
            let ipc_vec = ipc_pybytes.as_bytes().to_vec();
            let row_count = self.slab.count();
            let (norm_name, byte_len, region) = py
                .detach(move || write_shm_arrow_region(&resolved_shm_name, &ipc_vec))
                .map_err(PyRuntimeError::new_err)?;
            return Ok(ArrowCursor::from_shm_region(
                norm_name, byte_len, row_count, region,
            ));
        }
        let slab_ref = Arc::clone(&self.slab);
        let row_count = slab_ref.count();
        let col_owned = resolved_col.to_string();
        let (norm_name, byte_len, region) = py
            .detach(
                move || -> Result<(String, usize, Arc<vertebra_core::shm::ShmRegion>), String> {
                    let mut payload = Vec::with_capacity(64 + slab_ref.data_byte_len());
                    payload.extend_from_slice(b"VTBVECAR");
                    payload.extend_from_slice(&(slab_ref.count() as u32).to_le_bytes());
                    payload.extend_from_slice(&(slab_ref.dimensions() as u32).to_le_bytes());
                    payload.push(slab_ref.dtype() as u8);
                    let col_bytes = col_owned.as_bytes();
                    payload.extend_from_slice(&(col_bytes.len() as u16).to_le_bytes());
                    payload.extend_from_slice(col_bytes);
                    for i in 0..slab_ref.count() {
                        payload.extend_from_slice(slab_ref.row_bytes(i)?);
                    }
                    write_shm_arrow_region(&resolved_shm_name, &payload)
                },
            )
            .map_err(PyRuntimeError::new_err)?;
        Ok(ArrowCursor::from_shm_region(
            norm_name, byte_len, row_count, region,
        ))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        let ptr = this.slab.data_ptr();
        let byte_len = this.slab.data_byte_len();
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr(),
            ptr as *mut c_void,
            byte_len as ffi::Py_ssize_t,
            1,
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(py));
        }
        let dtype = this.slab.dtype();
        (*view).itemsize = dtype.elem_bytes() as ffi::Py_ssize_t;
        if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            (*view).format = pep3118_format_cstr(dtype).as_ptr().cast_mut();
        }
        (*view).ndim = 2;
        if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            (*view).shape = (&raw const this.shape_storage[0]).cast_mut();
        }
        if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            (*view).strides = (&raw const this.stride_storage[0]).cast_mut();
        }
        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}

    pub fn __repr__(&self) -> String {
        format!(
            "VectorBatchView(count={}, dimensions={}, dtype={:?})",
            self.slab.count(),
            self.slab.dimensions(),
            self.slab.dtype().as_str()
        )
    }
}

/// Shared-memory seqlock vector cache for `< 200 ns` exact FNV-1a lookups and off-GIL SIMD top-k search.
#[pyclass(module = "vertebra._vertebra", frozen, immutable_type)]
pub struct VectorCache {
    inner: Arc<RsVectorCache>,
}

#[pymethods]
impl VectorCache {
    #[new]
    #[pyo3(signature = (capacity=4096, dimensions=768, shm_name=None))]
    pub fn new(capacity: usize, dimensions: usize, shm_name: Option<&str>) -> PyResult<Self> {
        let inner = if shm_name.is_some() {
            RsVectorCache::create_shm(shm_name, capacity, dimensions)
        } else {
            RsVectorCache::new_in_memory(capacity, dimensions)
        }
        .map_err(PyValueError::new_err)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    pub fn dimensions(&self) -> usize {
        self.inner.dimensions()
    }

    #[getter]
    pub fn hits(&self) -> u64 {
        self.inner.hits()
    }

    #[getter]
    pub fn misses(&self) -> u64 {
        self.inner.misses()
    }

    pub fn lookup(&self, model: &str, task: &str, text: &str) -> PyResult<Option<VectorView>> {
        let dims = self.inner.dimensions();
        let key = compute_vector_cache_key(model, task, dims, VectorDtype::Float32, text);
        let mut slab = VectorSlab::new_in_memory(1, dims, VectorDtype::Float32, false)
            .map_err(PyRuntimeError::new_err)?;
        let dst = slab.row_f32_mut(0).map_err(PyRuntimeError::new_err)?;
        if self.inner.get_into(key, dst) {
            Ok(Some(VectorView::from_slab_row(Arc::new(slab), 0)?))
        } else {
            Ok(None)
        }
    }

    pub fn insert(&self, model: &str, task: &str, text: &str, vector: &VectorView) -> PyResult<()> {
        let slice = vector.row_f32()?;
        let key = compute_vector_cache_key(model, task, slice.len(), VectorDtype::Float32, text);
        self.inner.put(key, slice).map_err(PyValueError::new_err)
    }

    #[pyo3(signature = (query, k=10, metric="cosine"))]
    pub fn similarity_search(
        &self,
        py: Python<'_>,
        query: &VectorView,
        k: usize,
        metric: &str,
    ) -> PyResult<Vec<SimilarityMatch>> {
        let parsed_metric = SimilarityMetric::parse(metric)
            .ok_or_else(|| PyValueError::new_err(format!("unsupported metric '{metric}'")))?;
        let q_vec = query.row_f32()?.to_vec();
        let cache_ref = Arc::clone(&self.inner);
        let matches = py
            .detach(move || cache_ref.similarity_search(&q_vec, k, parsed_metric))
            .map_err(PyValueError::new_err)?;
        Ok(matches
            .into_iter()
            .map(|m| SimilarityMatch::new(m.index, m.score))
            .collect())
    }
}

/// Pure-Rust static token embedder (`< 10 us` mean pooling + MRL truncation + L2 normalization).
#[pyclass(module = "vertebra._vertebra", frozen, immutable_type)]
pub struct StaticEmbedderCore {
    inner: Arc<RsStaticEmbedderCore>,
}

#[pymethods]
impl StaticEmbedderCore {
    #[new]
    #[pyo3(signature = (vocab=None, weights=None, vocab_size=8192, dimensions=256, seed=42))]
    pub fn new(
        vocab: Option<HashMap<String, usize>>,
        weights: Option<Vec<Vec<f32>>>,
        vocab_size: usize,
        dimensions: usize,
        seed: u64,
    ) -> PyResult<Self> {
        let inner = match (vocab, weights) {
            (Some(v), Some(w)) => RsStaticEmbedderCore::from_vocab_and_weights(v, &w),
            (None, Some(w)) => RsStaticEmbedderCore::from_vocab_and_weights(HashMap::new(), &w),
            _ => RsStaticEmbedderCore::new(vocab_size, dimensions, seed),
        }
        .map_err(PyValueError::new_err)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    pub fn dimensions(&self) -> usize {
        self.inner.dimensions()
    }

    #[getter]
    pub fn vocab_size(&self) -> usize {
        self.inner.vocab_size()
    }

    #[pyo3(signature = (text, dimensions=None))]
    pub fn embed_text(
        &self,
        py: Python<'_>,
        text: &str,
        dimensions: Option<usize>,
    ) -> PyResult<VectorView> {
        let target_dim = dimensions.unwrap_or_else(|| self.inner.dimensions());
        let core_ref = Arc::clone(&self.inner);
        let text_owned = text.to_string();
        let slab = py
            .detach(move || -> Result<VectorSlab, String> {
                let mut s = VectorSlab::new_in_memory(1, target_dim, VectorDtype::Float32, false)?;
                core_ref.embed_text_into(&text_owned, target_dim, s.row_f32_mut(0)?)?;
                Ok(s)
            })
            .map_err(PyValueError::new_err)?;
        VectorView::from_slab_row(Arc::new(slab), 0)
    }

    #[pyo3(signature = (texts, dimensions=None))]
    pub fn embed_batch(
        &self,
        py: Python<'_>,
        texts: Vec<String>,
        dimensions: Option<usize>,
    ) -> PyResult<VectorBatchView> {
        let target_dim = dimensions.unwrap_or_else(|| self.inner.dimensions());
        let core_ref = Arc::clone(&self.inner);
        let slab = py
            .detach(move || core_ref.embed_batch(&texts, target_dim))
            .map_err(PyValueError::new_err)?;
        Ok(VectorBatchView::from_slab(Arc::new(slab)))
    }
}

/// Format an EmbeddingGemma prompt string (`"task: search result | query: ..."` or `"title: ... | text: ..."`).
#[pyfunction]
#[pyo3(signature = (task, text, title=None))]
pub fn format_gemma_prompt(task: &str, text: &str, title: Option<&str>) -> PyResult<String> {
    let parsed_task = EmbeddingTask::parse(task)
        .ok_or_else(|| PyValueError::new_err(format!("unsupported EmbeddingTask '{task}'")))?;
    Ok(rs_format_gemma_prompt(parsed_task, text, title))
}

/// Splice pre-tokenized task-prefix token IDs and document token IDs bounded by `max_seq_len`.
#[pyfunction]
#[pyo3(signature = (prefix_ids, doc_ids, eos_id=None, max_seq_len=2048))]
pub fn splice_prefix_tokens(
    prefix_ids: Vec<u32>,
    doc_ids: Vec<u32>,
    eos_id: Option<u32>,
    max_seq_len: usize,
) -> Vec<u32> {
    let mut out = Vec::new();
    rs_splice_prefix_tokens(&prefix_ids, &doc_ids, eos_id, max_seq_len, &mut out);
    out
}

/// Encode a `BatchEmbedContentsRequest` wrapped in a 5-byte gRPC Length-Prefixed Message frame.
#[pyfunction]
#[pyo3(signature = (model, texts, task=None, titles=None, output_dimensionality=None))]
pub fn encode_gemini_embed_grpc_frame<'py>(
    py: Python<'py>,
    model: &str,
    texts: Vec<String>,
    task: Option<&str>,
    titles: Option<Vec<Option<String>>>,
    output_dimensionality: Option<u32>,
) -> PyResult<Bound<'py, PyBytes>> {
    let task_type = match task {
        Some(t) if !t.is_empty() => Some(
            EmbeddingTask::parse(t)
                .ok_or_else(|| PyValueError::new_err(format!("unsupported task '{t}'")))?
                .gemini_proto_task_type(),
        ),
        _ => None,
    };
    let frame = encode_batch_embed_grpc_request(
        model,
        &texts,
        task_type,
        titles.as_deref(),
        output_dimensionality,
    );
    PyBytes::new_with(py, frame.len(), |buf| {
        buf.copy_from_slice(&frame);
        Ok(())
    })
}

/// Encode a `VectorBatchView` into a 5-byte gRPC LPM `BatchEmbedContentsResponse` frame.
#[pyfunction]
pub fn encode_gemini_embed_grpc_response_frame<'py>(
    py: Python<'py>,
    batch: &VectorBatchView,
) -> PyResult<Bound<'py, PyBytes>> {
    let frame =
        encode_batch_embed_grpc_response_from_slab(&batch.slab).map_err(PyValueError::new_err)?;
    PyBytes::new_with(py, frame.len(), |buf| {
        buf.copy_from_slice(&frame);
        Ok(())
    })
}

/// Decode a 5-byte gRPC LPM `BatchEmbedContentsResponse` frame directly into a `VectorBatchView`.
#[pyfunction]
#[pyo3(signature = (frame, dimensions=None, normalize=true))]
pub fn decode_gemini_embed_grpc_frame_to_batch(
    py: Python<'_>,
    frame: &[u8],
    dimensions: Option<usize>,
    normalize: bool,
) -> PyResult<VectorBatchView> {
    let frame_owned = frame.to_vec();
    let slab = py
        .detach(move || {
            decode_batch_embed_grpc_response_into_slab(&frame_owned, dimensions, normalize)
        })
        .map_err(PyValueError::new_err)?;
    Ok(VectorBatchView::from_slab(Arc::new(slab)))
}

pub fn register_vector_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SimilarityMatch>()?;
    m.add_class::<VectorView>()?;
    m.add_class::<VectorBatchView>()?;
    m.add_class::<VectorCache>()?;
    m.add_class::<StaticEmbedderCore>()?;
    m.add_function(wrap_pyfunction!(format_gemma_prompt, m)?)?;
    m.add_function(wrap_pyfunction!(splice_prefix_tokens, m)?)?;
    m.add_function(wrap_pyfunction!(encode_gemini_embed_grpc_frame, m)?)?;
    m.add_function(wrap_pyfunction!(
        encode_gemini_embed_grpc_response_frame,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        decode_gemini_embed_grpc_frame_to_batch,
        m
    )?)?;
    Ok(())
}
