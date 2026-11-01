//! Zero-copy `WsgiInput` and `WsgiFileWrapper` implementations.

use std::sync::Arc;

use bytes::Bytes;
use pyo3::exceptions::PyIOError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use vertebra_core::transport::BatchSnapshot;

enum WsgiInputSource {
    Snapshot {
        batch: Arc<BatchSnapshot>,
        index: usize,
    },
    Owned(Bytes),
}

impl WsgiInputSource {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Snapshot { batch, index } => batch.body(*index),
            Self::Owned(data) => data.as_ref(),
        }
    }
}

/// Zero-copy PEP 3333 `wsgi.input` reader backed by `BatchSnapshot` or `Bytes` without Python `io.BytesIO`.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "WsgiInput", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "WsgiInput", immutable_type))]
pub struct WsgiInput {
    source: WsgiInputSource,
    pos: usize,
}

impl WsgiInput {
    pub fn from_bytes(data: Bytes) -> Self {
        Self {
            source: WsgiInputSource::Owned(data),
            pos: 0,
        }
    }

    pub fn from_snapshot(batch: Arc<BatchSnapshot>, index: usize) -> Self {
        Self {
            source: WsgiInputSource::Snapshot { batch, index },
            pos: 0,
        }
    }
}

#[pymethods]
impl WsgiInput {
    #[new]
    #[pyo3(signature = (data=None))]
    pub fn py_new(data: Option<&[u8]>) -> Self {
        Self {
            source: WsgiInputSource::Owned(Bytes::copy_from_slice(data.unwrap_or(b""))),
            pos: 0,
        }
    }

    #[pyo3(signature = (size=-1))]
    pub fn read<'py>(&mut self, py: Python<'py>, size: isize) -> Bound<'py, PyBytes> {
        let data = self.source.as_slice();
        let remaining = data.len().saturating_sub(self.pos);
        let take = if size < 0 {
            remaining
        } else {
            (size as usize).min(remaining)
        };
        let slice = &data[self.pos..self.pos + take];
        self.pos += take;
        PyBytes::new(py, slice)
    }

    #[pyo3(signature = (size=-1))]
    pub fn readline<'py>(&mut self, py: Python<'py>, size: isize) -> Bound<'py, PyBytes> {
        let data = self.source.as_slice();
        if self.pos >= data.len() {
            return PyBytes::new(py, b"");
        }
        let max_end = if size < 0 {
            data.len()
        } else {
            data.len().min(self.pos + (size as usize))
        };
        let window = &data[self.pos..max_end];
        let take = match window.iter().position(|&b| b == b'\n') {
            Some(idx) => idx + 1,
            None => window.len(),
        };
        let slice = &data[self.pos..self.pos + take];
        self.pos += take;
        PyBytes::new(py, slice)
    }

    #[pyo3(signature = (hint=-1))]
    pub fn readlines<'py>(&mut self, py: Python<'py>, hint: isize) -> PyResult<Bound<'py, PyList>> {
        let mut lines = Vec::new();
        let mut total_bytes = 0usize;
        loop {
            let line = self.readline(py, -1);
            if line.as_bytes().is_empty() {
                break;
            }
            total_bytes += line.as_bytes().len();
            lines.push(line.into_any());
            if hint > 0 && total_bytes >= hint as usize {
                break;
            }
        }
        PyList::new(py, &lines)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        let line = self.readline(py, -1);
        if line.as_bytes().is_empty() {
            None
        } else {
            Some(line)
        }
    }

    /// Return a zero-copy read-only Python `memoryview` anchored to this `WsgiInput`.
    #[getter]
    pub fn body_view(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        unsafe {
            let mv = ffi::PyMemoryView_FromObject(slf.as_ptr());
            if mv.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Bound::from_owned_ptr(py, mv).unbind())
        }
    }

    /// Decode the unread slice of this `WsgiInput` directly via `msgspec` without a `PyBytes` copy.
    #[pyo3(signature = (target_type=None, format="json"))]
    pub fn decode_msgspec<'py>(
        &mut self,
        py: Python<'py>,
        target_type: Option<&Bound<'py, PyAny>>,
        format: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let data = self.source.as_slice();
        let remaining = &data[self.pos.min(data.len())..];
        self.pos = data.len();
        crate::msgspec_bridge::decode_msgspec_from_slice(py, remaining, target_type, format)
    }

    /// Decode the unread slice of this `WsgiInput` directly via `pydantic-core` `validate_json`.
    pub fn decode_pydantic_json<'py>(
        slf: Bound<'py, Self>,
        validator_or_type: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let mut borrow = slf.borrow_mut();
        let slice_vec = {
            let data = borrow.source.as_slice();
            let rem = &data[borrow.pos.min(data.len())..];
            rem.to_vec()
        };
        borrow.pos = borrow.source.as_slice().len();
        drop(borrow);
        crate::msgspec_bridge::decode_pydantic_from_buffer(
            py,
            slf.as_ptr(),
            validator_or_type,
            &slice_vec,
        )
    }

    /// Decode a unary 5-byte LPM gRPC body from `WsgiInput` in zero copy.
    #[pyo3(signature = (target_type=None, format="msgpack"))]
    pub fn decode_grpc<'py>(
        &mut self,
        py: Python<'py>,
        target_type: Option<&Bound<'py, PyAny>>,
        format: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let data = self.source.as_slice();
        let remaining = &data[self.pos.min(data.len())..];
        self.pos = data.len();
        crate::msgspec_bridge::decode_grpc_unary_from_slice(py, remaining, target_type, format)
    }

    /// Decode all concatenated 5-byte LPM gRPC frames in `WsgiInput` into a Python `list` in zero copy.
    #[pyo3(signature = (target_type=None, format="msgpack"))]
    pub fn decode_grpc_frames<'py>(
        &mut self,
        py: Python<'py>,
        target_type: Option<&Bound<'py, PyAny>>,
        format: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        let data = self.source.as_slice();
        let remaining = &data[self.pos.min(data.len())..];
        self.pos = data.len();
        crate::msgspec_bridge::decode_grpc_frames_from_slice(py, remaining, target_type, format)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: std::os::raw::c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        let borrow = slf.borrow();
        let data = borrow.source.as_slice();
        let slice = &data[borrow.pos.min(data.len())..];
        let ptr = if slice.is_empty() {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            slice.as_ptr()
        };
        let len = slice.len() as ffi::Py_ssize_t;
        drop(borrow);
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr(),
            ptr as *mut std::os::raw::c_void,
            len,
            1,
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(py));
        }
        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

/// PEP 3333 `wsgi.file_wrapper` implementation resolving filesystem paths for Rust file reads.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "WsgiFileWrapper", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "WsgiFileWrapper", immutable_type))]
pub struct WsgiFileWrapper {
    filelike: Py<PyAny>,
    block_size: usize,
    path: Option<String>,
}

#[pymethods]
impl WsgiFileWrapper {
    #[new]
    #[pyo3(signature = (filelike, block_size=65536))]
    pub fn py_new(filelike: &Bound<'_, PyAny>, block_size: usize) -> Self {
        let path = filelike
            .getattr("name")
            .ok()
            .and_then(|n| n.extract::<String>().ok())
            .filter(|p| std::path::Path::new(p).is_file());
        Self {
            filelike: filelike.clone().unbind(),
            block_size: block_size.max(4096),
            path,
        }
    }

    #[getter]
    pub fn path(&self) -> Option<String> {
        self.path.clone()
    }

    #[getter]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    #[getter]
    pub fn filelike(&self, py: Python<'_>) -> Py<PyAny> {
        self.filelike.clone_ref(py)
    }

    /// Read the entire wrapped file in Rust releasing the GIL when backed by a filesystem path.
    pub fn read_all_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if let Some(ref path) = self.path {
            let path_clone = path.clone();
            let bytes = py
                .detach(|| std::fs::read(&path_clone))
                .map_err(|e| PyIOError::new_err(format!("failed to read {path}: {e}")))?;
            return Ok(PyBytes::new(py, &bytes));
        }

        let file = self.filelike.bind(py);
        if file.hasattr("seek")? {
            let _ = file.call_method1("seek", (0i64,))?;
        }
        let mut chunks: Vec<u8> = Vec::new();
        loop {
            let chunk_obj = file.call_method1("read", (self.block_size,))?;
            let chunk_bytes = if let Ok(b) = chunk_obj.cast::<PyBytes>() {
                b.as_bytes()
            } else if let Ok(s) = chunk_obj.extract::<&[u8]>() {
                s
            } else {
                break;
            };
            if chunk_bytes.is_empty() {
                break;
            }
            chunks.extend_from_slice(chunk_bytes);
        }
        Ok(PyBytes::new(py, &chunks))
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let file = self.filelike.bind(py);
        if file.hasattr("close")? {
            file.call_method0("close")?;
        }
        Ok(())
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let file = self.filelike.bind(py);
        let chunk_obj = file.call_method1("read", (self.block_size,))?;
        let chunk_bytes = if let Ok(b) = chunk_obj.cast::<PyBytes>() {
            b.as_bytes().to_vec()
        } else if let Ok(s) = chunk_obj.extract::<&[u8]>() {
            s.to_vec()
        } else if let Ok(st) = chunk_obj.extract::<&str>() {
            st.as_bytes().to_vec()
        } else {
            Vec::new()
        };
        if chunk_bytes.is_empty() {
            let _ = self.close(py);
            Ok(None)
        } else {
            Ok(Some(PyBytes::new(py, &chunk_bytes)))
        }
    }
}
