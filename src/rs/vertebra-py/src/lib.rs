//! PyO3 bindings for Vertebra.
//!
//! This module is imported as `vertebra._vertebra` in Python and provides
//! access to high-performance Rust-based functionality.

use pyo3::exceptions::PyBufferError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyBytes;
use pyo3::types::PyMemoryView;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use pyo3::{ffi, IntoPyObject};

use std::ffi::{c_int, c_void, CString};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use vertebra_core::buffer::{Buffer, BufferPool, RingLayout};
use vertebra_core::channel::{BufferChannel, BufferMessage, ChannelError, MpmcRing, SlotView, SpscRing};
use vertebra_core::router::{RouteId, Router};
use vertebra_core::shm::ShmRegion;

mod async_ring;
mod supervisor;

#[pyclass(name = "Router")]
struct PyRouter {
    inner: Router,
}

#[pymethods]
impl PyRouter {
    #[new]
    fn new() -> Self {
        Self {
            inner: Router::new(),
        }
    }

    fn insert(&mut self, path: &str, id: u64) -> PyResult<()> {
        self.inner
            .insert(path, RouteId(id))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    fn match_path(&self, path: &str) -> Option<u64> {
        self.inner.match_path(path).map(|(id, _)| id.0)
    }
}

#[pyclass(name = "ShmRegion")]
struct PyShmRegion {
    inner: Option<ShmRegion>,
    exports: AtomicUsize,
}

impl PyShmRegion {
    fn inner_ref(&self) -> PyResult<&ShmRegion> {
        self.inner
            .as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("region is closed"))
    }

    fn inner_mut(&mut self) -> PyResult<&mut ShmRegion> {
        self.inner
            .as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("region is closed"))
    }
}

#[pymethods]
impl PyShmRegion {
    #[new]
    fn new(size: usize) -> PyResult<Self> {
        let inner = ShmRegion::create(size).map_err(map_err)?;
        Ok(Self {
            inner: Some(inner),
            exports: AtomicUsize::new(0),
        })
    }

    #[cfg(unix)]
    #[classmethod]
    fn create_named(_cls: &Bound<'_, PyType>, name: &str, size: usize) -> PyResult<Self> {
        let inner = ShmRegion::create_named(name, size).map_err(map_err)?;
        Ok(Self {
            inner: Some(inner),
            exports: AtomicUsize::new(0),
        })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn create_named(_cls: &Bound<'_, PyType>, _name: &str, _size: usize) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named shared memory is only supported on Unix",
        ))
    }

    #[cfg(unix)]
    #[classmethod]
    fn open_named(_cls: &Bound<'_, PyType>, name: &str, size: usize) -> PyResult<Self> {
        let inner = ShmRegion::open_named(name, size).map_err(map_err)?;
        Ok(Self {
            inner: Some(inner),
            exports: AtomicUsize::new(0),
        })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn open_named(_cls: &Bound<'_, PyType>, _name: &str, _size: usize) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named shared memory is only supported on Unix",
        ))
    }

    fn len(&self) -> PyResult<usize> {
        Ok(self.inner_ref()?.len())
    }

    fn read<'py>(
        &self,
        py: Python<'py>,
        offset: usize,
        len: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let inner = self.inner_ref()?;
        unsafe {
            let slice = inner.as_slice();
            let end = offset.checked_add(len).ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("invalid read range")
            })?;
            if end > slice.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "read out of bounds",
                ));
            }
            Ok(PyBytes::new(py, &slice[offset..end]))
        }
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> PyResult<()> {
        let inner = self.inner_mut()?;
        unsafe {
            let slice = inner.as_mut_slice();
            let end = offset.checked_add(data.len()).ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("invalid write range")
            })?;
            if end > slice.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "write out of bounds",
                ));
            }
            slice[offset..end].copy_from_slice(data);
        }
        Ok(())
    }

    /// Returns a memoryview exposing the shared memory region.
    fn as_memoryview<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("view is null"));
        }
        let mut region = slf.borrow_mut();
        let inner = region.inner_mut()?;
        fill_view_from_data(
            view,
            flags,
            unsafe { inner.as_mut_slice() },
            slf.into_any(),
            true,
        )?;
        region.exports.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        if view.is_null() {
            return;
        }
        if !(*view).format.is_null() {
            drop(CString::from_raw((*view).format));
        }
        self.exports.fetch_sub(1, AtomicOrdering::Release);
    }

    /// Close the region, releasing the underlying memory.
    ///
    /// Raises an error if there are active memoryviews.
    fn close(&mut self) -> PyResult<()> {
        if self.exports.load(AtomicOrdering::Acquire) > 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "region has active views",
            ));
        }
        self.inner.take();
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc: Option<Py<PyAny>>,
        _tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        Ok(false)
    }
}

#[pyclass(name = "BufferPool")]
struct PyBufferPool {
    inner: std::sync::Arc<BufferPool>,
}

#[pymethods]
impl PyBufferPool {
    #[new]
    fn new(buffer_size: usize, count: usize) -> Self {
        Self {
            inner: BufferPool::new(buffer_size, count),
        }
    }

    fn acquire(&self) -> PyResult<Option<PyBuffer>> {
        match self.inner.acquire() {
            Ok(buf) => Ok(Some(PyBuffer {
                inner: Some(buf),
                exports: AtomicUsize::new(0),
            })),
            Err(_) => Ok(None),
        }
    }

    fn available(&self) -> usize {
        self.inner.available()
    }

    fn buffer_size(&self) -> usize {
        self.inner.buffer_size()
    }
}

#[pyclass(name = "Buffer")]
struct PyBuffer {
    inner: Option<Buffer>,
    exports: AtomicUsize,
}

impl PyBuffer {
    fn inner_ref(&self) -> PyResult<&Buffer> {
        self.inner
            .as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("buffer is closed"))
    }

    fn inner_mut(&mut self) -> PyResult<&mut Buffer> {
        self.inner
            .as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("buffer is closed"))
    }

    fn take_inner(&mut self) -> PyResult<Buffer> {
        if self.exports.load(AtomicOrdering::Acquire) > 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "buffer has active views",
            ));
        }
        self.inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("buffer is closed"))
    }
}

/// Zero-copy view into a ring buffer slot.
///
/// This provides direct access to the ring buffer data without copying.
/// The slot is automatically released when the view is dropped or when
/// `release()` is called.
#[pyclass(name = "SlotView")]
struct PySlotView {
    inner: Option<SlotView>,
    exports: AtomicUsize,
}

#[pymethods]
impl PySlotView {
    /// Get the length of the payload.
    fn __len__(&self) -> PyResult<usize> {
        match &self.inner {
            Some(view) => Ok(view.len()),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "slot view is released",
            )),
        }
    }

    /// Get the payload length.
    #[getter]
    fn length(&self) -> PyResult<usize> {
        self.__len__()
    }

    /// Get the message flags.
    #[getter]
    fn flags(&self) -> PyResult<u32> {
        match &self.inner {
            Some(view) => Ok(view.flags()),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "slot view is released",
            )),
        }
    }

    /// Copy the data to bytes.
    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        match &self.inner {
            Some(view) => Ok(PyBytes::new(py, view.as_slice())),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "slot view is released",
            )),
        }
    }

    /// Get a zero-copy memoryview into the slot data.
    fn as_memoryview<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    /// Release the slot back to the ring buffer.
    ///
    /// Raises an error if there are active memoryviews.
    fn release(&mut self) -> PyResult<()> {
        if self.exports.load(AtomicOrdering::Acquire) > 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "slot has active views",
            ));
        }
        self.inner.take();
        Ok(())
    }

    /// Check if the slot view is still valid.
    #[getter]
    fn is_valid(&self) -> bool {
        self.inner.is_some()
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("view is null"));
        }
        let slot = slf.borrow();
        let inner = slot
            .inner
            .as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("slot is released"))?;

        let data = inner.as_slice();
        // SlotView data is read-only (we don't own it, just borrowing)
        fill_view_from_data(view, flags, data, slf.into_any(), false)?;
        slot.exports.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        if view.is_null() {
            return;
        }
        if !(*view).format.is_null() {
            drop(CString::from_raw((*view).format));
        }
        self.exports.fetch_sub(1, AtomicOrdering::Release);
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc: Option<Py<PyAny>>,
        _tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        // Release on context exit, but don't raise if already released
        if self.exports.load(AtomicOrdering::Acquire) == 0 {
            self.inner.take();
        }
        Ok(false)
    }
}

#[pyclass(name = "Channel")]
struct PyChannel {
    send_ring: SpscRing,
    recv_ring: SpscRing,
}

#[pymethods]
impl PyChannel {
    #[classmethod]
    fn create_pair<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Spsc);
        let req_region = Arc::new(ShmRegion::create(layout.total_size).map_err(map_err)?);
        let resp_region = Arc::new(ShmRegion::create(layout.total_size).map_err(map_err)?);
        let req_client = unsafe { SpscRing::initialize(Arc::clone(&req_region), layout, 0, 0) }
            .map_err(map_err)?;
        let req_server =
            unsafe { SpscRing::attach(Arc::clone(&req_region), layout) }.map_err(map_err)?;
        let resp_client = unsafe { SpscRing::initialize(Arc::clone(&resp_region), layout, 0, 0) }
            .map_err(map_err)?;
        let resp_server =
            unsafe { SpscRing::attach(Arc::clone(&resp_region), layout) }.map_err(map_err)?;
        let client = Py::new(
            py,
            PyChannel {
                send_ring: req_client,
                recv_ring: resp_client,
            },
        )?;
        let server = Py::new(
            py,
            PyChannel {
                send_ring: resp_server,
                recv_ring: req_server,
            },
        )?;
        Ok(PyTuple::new(py, vec![client, server])?)
    }

    #[cfg(unix)]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
        role: &str,
    ) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Spsc);
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
            "client" => Ok(Self {
                send_ring: req_ring,
                recv_ring: resp_ring,
            }),
            "server" => Ok(Self {
                send_ring: resp_ring,
                recv_ring: req_ring,
            }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "role must be 'client' or 'server'",
            )),
        }
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        _name: &str,
        _slot_size: usize,
        _slots: usize,
        _role: &str,
    ) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named channels are only supported on Unix",
        ))
    }

    #[cfg(unix)]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
        role: &str,
    ) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Spsc);
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
            "client" => Ok(Self {
                send_ring: req_ring,
                recv_ring: resp_ring,
            }),
            "server" => Ok(Self {
                send_ring: resp_ring,
                recv_ring: req_ring,
            }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "role must be 'client' or 'server'",
            )),
        }
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        _name: &str,
        _slot_size: usize,
        _slots: usize,
        _role: &str,
    ) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named channels are only supported on Unix",
        ))
    }

    fn payload_capacity(&self) -> usize {
        self.send_ring.payload_capacity()
    }

    fn send_request(&self, data: &[u8]) -> PyResult<()> {
        Python::attach(|py| py.detach(|| self.send_ring.send(data, 0).map_err(map_err)))
    }

    fn try_send_request(&self, data: &[u8]) -> PyResult<bool> {
        match self.send_ring.try_send(data, 0) {
            Ok(()) => Ok(true),
            Err(ChannelError::Full) => Ok(false),
            Err(err) => Err(map_err(err)),
        }
    }

    fn send_request_timeout(&self, data: &[u8], timeout_ms: u64) -> PyResult<bool> {
        Python::attach(|py| {
            py.detach(|| {
                self.send_ring
                    .send_timeout(data, 0, std::time::Duration::from_millis(timeout_ms))
                    .map_err(map_err)
            })
        })
    }

    fn recv_request<'py>(&self, py: Python<'py>, max_len: usize) -> PyResult<Bound<'py, PyBytes>> {
        let mut buffer = vec![0u8; max_len];
        let len = py.detach(|| self.recv_ring.recv(&mut buffer).map_err(map_err))?;
        Ok(PyBytes::new(py, &buffer[..len]))
    }

    fn try_recv_request<'py>(
        &self,
        py: Python<'py>,
        max_len: usize,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut buffer = vec![0u8; max_len];
        match self.recv_ring.try_recv(&mut buffer) {
            Ok(len) => Ok(Some(PyBytes::new(py, &buffer[..len]))),
            Err(ChannelError::Empty) => Ok(None),
            Err(err) => Err(map_err(err)),
        }
    }

    fn recv_request_timeout<'py>(
        &self,
        py: Python<'py>,
        max_len: usize,
        timeout_ms: u64,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut buffer = vec![0u8; max_len];
        let result = py.detach(|| {
            self.recv_ring
                .recv_timeout(&mut buffer, std::time::Duration::from_millis(timeout_ms))
                .map_err(map_err)
        })?;
        Ok(result.map(|len| PyBytes::new(py, &buffer[..len])))
    }

    fn send_response(&self, data: &[u8]) -> PyResult<()> {
        Python::attach(|py| py.detach(|| self.send_ring.send(data, 0).map_err(map_err)))
    }

    fn try_send_response(&self, data: &[u8]) -> PyResult<bool> {
        match self.send_ring.try_send(data, 0) {
            Ok(()) => Ok(true),
            Err(ChannelError::Full) => Ok(false),
            Err(err) => Err(map_err(err)),
        }
    }

    fn send_response_timeout(&self, data: &[u8], timeout_ms: u64) -> PyResult<bool> {
        Python::attach(|py| {
            py.detach(|| {
                self.send_ring
                    .send_timeout(data, 0, std::time::Duration::from_millis(timeout_ms))
                    .map_err(map_err)
            })
        })
    }

    fn recv_response<'py>(&self, py: Python<'py>, max_len: usize) -> PyResult<Bound<'py, PyBytes>> {
        let mut buffer = vec![0u8; max_len];
        let len = py.detach(|| self.recv_ring.recv(&mut buffer).map_err(map_err))?;
        Ok(PyBytes::new(py, &buffer[..len]))
    }

    fn try_recv_response<'py>(
        &self,
        py: Python<'py>,
        max_len: usize,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut buffer = vec![0u8; max_len];
        match self.recv_ring.try_recv(&mut buffer) {
            Ok(len) => Ok(Some(PyBytes::new(py, &buffer[..len]))),
            Err(ChannelError::Empty) => Ok(None),
            Err(err) => Err(map_err(err)),
        }
    }

    fn recv_response_timeout<'py>(
        &self,
        py: Python<'py>,
        max_len: usize,
        timeout_ms: u64,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut buffer = vec![0u8; max_len];
        let result = py.detach(|| {
            self.recv_ring
                .recv_timeout(&mut buffer, std::time::Duration::from_millis(timeout_ms))
                .map_err(map_err)
        })?;
        Ok(result.map(|len| PyBytes::new(py, &buffer[..len])))
    }
}

#[pymethods]
impl PyBuffer {
    fn len(&self) -> PyResult<usize> {
        Ok(self.inner_ref()?.len())
    }

    fn index(&self) -> PyResult<usize> {
        Ok(self.inner_ref()?.index())
    }

    fn read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(py, self.inner_ref()?.as_slice()))
    }

    fn write(&mut self, data: &[u8]) -> PyResult<()> {
        let slice = self.inner_mut()?.as_mut_slice();
        if data.len() > slice.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "data too large",
            ));
        }
        slice[..data.len()].copy_from_slice(data);
        Ok(())
    }

    fn as_memoryview<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("view is null"));
        }
        let mut buffer = slf.borrow_mut();
        let inner = buffer.inner_mut()?;
        fill_view_from_data(view, flags, inner.as_mut_slice(), slf.into_any(), true)?;
        buffer.exports.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        if view.is_null() {
            return;
        }
        if !(*view).format.is_null() {
            drop(CString::from_raw((*view).format));
        }
        self.exports.fetch_sub(1, AtomicOrdering::Release);
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc: Option<Py<PyAny>>,
        _tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        Ok(false)
    }
}

#[pyclass(name = "SpscRing")]
struct PySpscRing {
    inner: Arc<SpscRing>,
}

#[pymethods]
impl PySpscRing {
    #[new]
    fn new(slot_size: usize, slots: usize) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::create(layout.total_size).map_err(map_err)?);
        let ring =
            unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0) }.map_err(map_err)?;
        Ok(Self {
            inner: Arc::new(ring),
        })
    }

    #[cfg(unix)]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::create_named(name, layout.total_size).map_err(map_err)?);
        let ring =
            unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0) }.map_err(map_err)?;
        Ok(Self {
            inner: Arc::new(ring),
        })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        _name: &str,
        _slot_size: usize,
        _slots: usize,
    ) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named channels are only supported on Unix",
        ))
    }

    #[cfg(unix)]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Spsc);
        let region = Arc::new(ShmRegion::open_named(name, layout.total_size).map_err(map_err)?);
        let ring = unsafe { SpscRing::attach(Arc::clone(&region), layout) }.map_err(map_err)?;
        Ok(Self {
            inner: Arc::new(ring),
        })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        _name: &str,
        _slot_size: usize,
        _slots: usize,
    ) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named channels are only supported on Unix",
        ))
    }

    fn send(&self, data: &[u8], flags: u32) -> PyResult<()> {
        Python::attach(|py| py.detach(|| self.inner.send(data, flags).map_err(map_err)))
    }

    fn try_send(&self, data: &[u8], flags: u32) -> PyResult<()> {
        self.inner.try_send(data, flags).map_err(map_err)
    }

    fn send_timeout(&self, data: &[u8], flags: u32, timeout_ms: u64) -> PyResult<bool> {
        Python::attach(|py| {
            py.detach(|| {
                self.inner
                    .send_timeout(data, flags, std::time::Duration::from_millis(timeout_ms))
                    .map_err(map_err)
            })
        })
    }

    fn recv<'py>(&self, py: Python<'py>, max_len: usize) -> PyResult<Bound<'py, PyBytes>> {
        let mut buffer = vec![0u8; max_len];
        let len = py.detach(|| self.inner.recv(&mut buffer).map_err(map_err))?;
        Ok(PyBytes::new(py, &buffer[..len]))
    }

    fn try_recv<'py>(
        &self,
        py: Python<'py>,
        max_len: usize,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut buffer = vec![0u8; max_len];
        match self.inner.try_recv(&mut buffer) {
            Ok(len) => Ok(Some(PyBytes::new(py, &buffer[..len]))),
            Err(_) => Ok(None),
        }
    }

    fn recv_timeout<'py>(
        &self,
        py: Python<'py>,
        max_len: usize,
        timeout_ms: u64,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut buffer = vec![0u8; max_len];
        let result = py.detach(|| {
            self.inner
                .recv_timeout(&mut buffer, std::time::Duration::from_millis(timeout_ms))
                .map_err(map_err)
        })?;
        Ok(result.map(|len| PyBytes::new(py, &buffer[..len])))
    }

    /// Receive a message as a zero-copy view.
    ///
    /// Returns a SlotView that provides direct access to the ring buffer data.
    /// The slot is automatically released when the view is dropped.
    fn try_recv_view<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PySlotView>>> {
        match self.inner.try_recv_view() {
            Ok(view) => {
                let py_view = Py::new(
                    py,
                    PySlotView {
                        inner: Some(view),
                        exports: AtomicUsize::new(0),
                    },
                )?;
                Ok(Some(py_view.into_bound(py)))
            }
            Err(ChannelError::Empty) | Err(ChannelError::SlotBorrowed) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Receive a message as a zero-copy view, blocking until data is available.
    fn recv_view<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySlotView>> {
        let view = py.detach(|| self.inner.recv_view().map_err(map_err))?;
        let py_view = Py::new(
            py,
            PySlotView {
                inner: Some(view),
                exports: AtomicUsize::new(0),
            },
        )?;
        Ok(py_view.into_bound(py))
    }

    /// Receive a message as a zero-copy view with timeout.
    #[pyo3(signature = (timeout_ms=1000))]
    fn recv_view_timeout<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: u64,
    ) -> PyResult<Option<Bound<'py, PySlotView>>> {
        let result = py.detach(|| {
            self.inner
                .recv_view_timeout(std::time::Duration::from_millis(timeout_ms))
                .map_err(map_err)
        })?;
        match result {
            Some(view) => {
                let py_view = Py::new(
                    py,
                    PySlotView {
                        inner: Some(view),
                        exports: AtomicUsize::new(0),
                    },
                )?;
                Ok(Some(py_view.into_bound(py)))
            }
            None => Ok(None),
        }
    }

    /// Close the ring buffer.
    ///
    /// Note: This is a no-op; Rust's Drop trait handles cleanup.
    /// Provided for API compatibility with Python expectations.
    fn close(&self) -> PyResult<()> {
        // No-op: Rust handles cleanup via Drop
        Ok(())
    }
}

#[pyclass(name = "MpmcRing")]
struct PyMpmcRing {
    inner: Arc<MpmcRing>,
}

#[pymethods]
impl PyMpmcRing {
    #[new]
    fn new(slot_size: usize, slots: usize) -> PyResult<Self> {
        let inner = MpmcRing::new(slot_size, slots).map_err(map_err)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[cfg(unix)]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::create_named(name, layout.total_size).map_err(map_err)?);
        let ring = unsafe {
            MpmcRing::initialize(
                Arc::clone(&region),
                layout,
                std::process::id(),
                0, // Epoch 0 for now or implement epoch logic
            )
        }
        .map_err(map_err)?;
        Ok(Self {
            inner: Arc::new(ring),
        })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        _name: &str,
        _slot_size: usize,
        _slots: usize,
    ) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named channels are only supported on Unix",
        ))
    }

    #[cfg(unix)]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots, vertebra_core::buffer::ChannelMode::Mpmc);
        let region = Arc::new(ShmRegion::open_named(name, layout.total_size).map_err(map_err)?);
        let ring = unsafe { MpmcRing::attach(Arc::clone(&region), layout) }.map_err(map_err)?;
        Ok(Self {
            inner: Arc::new(ring),
        })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        _name: &str,
        _slot_size: usize,
        _slots: usize,
    ) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named channels are only supported on Unix",
        ))
    }

    fn send(&self, data: &[u8], flags: u32) -> PyResult<()> {
        Python::attach(|py| py.detach(|| self.inner.send(data, flags).map_err(map_err)))
    }

    fn recv<'py>(&self, py: Python<'py>, max_len: usize) -> PyResult<Bound<'py, PyBytes>> {
        let mut buffer = vec![0u8; max_len];
        let len = py.detach(|| self.inner.recv(&mut buffer).map_err(map_err))?;
        Ok(PyBytes::new(py, &buffer[..len]))
    }
}

#[pyclass(name = "BufferChannel")]
struct PyBufferChannel {
    pool: Arc<BufferPool>,
    sender: vertebra_core::channel::BufferSender,
    receiver: vertebra_core::channel::BufferReceiver,
}

#[pymethods]
impl PyBufferChannel {
    #[new]
    fn new(buffer_size: usize, count: usize, slots: usize) -> PyResult<Self> {
        let pool = BufferPool::new(buffer_size, count);
        let (sender, receiver) = BufferChannel::split(Arc::clone(&pool), slots).map_err(map_err)?;
        Ok(Self {
            pool,
            sender,
            receiver,
        })
    }

    fn acquire(&self) -> PyResult<Option<PyBuffer>> {
        match self.pool.acquire() {
            Ok(buf) => Ok(Some(PyBuffer {
                inner: Some(buf),
                exports: AtomicUsize::new(0),
            })),
            Err(_) => Ok(None),
        }
    }

    fn send(&self, buffer: &Bound<'_, PyBuffer>, len: usize, flags: u32) -> PyResult<()> {
        let mut buffer = buffer.borrow_mut();
        let inner = buffer.take_inner()?;
        Python::attach(|py| py.detach(|| self.sender.send(inner, len, flags).map_err(map_err)))
    }

    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let BufferMessage { buffer, len, flags } =
            py.detach(|| self.receiver.recv().map_err(map_err))?;
        let buffer = Py::new(
            py,
            PyBuffer {
                inner: Some(buffer),
                exports: AtomicUsize::new(0),
            },
        )?;
        (buffer, len, flags).into_pyobject(py)
    }
}

/// Unlink a named shared memory region.
///
/// This removes the shared memory name from the filesystem so no new processes
/// can open it. Existing mappings remain valid until all handles are closed.
#[cfg(unix)]
#[pyfunction]
fn shm_unlink(name: &str) -> PyResult<()> {
    vertebra_core::shm::shm_unlink_named(name).map_err(map_err)
}

/// Unlink a named shared memory region (not available on this platform).
#[cfg(not(unix))]
#[pyfunction]
fn shm_unlink(_name: &str) -> PyResult<()> {
    Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
        "shm_unlink is only supported on Unix",
    ))
}

/// Python module initialization.
#[pymodule]
fn _vertebra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", vertebra_core::VERSION)?;
    m.add_class::<PyRouter>()?;
    m.add_class::<PyShmRegion>()?;
    m.add_class::<PyBufferPool>()?;
    m.add_class::<PyBuffer>()?;
    m.add_class::<PySlotView>()?;
    m.add_class::<PyChannel>()?;
    m.add_class::<PySpscRing>()?;
    m.add_class::<PyMpmcRing>()?;
    m.add_class::<PyBufferChannel>()?;
    m.add_class::<supervisor::NativeSupervisor>()?;
    m.add_function(wrap_pyfunction!(shm_unlink, m)?)?;
    // Register async ring types (PRD-10: eventfd async transport)
    async_ring::register(m)?;
    Ok(())
}

fn map_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}

unsafe fn fill_view_from_data(
    view: *mut ffi::Py_buffer,
    flags: c_int,
    data: &[u8],
    owner: Bound<'_, PyAny>,
    writable: bool,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyBufferError::new_err("view is null"));
    }
    if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE && !writable {
        return Err(PyBufferError::new_err("object is not writable"));
    }
    (*view).obj = owner.into_ptr();
    (*view).buf = data.as_ptr() as *mut c_void;
    (*view).len = data.len() as isize;
    (*view).readonly = if writable { 0 } else { 1 };
    (*view).itemsize = 1;
    (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
        CString::new("B").unwrap().into_raw()
    } else {
        ptr::null_mut()
    };
    (*view).ndim = 1;
    (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
        &mut (*view).len
    } else {
        ptr::null_mut()
    };
    (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
        &mut (*view).itemsize
    } else {
        ptr::null_mut()
    };
    (*view).suboffsets = ptr::null_mut();
    (*view).internal = ptr::null_mut();
    Ok(())
}
