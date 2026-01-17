//! PyO3 bindings for Vertebra.
//!
//! This module is imported as `vertebra._vertebra` in Python and provides
//! access to high-performance Rust-based functionality.

use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyBytes;
use pyo3::types::PyTuple;
use pyo3::types::PyType;
use pyo3::IntoPyObject;

use std::sync::Arc;

use vertebra_core::buffer::{Buffer, BufferPool, RingLayout};
use vertebra_core::channel::{BufferChannel, BufferMessage, MpmcRing, SpscRing};
use vertebra_core::shm::ShmRegion;

#[pyclass(name = "ShmRegion")]
struct PyShmRegion {
    inner: ShmRegion,
}

#[pymethods]
impl PyShmRegion {
    #[new]
    fn new(size: usize) -> PyResult<Self> {
        let inner = ShmRegion::create(size).map_err(map_err)?;
        Ok(Self { inner })
    }

    #[cfg(unix)]
    #[classmethod]
    fn create_named(_cls: &Bound<'_, PyType>, name: &str, size: usize) -> PyResult<Self> {
        let inner = ShmRegion::create_named(name, size).map_err(map_err)?;
        Ok(Self { inner })
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
        Ok(Self { inner })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn open_named(_cls: &Bound<'_, PyType>, _name: &str, _size: usize) -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "named shared memory is only supported on Unix",
        ))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn read<'py>(
        &self,
        py: Python<'py>,
        offset: usize,
        len: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        unsafe {
            let slice = self.inner.as_slice();
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
        unsafe {
            let slice = self.inner.as_mut_slice();
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
            Ok(buf) => Ok(Some(PyBuffer { inner: Some(buf) })),
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
        self.inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("buffer is closed"))
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
    inner: SpscRing,
}

#[pymethods]
impl PySpscRing {
    #[new]
    fn new(slot_size: usize, slots: usize) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots);
        let region = Arc::new(ShmRegion::create(layout.total_size).map_err(map_err)?);
        let ring =
            unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0) }.map_err(map_err)?;
        Ok(Self { inner: ring })
    }

    #[cfg(unix)]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Self> {
        let layout = RingLayout::new(slot_size, slots);
        let region = Arc::new(ShmRegion::create_named(name, layout.total_size).map_err(map_err)?);
        let ring =
            unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0) }.map_err(map_err)?;
        Ok(Self { inner: ring })
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
        let layout = RingLayout::new(slot_size, slots);
        let region = Arc::new(ShmRegion::open_named(name, layout.total_size).map_err(map_err)?);
        let ring = unsafe { SpscRing::attach(Arc::clone(&region), layout) }.map_err(map_err)?;
        Ok(Self { inner: ring })
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
        self.inner.send(data, flags).map_err(map_err)
    }

    fn try_send(&self, data: &[u8], flags: u32) -> PyResult<()> {
        self.inner.try_send(data, flags).map_err(map_err)
    }

    fn send_timeout(&self, data: &[u8], flags: u32, timeout_ms: u64) -> PyResult<bool> {
        self.inner
            .send_timeout(data, flags, std::time::Duration::from_millis(timeout_ms))
            .map_err(map_err)
    }

    fn recv<'py>(&self, py: Python<'py>, max_len: usize) -> PyResult<Bound<'py, PyBytes>> {
        let mut buffer = vec![0u8; max_len];
        let len = self.inner.recv(&mut buffer).map_err(map_err)?;
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
        let result = self
            .inner
            .recv_timeout(&mut buffer, std::time::Duration::from_millis(timeout_ms))
            .map_err(map_err)?;
        Ok(result.map(|len| PyBytes::new(py, &buffer[..len])))
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

    fn send(&self, data: &[u8], flags: u32) -> PyResult<()> {
        self.inner.send(data, flags).map_err(map_err)
    }

    fn recv<'py>(&self, py: Python<'py>, max_len: usize) -> PyResult<Bound<'py, PyBytes>> {
        let mut buffer = vec![0u8; max_len];
        let len = self.inner.recv(&mut buffer).map_err(map_err)?;
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
            Ok(buf) => Ok(Some(PyBuffer { inner: Some(buf) })),
            Err(_) => Ok(None),
        }
    }

    fn send(&self, buffer: &Bound<'_, PyBuffer>, len: usize, flags: u32) -> PyResult<()> {
        let mut buffer = buffer.borrow_mut();
        let inner = buffer.take_inner()?;
        self.sender.send(inner, len, flags).map_err(map_err)
    }

    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let BufferMessage { buffer, len, flags } = self.receiver.recv().map_err(map_err)?;
        let buffer = Py::new(
            py,
            PyBuffer {
                inner: Some(buffer),
            },
        )?;
        (buffer, len, flags).into_pyobject(py)
    }
}

/// Python module initialization.
#[pymodule]
fn _vertebra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", vertebra_core::VERSION)?;
    m.add_class::<PyShmRegion>()?;
    m.add_class::<PyBufferPool>()?;
    m.add_class::<PyBuffer>()?;
    m.add_class::<PySpscRing>()?;
    m.add_class::<PyMpmcRing>()?;
    m.add_class::<PyBufferChannel>()?;
    Ok(())
}

fn map_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}
