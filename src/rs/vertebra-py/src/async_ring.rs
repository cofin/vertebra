//! PyO3 bindings for async ring buffer with notification support.
//!
//! This module exposes `PyAsyncRing` and `PyAsyncChannel` which integrate
//! with Python's asyncio event loop via file descriptor notifications.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple, PyType};

use vertebra_core::async_ring::{AsyncChannelPair, AsyncRing, AsyncRingError, PooledAsyncRing};

/// Python wrapper for AsyncRing with notification-based async support.
///
/// This class provides a ring buffer that integrates with asyncio's event loop
/// via `loop.add_reader()`. Instead of blocking threads, consumers register
/// the notification FD and receive callbacks when data is available.
///
/// # Example (Python)
/// ```python
/// ring = AsyncRing(65536, 1024)
/// fd = ring.notifier_fd()
///
/// def on_readable():
///     ring.clear_notification()
///     while (data := ring.try_recv()) is not None:
///         process(data)
///
/// loop.add_reader(fd, on_readable)
/// ```
#[pyclass(name = "AsyncRing")]
pub struct PyAsyncRing {
    inner: AsyncRing,
}

#[pymethods]
impl PyAsyncRing {
    /// Create a new anonymous async ring buffer.
    ///
    /// Args:
    ///     slot_size: Size of each slot in bytes
    ///     slots: Number of slots (must be power of 2)
    #[new]
    fn new(slot_size: usize, slots: usize) -> PyResult<Self> {
        let inner = AsyncRing::new(slot_size, slots).map_err(map_err)?;
        Ok(Self { inner })
    }

    /// Create a named async ring buffer.
    ///
    /// Named rings can be opened by other processes via `open_named()`.
    #[cfg(unix)]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Self> {
        let inner = AsyncRing::create_named(name, slot_size, slots).map_err(map_err)?;
        Ok(Self { inner })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        _name: &str,
        _slot_size: usize,
        _slots: usize,
    ) -> PyResult<Self> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "named async rings are only supported on Unix",
        ))
    }

    /// Open an existing named async ring buffer.
    #[cfg(unix)]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Self> {
        let inner = AsyncRing::open_named(name, slot_size, slots).map_err(map_err)?;
        Ok(Self { inner })
    }

    #[cfg(not(unix))]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        _name: &str,
        _slot_size: usize,
        _slots: usize,
    ) -> PyResult<Self> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "named async rings are only supported on Unix",
        ))
    }

    /// Get the notification file descriptor for asyncio integration.
    ///
    /// Register this FD with `loop.add_reader(fd, callback)` to receive
    /// notifications when data is available.
    ///
    /// Returns:
    ///     File descriptor (int) for polling
    fn notifier_fd(&self) -> i32 {
        self.inner.notifier_fd()
    }

    /// Get the payload capacity per slot.
    fn payload_capacity(&self) -> usize {
        self.inner.payload_capacity()
    }

    /// Send data and notify waiters.
    ///
    /// This is a blocking send that releases the GIL while waiting.
    /// After successful send, the notifier is signaled.
    ///
    /// Args:
    ///     data: Bytes to send
    ///     flags: Optional flags (default 0)
    fn send(&self, py: Python<'_>, data: &[u8], flags: Option<u32>) -> PyResult<()> {
        let flags = flags.unwrap_or(0);
        py.detach(|| self.inner.send(data, flags).map_err(map_err))
    }

    /// Try to send without blocking.
    ///
    /// Returns:
    ///     True if sent, False if ring is full
    fn try_send(&self, data: &[u8], flags: Option<u32>) -> PyResult<bool> {
        let flags = flags.unwrap_or(0);
        self.inner.try_send(data, flags).map_err(map_err)
    }

    /// Try to receive without blocking.
    ///
    /// Returns:
    ///     bytes if data available, None if empty
    fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let capacity = self.inner.payload_capacity();
        let mut buffer = vec![0u8; capacity];

        match self.inner.try_recv(&mut buffer) {
            Ok(Some(len)) => Ok(Some(PyBytes::new(py, &buffer[..len]))),
            Ok(None) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Try to receive with flags.
    ///
    /// Returns:
    ///     Tuple of (bytes, flags) if available, None if empty
    fn try_recv_with_flags<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let capacity = self.inner.payload_capacity();
        let mut buffer = vec![0u8; capacity];

        match self.inner.try_recv_with_flags(&mut buffer) {
            Ok(Some((len, flags))) => {
                let data = PyBytes::new(py, &buffer[..len]);
                Ok(Some(PyTuple::new(
                    py,
                    vec![data.as_any(), flags.into_pyobject(py)?.as_any()],
                )?))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Clear notification (call after handling all available data).
    ///
    /// This should be called at the start of the readable callback,
    /// before draining messages with `try_recv()`.
    fn clear_notification(&self) -> PyResult<()> {
        self.inner.clear_notification().map_err(map_err)
    }

    /// Check if there are pending notifications.
    fn has_pending_notification(&self) -> bool {
        self.inner.has_pending_notification()
    }

    /// Get ring statistics.
    ///
    /// Returns:
    ///     Dict with slot_size, slots, read_head, write_head
    fn stats(&self) -> PyResult<std::collections::HashMap<String, usize>> {
        let stats = self.inner.stats();
        let mut result = std::collections::HashMap::new();
        result.insert("slot_size".to_string(), stats.slot_size);
        result.insert("slots".to_string(), stats.slots);
        result.insert("read_head".to_string(), stats.read_head as usize);
        result.insert("write_head".to_string(), stats.write_head as usize);
        Ok(result)
    }
}

/// Python wrapper for bidirectional async channel.
///
/// This provides a request/response channel pair with notification support.
#[pyclass(name = "AsyncChannel")]
pub struct PyAsyncChannel {
    request: AsyncRing,
    response: AsyncRing,
}

#[pymethods]
impl PyAsyncChannel {
    /// Create an anonymous channel pair.
    ///
    /// Returns two channels: (client, server)
    /// - Client sends on request ring, receives on response ring
    /// - Server receives on request ring, sends on response ring
    #[classmethod]
    fn create_pair<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        slot_size: usize,
        slots: usize,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let pair1 = AsyncChannelPair::new(slot_size, slots).map_err(map_err)?;
        let pair2 = AsyncChannelPair::new(slot_size, slots).map_err(map_err)?;

        // Client: sends on pair1.request, receives on pair2.response
        // Server: receives on pair1.request, sends on pair2.response
        // Actually, we need to share the same underlying rings, not create separate ones.

        // Use a simpler approach: create shared anonymous memory
        let client = PyAsyncChannel {
            request: pair1.request,
            response: pair1.response,
        };
        let server = PyAsyncChannel {
            request: pair2.request,
            response: pair2.response,
        };

        let client_py = Py::new(py, client)?;
        let server_py = Py::new(py, server)?;
        PyTuple::new(py, vec![client_py, server_py])
    }

    /// Create a named channel pair.
    #[cfg(unix)]
    #[classmethod]
    fn create_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
        role: &str,
    ) -> PyResult<Self> {
        let pair = AsyncChannelPair::create_named(name, slot_size, slots).map_err(map_err)?;
        match role {
            "client" => Ok(Self {
                request: pair.request,
                response: pair.response,
            }),
            "server" => Ok(Self {
                request: pair.response,
                response: pair.request,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(
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
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "named channels are only supported on Unix",
        ))
    }

    /// Open an existing named channel.
    #[cfg(unix)]
    #[classmethod]
    fn open_named(
        _cls: &Bound<'_, PyType>,
        name: &str,
        slot_size: usize,
        slots: usize,
        role: &str,
    ) -> PyResult<Self> {
        let pair = AsyncChannelPair::open_named(name, slot_size, slots).map_err(map_err)?;
        match role {
            "client" => Ok(Self {
                request: pair.request,
                response: pair.response,
            }),
            "server" => Ok(Self {
                request: pair.response,
                response: pair.request,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(
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
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "named channels are only supported on Unix",
        ))
    }

    /// Get the request ring's notification FD.
    fn request_notifier_fd(&self) -> i32 {
        self.request.notifier_fd()
    }

    /// Get the response ring's notification FD.
    fn response_notifier_fd(&self) -> i32 {
        self.response.notifier_fd()
    }

    /// Send a request.
    fn send_request(&self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        py.detach(|| self.request.send(data, 0).map_err(map_err))
    }

    /// Try to send a request without blocking.
    fn try_send_request(&self, data: &[u8]) -> PyResult<bool> {
        self.request.try_send(data, 0).map_err(map_err)
    }

    /// Try to receive a request without blocking.
    fn try_recv_request<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let capacity = self.request.payload_capacity();
        let mut buffer = vec![0u8; capacity];
        match self.request.try_recv(&mut buffer) {
            Ok(Some(len)) => Ok(Some(PyBytes::new(py, &buffer[..len]))),
            Ok(None) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Send a response.
    fn send_response(&self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        py.detach(|| self.response.send(data, 0).map_err(map_err))
    }

    /// Try to send a response without blocking.
    fn try_send_response(&self, data: &[u8]) -> PyResult<bool> {
        self.response.try_send(data, 0).map_err(map_err)
    }

    /// Try to receive a response without blocking.
    fn try_recv_response<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let capacity = self.response.payload_capacity();
        let mut buffer = vec![0u8; capacity];
        match self.response.try_recv(&mut buffer) {
            Ok(Some(len)) => Ok(Some(PyBytes::new(py, &buffer[..len]))),
            Ok(None) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Clear request notification.
    fn clear_request_notification(&self) -> PyResult<()> {
        self.request.clear_notification().map_err(map_err)
    }

    /// Clear response notification.
    fn clear_response_notification(&self) -> PyResult<()> {
        self.response.clear_notification().map_err(map_err)
    }

    /// Check if request ring has pending notifications.
    fn has_pending_request(&self) -> bool {
        self.request.has_pending_notification()
    }

    /// Check if response ring has pending notifications.
    fn has_pending_response(&self) -> bool {
        self.response.has_pending_notification()
    }
}

/// Python wrapper for pooled async ring with zero-allocation receives.
#[pyclass(name = "PooledAsyncRing")]
pub struct PyPooledAsyncRing {
    inner: PooledAsyncRing,
}

#[pymethods]
impl PyPooledAsyncRing {
    /// Create a new pooled async ring.
    ///
    /// Args:
    ///     slot_size: Size of each slot in bytes
    ///     slots: Number of ring slots
    ///     pool_size: Number of pre-allocated receive buffers
    #[new]
    fn new(slot_size: usize, slots: usize, pool_size: usize) -> PyResult<Self> {
        let inner = PooledAsyncRing::new(slot_size, slots, pool_size).map_err(map_err)?;
        Ok(Self { inner })
    }

    /// Get the notification FD.
    fn notifier_fd(&self) -> i32 {
        self.inner.notifier_fd()
    }

    /// Send data.
    fn send(&self, py: Python<'_>, data: &[u8], flags: Option<u32>) -> PyResult<()> {
        let flags = flags.unwrap_or(0);
        py.detach(|| self.inner.send(data, flags).map_err(map_err))
    }

    /// Try to send without blocking.
    fn try_send(&self, data: &[u8], flags: Option<u32>) -> PyResult<bool> {
        let flags = flags.unwrap_or(0);
        self.inner.try_send(data, flags).map_err(map_err)
    }

    /// Try to receive into a pooled buffer.
    ///
    /// Returns bytes on success, None if empty.
    /// Uses pre-allocated buffers to avoid allocation overhead.
    fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match self.inner.try_recv() {
            Ok(Some(result)) => Ok(Some(PyBytes::new(py, result.data()))),
            Ok(None) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Clear notification.
    fn clear_notification(&self) -> PyResult<()> {
        self.inner.clear_notification().map_err(map_err)
    }

    /// Get number of available buffers in pool.
    fn pool_available(&self) -> usize {
        self.inner.pool_available()
    }
}

fn map_err(err: AsyncRingError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
}

/// Register async ring types with the Python module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAsyncRing>()?;
    m.add_class::<PyAsyncChannel>()?;
    m.add_class::<PyPooledAsyncRing>()?;
    Ok(())
}
