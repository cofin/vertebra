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
/// via `loop.add_reader()`. For low-latency synchronous receives, use
/// `recv_hybrid()` which spins briefly before falling back to blocking.
///
/// # Example (Python)
/// ```python
/// ring = AsyncRing(65536, 1024)
/// fd = ring.notifier_fd()
///
/// # Async pattern with add_reader:
/// def on_readable():
///     ring.clear_notification()
///     while (data := ring.try_recv()) is not None:
///         process(data)
/// loop.add_reader(fd, on_readable)
///
/// # Or sync low-latency pattern:
/// data = ring.recv_hybrid(spin_us=5, timeout_ms=1000)
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

    /// Receive with hybrid spin-wait for low-latency (releases GIL during wait).
    ///
    /// Spins for `spin_us` microseconds checking for data, then falls back to
    /// blocking wait. This captures immediate responses without syscall overhead.
    ///
    /// Args:
    ///     spin_us: Microseconds to spin before blocking (typically 2-10, default 5)
    ///     timeout_ms: Total timeout in milliseconds (-1 for infinite, default 1000)
    ///
    /// Returns:
    ///     bytes if data received, None on timeout
    #[pyo3(signature = (spin_us=5, timeout_ms=1000))]
    fn recv_hybrid<'py>(
        &self,
        py: Python<'py>,
        spin_us: u64,
        timeout_ms: i32,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let capacity = self.inner.payload_capacity();
        let mut buffer = vec![0u8; capacity];

        // Release GIL during blocking operation
        #[allow(deprecated)]
        let result = py.allow_threads(|| {
            self.inner
                .recv_hybrid(&mut buffer, spin_us, timeout_ms)
                .map_err(map_err)
        })?;

        match result {
            Some(len) => Ok(Some(PyBytes::new(py, &buffer[..len]))),
            None => Ok(None),
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
        // Use named channels with a unique ID for proper memory sharing.
        // This ensures both client and server operate on the same underlying rings.
        // Names must start with '/' for shm_open compatibility.
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("/vb_pair_{id}");

        // Create named channels - these create the shared memory regions
        let req_ring =
            AsyncRing::create_named(&format!("{name}_req"), slot_size, slots).map_err(map_err)?;
        let resp_ring =
            AsyncRing::create_named(&format!("{name}_resp"), slot_size, slots).map_err(map_err)?;

        // Open the same rings for server (swapped perspective)
        let req_ring_server =
            AsyncRing::open_named(&format!("{name}_req"), slot_size, slots).map_err(map_err)?;
        let resp_ring_server =
            AsyncRing::open_named(&format!("{name}_resp"), slot_size, slots).map_err(map_err)?;

        // Client: sends on request, receives on response
        let client = PyAsyncChannel {
            request: req_ring,
            response: resp_ring_server,
        };

        // Server: receives on request (client's send), sends on response
        let server = PyAsyncChannel {
            request: req_ring_server,
            response: resp_ring,
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

/// Fast request-response channel with Rust-side correlation tracking.
///
/// This channel handles the entire request/response cycle in Rust,
/// minimizing Python/Rust boundary crossings.
#[pyclass(name = "FastChannel")]
pub struct PyFastChannel {
    request: AsyncRing,
    response: AsyncRing,
    next_id: std::sync::atomic::AtomicU64,
}

#[pymethods]
impl PyFastChannel {
    /// Create a named fast channel.
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
                next_id: std::sync::atomic::AtomicU64::new(1),
            }),
            "server" => Ok(Self {
                request: pair.response,
                response: pair.request,
                next_id: std::sync::atomic::AtomicU64::new(1),
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

    /// Open an existing named fast channel.
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
                next_id: std::sync::atomic::AtomicU64::new(1),
            }),
            "server" => Ok(Self {
                request: pair.response,
                response: pair.request,
                next_id: std::sync::atomic::AtomicU64::new(1),
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

    /// Get the response ring's notification FD.
    fn response_notifier_fd(&self) -> i32 {
        self.response.notifier_fd()
    }

    /// Get the payload capacity.
    fn payload_capacity(&self) -> usize {
        self.request.payload_capacity()
    }

    /// Send a request with a new correlation ID.
    ///
    /// Returns the correlation ID for later matching.
    fn send_request(&self, py: Python<'_>, data: &[u8]) -> PyResult<u64> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        py.detach(|| self.request.send(data, id as u32).map_err(map_err))?;
        Ok(id)
    }

    /// Try to receive a response without blocking.
    ///
    /// Returns (correlation_id, data) if available, None otherwise.
    fn try_recv_response<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, pyo3::types::PyTuple>>> {
        let capacity = self.response.payload_capacity();
        let mut buffer = vec![0u8; capacity];

        match self.response.try_recv_with_flags(&mut buffer) {
            Ok(Some((len, flags))) => {
                let data = PyBytes::new(py, &buffer[..len]);
                let id = flags as u64; // Correlation ID stored in flags
                Ok(Some(pyo3::types::PyTuple::new(
                    py,
                    vec![id.into_pyobject(py)?.as_any(), data.as_any()],
                )?))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Execute a complete request/response cycle with minimal GIL releases.
    ///
    /// This is the fastest path for synchronous RPC - the entire cycle
    /// happens in Rust with a single GIL release.
    ///
    /// Args:
    ///     data: Request payload
    ///     spin_us: Microseconds to spin before blocking (default 5)
    ///     timeout_ms: Total timeout in milliseconds (default 1000)
    ///
    /// Returns:
    ///     Response payload bytes
    #[pyo3(signature = (data, spin_us=5, timeout_ms=1000))]
    fn request_response<'py>(
        &self,
        py: Python<'py>,
        data: &[u8],
        spin_us: u64,
        timeout_ms: i32,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let capacity = self.response.payload_capacity();
        let mut buffer = vec![0u8; capacity];

        // Release GIL during the entire request/response cycle
        #[allow(deprecated)]
        let result = py.allow_threads(|| {
            use std::time::{Duration, Instant};

            // Send request with correlation ID in flags
            self.request.send(data, id as u32).map_err(map_err)?;

            // Hybrid spin-wait + poll approach
            // Note: We can't use recv_hybrid because the notifier is per-ring,
            // not shared between channel endpoints. Instead we spin-poll.
            let start = Instant::now();
            let spin_deadline = start + Duration::from_micros(spin_us);
            let timeout_deadline = start + Duration::from_millis(timeout_ms as u64);

            // Spin phase: tight loop checking for data
            while Instant::now() < spin_deadline {
                match self.response.try_recv(&mut buffer) {
                    Ok(Some(len)) => return Ok(Some(len)),
                    Ok(None) => std::hint::spin_loop(),
                    Err(e) => return Err(map_err(e)),
                }
            }

            // Poll phase: yield CPU between checks
            while Instant::now() < timeout_deadline {
                match self.response.try_recv(&mut buffer) {
                    Ok(Some(len)) => return Ok(Some(len)),
                    Ok(None) => std::thread::sleep(Duration::from_micros(100)),
                    Err(e) => return Err(map_err(e)),
                }
            }

            Ok(None)
        })?;

        match result {
            Some(len) => Ok(PyBytes::new(py, &buffer[..len])),
            None => Err(pyo3::exceptions::PyTimeoutError::new_err(
                "response timeout",
            )),
        }
    }

    /// Clear response notifications.
    fn clear_response_notification(&self) -> PyResult<()> {
        self.response.clear_notification().map_err(map_err)
    }
}

/// Register async ring types with the Python module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAsyncRing>()?;
    m.add_class::<PyAsyncChannel>()?;
    m.add_class::<PyPooledAsyncRing>()?;
    m.add_class::<PyFastChannel>()?;
    Ok(())
}
