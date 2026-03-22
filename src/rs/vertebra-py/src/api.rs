//! Vectorized batch FFI API.
//!
//! Connects Python workers to either:
//! - `BatchBridge` (in-process mode, default)
//! - `Transport::SharedMemory` (SHM mode, for forked workers)
//!
//! The transport mode is selected by the `mode` parameter of `init_transport()`.

use std::sync::OnceLock;
use std::time::Duration;

use pyo3::prelude::*;

use vertebra_core::bridge::{BatchBridge, BridgeResponse};
use vertebra_core::transport::Transport;

use crate::batch::VertebraBatch;
use crate::response::VertebraResponse;

/// Global SHM transport, set once by `init_transport(mode="shm")`.
static SHM_TRANSPORT: OnceLock<Transport> = OnceLock::new();

/// Initialize the transport channel. Called once during worker startup.
///
/// # Modes
/// - `mode="inprocess"` (default): Uses the in-process `BatchBridge` (crossbeam channels).
/// - `mode="shm"`: Reads `VERTEBRA_*` env vars to reconstruct a `RingPair`
///   from inherited file descriptors. Used by forked worker processes.
#[pyfunction]
#[pyo3(name = "init_transport", signature = (mode="inprocess", ring_name="default", slot_size=4096, slots=64, batch_size=64))]
pub fn init_transport(
    mode: &str,
    ring_name: &str,
    slot_size: usize,
    slots: usize,
    batch_size: usize,
) -> PyResult<()> {
    match mode {
        "shm" => {
            #[cfg(unix)]
            {
                use std::sync::Arc;
                use vertebra_core::transport::ring_pair::RingPair;

                let ring_pair = RingPair::from_env().map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to init SHM transport: {e}"
                    ))
                })?;

                let transport = Transport::new_shared_memory(Arc::new(ring_pair));
                SHM_TRANSPORT.set(transport).map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err("SHM transport already initialized")
                })?;

                Ok(())
            }
            #[cfg(not(unix))]
            {
                Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "SHM transport is not supported on this platform",
                ))
            }
        }
        "inprocess" | _ => {
            let _ = ring_name;
            let slab_capacity = slot_size * slots;
            let arena_capacity = batch_size;
            let channel_depth = 4;

            match BatchBridge::init(arena_capacity, slab_capacity, channel_depth, 1) {
                Ok(()) => Ok(()),
                Err("bridge already initialized") => Ok(()),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        }
    }
}

/// Fetch the next batch of requests as a vectorized object. GIL released.
#[pyfunction]
#[pyo3(name = "get_batch", signature = (timeout_ms=100))]
pub fn get_batch(py: Python<'_>, timeout_ms: u64) -> PyResult<Option<VertebraBatch>> {
    // SHM path: poll from SPSC ring
    if let Some(transport) = SHM_TRANSPORT.get() {
        let snapshot = py.detach(|| transport.poll_batch(Duration::from_millis(timeout_ms)));
        return Ok(snapshot.map(|s| VertebraBatch { snapshot: s }));
    }

    // In-process path: poll from BatchBridge
    let bridge = BatchBridge::get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "transport not initialized; call init_transport() first",
        )
    })?;

    let snapshot = py.detach(|| bridge.poll_batch(Duration::from_millis(timeout_ms)));
    Ok(snapshot.map(|s| VertebraBatch { snapshot: s }))
}

/// Submit processed responses back to the HTTP server.
///
/// In SHM mode, serializes responses into the tiered response ring.
/// In in-process mode, pushes through the BatchBridge.
#[pyfunction]
pub fn submit_results(py: Python<'_>, responses: Vec<Py<VertebraResponse>>) -> PyResult<()> {
    // SHM path: write responses to tiered ring
    if let Some(transport) = SHM_TRANSPORT.get() {
        for py_resp in &responses {
            let resp = py_resp.borrow(py);

            // Encode headers into compact binary format
            let headers_raw = encode_headers_compact(&resp.headers);

            transport
                .send_response(resp.request_id, resp.status, &headers_raw, &resp.body)
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to send response: {e}"
                    ))
                })?;
        }

        // Notify supervisor that responses are available
        // (notifier is on the ring pair, accessed via transport internals)
        return Ok(());
    }

    // In-process path: BatchBridge
    let bridge = BatchBridge::get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "batch channel not initialized; call init_batch_channel() first",
        )
    })?;

    let mut bridge_responses = Vec::with_capacity(responses.len());
    for py_resp in &responses {
        let resp = py_resp.borrow(py);
        bridge_responses.push(BridgeResponse {
            request_id: resp.request_id,
            status: resp.status,
            headers: resp.headers.clone(),
            body: resp.body.clone(),
        });
    }

    bridge.submit_responses(bridge_responses).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("failed to submit responses: {e}"))
    })?;

    Ok(())
}

/// Get the request notification FD for asyncio `loop.add_reader()`.
///
/// Only available in SHM mode. Returns -1 if not in SHM mode.
#[pyfunction]
#[pyo3(name = "get_request_notify_fd")]
pub fn get_request_notify_fd() -> i32 {
    #[cfg(unix)]
    {
        if let Some(transport) = SHM_TRANSPORT.get() {
            if let vertebra_core::transport::Backend::SharedMemory(rp) = transport.backend() {
                return rp.request_notifier.as_raw_fd();
            }
        }
    }
    -1
}

/// Clear the request notification counter after an `add_reader` callback.
///
/// Must be called after reading from the request ring in async mode
/// to prevent spurious wakeups.
#[pyfunction]
#[pyo3(name = "clear_request_notification")]
pub fn clear_request_notification() -> PyResult<()> {
    if let Some(transport) = SHM_TRANSPORT.get() {
        if let vertebra_core::transport::Backend::SharedMemory(rp) = transport.backend() {
            rp.request_notifier.clear().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("clear failed: {e}"))
            })?;
        }
    }
    Ok(())
}

/// Non-blocking poll for the next batch (for use in `add_reader` callbacks).
///
/// Returns `None` immediately if no data is available.
#[pyfunction]
#[pyo3(name = "try_get_batch")]
pub fn try_get_batch() -> PyResult<Option<VertebraBatch>> {
    if let Some(transport) = SHM_TRANSPORT.get() {
        let snapshot = transport.poll_batch(Duration::ZERO);
        return Ok(snapshot.map(|s| VertebraBatch { snapshot: s }));
    }
    Ok(None)
}

/// Encode headers into Vertebra's compact binary format.
///
/// Format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`
fn encode_headers_compact(headers: &[(String, String)]) -> Vec<u8> {
    if headers.is_empty() {
        return Vec::new();
    }
    let mut buf = Vec::with_capacity(headers.len() * 32);
    for (name, value) in headers {
        let n = name.as_bytes();
        let v = value.as_bytes();
        buf.push(n.len() as u8);
        buf.extend_from_slice(n);
        let vlen = v.len() as u16;
        buf.extend_from_slice(&vlen.to_le_bytes());
        buf.extend_from_slice(v);
    }
    buf
}
