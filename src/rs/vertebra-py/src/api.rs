//! Vectorized batch FFI API.
//!
//! Connects Python workers to the arena-backed `BatchBridge` from
//! `vertebra-core`. Python calls `init_batch_channel()` to verify the
//! bridge is ready, then polls with `get_batch()` and submits results
//! with `submit_results()`.

use std::time::Duration;

use pyo3::prelude::*;

use vertebra_core::bridge::{BatchBridge, BridgeResponse};

use crate::batch::VertebraBatch;
use crate::response::VertebraResponse;

/// Initialize the transport channel. Called once during worker startup.
#[pyfunction]
#[pyo3(name = "init_transport", signature = (ring_name="default", slot_size=4096, slots=64, batch_size=64))]
pub fn init_transport(
    ring_name: &str,
    slot_size: usize,
    slots: usize,
    batch_size: usize,
) -> PyResult<()> {
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

/// Fetch the next batch of requests as a vectorized object. GIL released.
#[pyfunction]
#[pyo3(name = "get_batch", signature = (timeout_ms=100))]
pub fn get_batch(py: Python<'_>, timeout_ms: u64) -> PyResult<Option<VertebraBatch>> {
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
/// Takes a list of `VertebraResponse` objects and pushes them through the
/// bridge to complete the corresponding Hyper response futures.
#[pyfunction]
pub fn submit_results(py: Python<'_>, responses: Vec<Py<VertebraResponse>>) -> PyResult<()> {
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
