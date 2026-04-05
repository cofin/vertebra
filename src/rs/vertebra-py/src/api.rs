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
use vertebra_core::transport::{Transport, TransportError};

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
                use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

                let config = RingPairConfig::default();
                let ring_pair = RingPair::from_env(config).map_err(|e| {
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
        _ => {
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
        if responses.is_empty() {
            return Ok(());
        }

        let mut pending_notification = false;
        for py_resp in &responses {
            let resp = py_resp.borrow(py);
            let headers_raw = encode_headers_compact(&resp.headers);
            send_shm_response_with_backpressure(
                py,
                transport,
                &mut pending_notification,
                resp.request_id,
                resp.status,
                &headers_raw,
                &resp.body,
            )
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("failed to send response: {e}"))
            })?;
        }

        flush_shm_response_notification(transport, &mut pending_notification).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to notify supervisor: {e}"))
        })?;

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
            body: resp.body.clone(), // This is now a cheap Bytes clone
        });
    }

    bridge.submit_responses(bridge_responses).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("failed to submit responses: {e}"))
    })?;

    Ok(())
}

/// Submit a single processed response back to the HTTP server.
///
/// This is used for fire-and-forget asynchronous completion.
#[pyfunction]
pub fn submit_response(py: Python<'_>, py_resp: Py<VertebraResponse>) -> PyResult<()> {
    let resp = py_resp.borrow(py);

    // SHM path: write response to tiered ring
    if let Some(transport) = SHM_TRANSPORT.get() {
        let mut pending_notification = false;
        let headers_raw = encode_headers_compact(&resp.headers);
        send_shm_response_with_backpressure(
            py,
            transport,
            &mut pending_notification,
            resp.request_id,
            resp.status,
            &headers_raw,
            &resp.body,
        )
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to send response: {e}"))
        })?;

        flush_shm_response_notification(transport, &mut pending_notification).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to notify supervisor: {e}"))
        })?;

        return Ok(());
    }

    // In-process path: BatchBridge
    let bridge = BatchBridge::get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "batch channel not initialized; call init_batch_channel() first",
        )
    })?;

    bridge
        .submit_responses(vec![BridgeResponse {
            request_id: resp.request_id,
            status: resp.status,
            headers: resp.headers.clone(),
            body: resp.body.clone(),
        }])
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to submit response: {e}"))
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

#[pyfunction]
#[pyo3(name = "clear_request_notification")]
pub fn clear_request_notification() -> PyResult<()> {
    if let Some(transport) = SHM_TRANSPORT.get() {
        transport
            .clear_request()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("clear failed: {e}")))?;
    }
    Ok(())
}

#[pyfunction]
#[pyo3(name = "try_get_batch")]
pub fn try_get_batch() -> PyResult<Option<VertebraBatch>> {
    if let Some(transport) = SHM_TRANSPORT.get() {
        let snapshot = transport.poll_batch_async();
        return Ok(snapshot.map(|s| VertebraBatch { snapshot: s }));
    }
    Ok(None)
}

/// Encode headers into Vertebra's compact binary format.
///
/// Format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`
fn encode_headers_compact(headers: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    if headers.is_empty() {
        return Vec::new();
    }
    let mut buf = Vec::with_capacity(headers.len() * 32);
    for (name, value) in headers {
        buf.push(name.len() as u8);
        buf.extend_from_slice(name);
        let vlen = value.len() as u16;
        buf.extend_from_slice(&vlen.to_le_bytes());
        buf.extend_from_slice(value);
    }
    buf
}

fn flush_shm_response_notification(
    transport: &Transport,
    pending_notification: &mut bool,
) -> Result<(), TransportError> {
    if *pending_notification {
        transport.notify_response()?;
        *pending_notification = false;
    }
    Ok(())
}

fn send_shm_response_with_backpressure(
    py: Python<'_>,
    transport: &Transport,
    pending_notification: &mut bool,
    request_id: u64,
    status: u16,
    headers_raw: &[u8],
    body: &[u8],
) -> Result<(), TransportError> {
    match transport.send_response(request_id, status, headers_raw, body) {
        Ok(()) => {
            *pending_notification = true;
            Ok(())
        }
        Err(TransportError::RingFull) => {
            // Wake the supervisor before waiting so previously queued responses
            // can drain and make room for the current one.
            transport.notify_response()?;
            *pending_notification = false;

            // Release the GIL while blocking on the ring buffer
            py.detach(|| transport.send_response_blocking(request_id, status, headers_raw, body))?;

            *pending_notification = true;
            Ok(())
        }
        Err(err) => {
            flush_shm_response_notification(transport, pending_notification)?;
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    use super::*;

    #[test]
    fn shm_submit_notifies_and_recovers_from_mid_batch_ring_full() {
        let mut config = RingPairConfig::default();
        config.response_tiers.small.slot_count = 2;

        let pair = Arc::new(RingPair::create(config).expect("create ring pair"));
        let transport = Transport::new_shared_memory(Arc::clone(&pair));

        let drained_ids = Arc::new(Mutex::new(Vec::new()));
        let drained_ids_reader = Arc::clone(&drained_ids);
        let pair_reader = Arc::clone(&pair);
        let reader = std::thread::spawn(move || loop {
            if drained_ids_reader.lock().expect("lock ids").len() >= 2 {
                break;
            }

            assert!(
                pair_reader
                    .response_notifier
                    .wait(100)
                    .expect("wait for response notification"),
                "response notification should arrive"
            );
            pair_reader
                .response_notifier
                .clear()
                .expect("clear response notifier");

            while let Some(resp) = pair_reader
                .response_rings
                .try_recv_response()
                .expect("drain response ring")
            {
                drained_ids_reader
                    .lock()
                    .expect("lock ids")
                    .push(resp.correlation_id);
            }
        });

        let mut pending_notification = false;
        pyo3::Python::attach(|py| {
            send_shm_response_with_backpressure(
                py,
                &transport,
                &mut pending_notification,
                1,
                200,
                b"",
                b"first",
            )
            .expect("send first response");
            send_shm_response_with_backpressure(
                py,
                &transport,
                &mut pending_notification,
                2,
                200,
                b"",
                b"second",
            )
            .expect("recover from ring full");
        });
        flush_shm_response_notification(&transport, &mut pending_notification)
            .expect("flush final notification");

        reader.join().expect("reader thread should exit");

        let drained_ids = drained_ids.lock().expect("lock ids");
        assert_eq!(drained_ids.as_slice(), &[1, 2]);
    }
}
