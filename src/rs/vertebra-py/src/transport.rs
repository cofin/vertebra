//! Vectorized batch FFI API and response submission.
//!
//! Connects Python workers to either:
//! - `BatchBridge` (in-process mode, default)
//! - `Transport::SharedMemory` (SHM mode, for forked workers)
//!
//! The transport mode is selected by the `mode` parameter of `init_transport()`.

use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;

use vertebra_core::bridge::{BatchBridge, BridgeResponse};
use vertebra_core::ipc::{
    FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING, RESPONSE_SLOT_HEADER_LEN, ResponseSlot,
};
use vertebra_core::transport::{
    Transport, TransportError, encode_headers_compact, iter_raw_headers,
};

use crate::request::RequestBatch;
use crate::response::Response;

/// Global SHM transport, set once by `init_transport(mode="shm")`.
static SHM_TRANSPORT: OnceLock<Transport> = OnceLock::new();

/// Initialize the transport channel. Called once during worker startup.
///
/// # Modes
/// - `mode="inprocess"` (default): Uses the in-process `BatchBridge` (crossbeam channels).
/// - `mode="shm"`: Reads `VERTEBRA_*` env vars to reconstruct a `RingPair`
///   from inherited file descriptors. Used by forked worker processes.
#[pyfunction]
#[pyo3(name = "init_transport", signature = (mode="inprocess", slot_size=4096, slots=64, batch_size=64))]
pub fn init_transport(
    mode: &str,
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
pub fn get_batch(py: Python<'_>, timeout_ms: u64) -> PyResult<Option<RequestBatch>> {
    // SHM path: poll from SPSC ring
    if let Some(transport) = SHM_TRANSPORT.get() {
        let snapshot = py.detach(|| transport.poll_batch(Duration::from_millis(timeout_ms)));
        return Ok(snapshot.map(|s| RequestBatch { snapshot: s }));
    }

    // In-process path: poll from BatchBridge
    let bridge = BatchBridge::get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "transport not initialized; call init_transport() first",
        )
    })?;

    let snapshot = py.detach(|| bridge.poll_batch(Duration::from_millis(timeout_ms)));
    Ok(snapshot.map(|s| RequestBatch { snapshot: s }))
}

/// Get the request notification FD for asyncio `loop.add_reader()`.
///
/// Only available in SHM mode. Returns -1 if not in SHM mode.
#[pyfunction]
#[pyo3(name = "get_request_notify_fd")]
pub fn get_request_notify_fd() -> i32 {
    #[cfg(unix)]
    {
        if let Some(transport) = SHM_TRANSPORT.get()
            && let vertebra_core::transport::Backend::SharedMemory(rp) = transport.backend()
        {
            return rp.request_notifier.as_raw_fd();
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
pub fn try_get_batch() -> PyResult<Option<RequestBatch>> {
    if let Some(transport) = SHM_TRANSPORT.get() {
        let snapshot = transport.poll_batch_async();
        return Ok(snapshot.map(|s| RequestBatch { snapshot: s }));
    }
    Ok(None)
}

fn decode_headers_from_raw(raw: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter_raw_headers(raw)
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect()
}

pub struct ResponseSubmitter;

impl ResponseSubmitter {
    pub fn submit_raw_inner(
        py: Python<'_>,
        request_id: u64,
        status: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> PyResult<()> {
        if let Some(transport) = SHM_TRANSPORT.get() {
            py.detach(|| -> Result<(), (bool, TransportError)> {
                let mut pending = false;
                send_shm_response_detached(
                    transport,
                    &mut pending,
                    request_id,
                    status,
                    headers_raw,
                    body,
                )
                .map_err(|e| (false, e))?;
                flush_shm_response_notification(transport, &mut pending).map_err(|e| (true, e))?;
                Ok(())
            })
            .map_err(|(is_notify, e)| {
                if is_notify {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to notify supervisor: {e}"
                    ))
                } else {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to send response: {e}"
                    ))
                }
            })?;
            return Ok(());
        }

        let bridge = BatchBridge::get().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "transport not initialized; call init_transport() first",
            )
        })?;

        let resp = BridgeResponse {
            request_id,
            status,
            headers: decode_headers_from_raw(headers_raw),
            body: Bytes::copy_from_slice(body),
        };
        py.detach(|| bridge.submit_responses(vec![resp]))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("failed to submit response: {e}"))
            })?;
        Ok(())
    }

    pub fn submit_batch_raw_inner(
        py: Python<'_>,
        items: &[(u64, u16, &[u8], &[u8])],
    ) -> PyResult<()> {
        if items.is_empty() {
            return Ok(());
        }

        if let Some(transport) = SHM_TRANSPORT.get() {
            py.detach(|| -> Result<(), (bool, TransportError)> {
                let mut pending = false;
                for (req_id, status, headers_raw, body) in items {
                    send_shm_response_detached(
                        transport,
                        &mut pending,
                        *req_id,
                        *status,
                        headers_raw,
                        body,
                    )
                    .map_err(|e| (false, e))?;
                }
                flush_shm_response_notification(transport, &mut pending).map_err(|e| (true, e))?;
                Ok(())
            })
            .map_err(|(is_notify, e)| {
                if is_notify {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to notify supervisor: {e}"
                    ))
                } else {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to send response: {e}"
                    ))
                }
            })?;
            return Ok(());
        }

        let bridge = BatchBridge::get().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "transport not initialized; call init_transport() first",
            )
        })?;

        let mut bridge_responses = Vec::with_capacity(items.len());
        for (req_id, status, headers_raw, body) in items {
            bridge_responses.push(BridgeResponse {
                request_id: *req_id,
                status: *status,
                headers: decode_headers_from_raw(headers_raw),
                body: Bytes::copy_from_slice(body),
            });
        }

        py.detach(|| bridge.submit_responses(bridge_responses))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to submit responses: {e}"
                ))
            })?;
        Ok(())
    }
}

/// Submit processed responses back to the HTTP server.
#[pyfunction]
#[pyo3(name = "submit_results")]
pub fn submit_results(py: Python<'_>, responses: Vec<Py<Response>>) -> PyResult<()> {
    let mut prepared = Vec::with_capacity(responses.len());
    for py_resp in &responses {
        let resp = py_resp.borrow(py);
        prepared.push((
            resp.request_id,
            resp.status,
            encode_headers_compact(&resp.headers),
            resp.body.clone(),
        ));
    }
    let items: Vec<(u64, u16, &[u8], &[u8])> = prepared
        .iter()
        .map(|(id, st, h, b)| (*id, *st, h.as_slice(), b.as_ref()))
        .collect();
    ResponseSubmitter::submit_batch_raw_inner(py, &items)
}

/// Submit a single processed response back to the HTTP server.
#[pyfunction]
#[pyo3(name = "submit_response", signature = (response))]
pub fn submit_response(py: Python<'_>, response: Py<Response>) -> PyResult<()> {
    let resp = response.borrow(py);
    let request_id = resp.request_id;
    let status = resp.status;
    let headers_raw = encode_headers_compact(&resp.headers);
    let body = resp.body.clone();
    drop(resp);
    ResponseSubmitter::submit_raw_inner(py, request_id, status, &headers_raw, &body)
}

/// Submit a raw response directly without allocating a `Response` pyclass instance.
#[pyfunction]
#[pyo3(name = "submit_response_raw", signature = (request_id, status, body, headers_raw=None))]
pub fn submit_response_raw(
    py: Python<'_>,
    request_id: u64,
    status: u16,
    body: &[u8],
    headers_raw: Option<&[u8]>,
) -> PyResult<()> {
    ResponseSubmitter::submit_raw_inner(py, request_id, status, headers_raw.unwrap_or(b""), body)
}

/// Submit a batch of raw responses directly without allocating `Response` pyclass instances.
#[pyfunction]
#[pyo3(name = "submit_batch_raw")]
pub fn submit_batch_raw(py: Python<'_>, items: Vec<(u64, u16, Vec<u8>)>) -> PyResult<()> {
    let prepared: Vec<(u64, u16, &[u8], &[u8])> = items
        .iter()
        .map(|(id, st, b)| (*id, *st, &[] as &[u8], b.as_slice()))
        .collect();
    ResponseSubmitter::submit_batch_raw_inner(py, &prepared)
}

/// Encode and submit a raw buffer-protocol response directly into `ResponseSlot` wire frames.
#[pyfunction]
#[pyo3(
    name = "submit_raw_response",
    signature = (request_id, status, headers, body, flags=FLAG_FINAL, slot_capacity=16384)
)]
pub fn submit_raw_response(
    py: Python<'_>,
    request_id: u64,
    status: u16,
    headers: Vec<(String, String)>,
    body: &Bound<'_, PyAny>,
    flags: u16,
    slot_capacity: usize,
) -> PyResult<Vec<Vec<u8>>> {
    let py_buf = PyBuffer::<u8>::get(body)?;
    let owned_fallback: Option<Vec<u8>> = if py_buf.is_c_contiguous() {
        None
    } else {
        Some(py_buf.to_vec(py)?)
    };

    let body_slice: &[u8] = match &owned_fallback {
        Some(vec) => vec.as_slice(),
        None => {
            let len = py_buf.item_count();
            if len == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(py_buf.buf_ptr() as *const u8, len) }
            }
        }
    };

    let cap = slot_capacity.max(RESPONSE_SLOT_HEADER_LEN + 64);
    let single_size = ResponseSlot::encoded_size(&headers, body_slice);
    let mut encoded_frames: Vec<Vec<u8>> = Vec::new();

    if single_size <= cap && (flags & FLAG_STREAMING) == 0 {
        let mut frame = vec![0u8; single_size];
        ResponseSlot::encode_raw_parts_into(
            request_id,
            status,
            flags | FLAG_FINAL,
            &headers,
            body_slice,
            &mut frame,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        if let Some(transport) = SHM_TRANSPORT.get() {
            let headers_raw = encode_headers_compact(
                &headers
                    .iter()
                    .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                    .collect::<Vec<_>>(),
            );
            py.detach(|| -> Result<(), (bool, TransportError)> {
                let mut pending_notify = false;
                send_shm_response_detached(
                    transport,
                    &mut pending_notify,
                    request_id,
                    status,
                    &headers_raw,
                    body_slice,
                )
                .map_err(|e| (false, e))?;
                flush_shm_response_notification(transport, &mut pending_notify)
                    .map_err(|e| (true, e))?;
                Ok(())
            })
            .map_err(|(is_notify, e)| {
                if is_notify {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to notify supervisor: {e}"
                    ))
                } else {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to send response: {e}"
                    ))
                }
            })?;
        }

        encoded_frames.push(frame);
        return Ok(encoded_frames);
    }

    let first_overhead = ResponseSlot::encoded_size(&headers, &[]);
    let first_max_body = cap.saturating_sub(first_overhead).max(1);
    let subsequent_overhead = ResponseSlot::encoded_size(&[], &[]);
    let subsequent_max_body = cap.saturating_sub(subsequent_overhead).max(1);

    let mut offset = 0usize;
    let total = body_slice.len();
    let mut is_first = true;

    while offset < total || is_first {
        let chunk_headers: &[(String, String)] = if is_first { &headers } else { &[] };
        let max_chunk = if is_first {
            first_max_body
        } else {
            subsequent_max_body
        };
        let end = (offset + max_chunk).min(total);
        let chunk = &body_slice[offset..end];
        let is_last = end >= total;

        let mut chunk_flags = FLAG_STREAMING;
        if is_first {
            chunk_flags |= FLAG_HEADERS_SENT;
        }
        if is_last {
            chunk_flags |= FLAG_FINAL;
        }

        let frame_size = ResponseSlot::encoded_size(chunk_headers, chunk);
        let mut frame = vec![0u8; frame_size];
        ResponseSlot::encode_raw_parts_into(
            request_id,
            status,
            chunk_flags,
            chunk_headers,
            chunk,
            &mut frame,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        if let Some(transport) = SHM_TRANSPORT.get() {
            let headers_raw = encode_headers_compact(
                &chunk_headers
                    .iter()
                    .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                    .collect::<Vec<_>>(),
            );
            py.detach(|| -> Result<(), (bool, TransportError)> {
                let mut pending_notify = false;
                send_shm_response_detached(
                    transport,
                    &mut pending_notify,
                    request_id,
                    status,
                    &headers_raw,
                    chunk,
                )
                .map_err(|e| (false, e))?;
                flush_shm_response_notification(transport, &mut pending_notify)
                    .map_err(|e| (true, e))?;
                Ok(())
            })
            .map_err(|(is_notify, e)| {
                if is_notify {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to notify supervisor: {e}"
                    ))
                } else {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to send response: {e}"
                    ))
                }
            })?;
        }

        encoded_frames.push(frame);
        offset = end;
        is_first = false;
    }

    Ok(encoded_frames)
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

fn send_shm_response_detached(
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
            transport.notify_response()?;
            *pending_notification = false;

            transport.send_response_blocking(request_id, status, headers_raw, body)?;

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

        let mut pending_notification = false;
        pyo3::Python::attach(|py| {
            let reader = std::thread::spawn(move || {
                loop {
                    if drained_ids_reader.lock().expect("lock ids").len() >= 2 {
                        break;
                    }

                    assert!(
                        pair_reader
                            .response_notifier
                            .wait(5_000)
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
                }
            });

            py.detach(|| {
                send_shm_response_detached(
                    &transport,
                    &mut pending_notification,
                    1,
                    200,
                    b"",
                    b"first",
                )
                .expect("send first response");
                send_shm_response_detached(
                    &transport,
                    &mut pending_notification,
                    2,
                    200,
                    b"",
                    b"second",
                )
                .expect("recover from ring full");

                flush_shm_response_notification(&transport, &mut pending_notification)
                    .expect("flush final notification");
            });

            py.detach(|| {
                reader.join().expect("reader thread should exit");
            });
        });

        let drained_ids = drained_ids.lock().expect("lock ids");
        assert_eq!(drained_ids.as_slice(), &[1, 2]);
    }
}
