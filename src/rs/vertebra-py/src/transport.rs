//! Vectorized batch FFI API and response submission.
//!
//! Connects Python workers to either:
//! - `BatchBridge` (in-process mode, default)
//! - `Transport::SharedMemory` (SHM mode, for forked workers)
//!
//! The transport mode is selected by the `mode` parameter of `init_transport()`.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyConnectionResetError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyList, PyTuple};

use vertebra_core::bridge::{BatchBridge, BridgeResponse, ResponseStreamFrame, StreamDelivery};
use vertebra_core::buffer::{ChannelMode, RingLayout};
use vertebra_core::ipc::{
    FLAG_ERROR, FLAG_FINAL, FLAG_HEADERS_SENT, FLAG_STREAMING, FLAG_TRAILERS,
    RESPONSE_SLOT_HEADER_LEN, ResponseSlot,
};
use vertebra_core::tiered_ring::TieredRingConfig;
use vertebra_core::transport::{Backend, Transport, TransportError, encode_headers_compact};

use crate::asgi::encode_py_headers_into;
use crate::request::RequestBatch;
use crate::response::Response;

/// Global SHM transport, set once by `init_transport(mode="shm")`.
static SHM_TRANSPORT: OnceLock<Transport> = OnceLock::new();
const STREAM_FRAME_WIRE_OVERHEAD: usize = 22;

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
        if let Some(s) = transport.poll_batch_async() {
            return Ok(Some(RequestBatch { snapshot: s }));
        }
        let snapshot = py.detach(|| transport.poll_batch(Duration::from_millis(timeout_ms)));
        return Ok(snapshot.map(|s| RequestBatch { snapshot: s }));
    }

    // In-process path: poll from BatchBridge
    let bridge = BatchBridge::get().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "transport not initialized; call init_transport() first",
        )
    })?;

    if let Some(s) = bridge.try_poll_batch() {
        return Ok(Some(RequestBatch { snapshot: s }));
    }

    let snapshot = py.detach(|| bridge.poll_batch(Duration::from_millis(timeout_ms)));
    Ok(snapshot.map(|s| RequestBatch { snapshot: s }))
}

/// Get the request notification FD for asyncio `loop.add_reader()`.
///
/// Available in SHM mode and single-consumer Embedded mode. Returns -1 otherwise.
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
        if let Some(bridge) = BatchBridge::get() {
            return bridge.request_notify_fd();
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
        return Ok(());
    }
    if let Some(bridge) = BatchBridge::get() {
        bridge
            .clear_request_notification()
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
    if let Some(bridge) = BatchBridge::get() {
        let snapshot = bridge.try_poll_batch();
        return Ok(snapshot.map(|s| RequestBatch { snapshot: s }));
    }
    Ok(None)
}

/// Arm Dekker sleep on the request queue (`true` when empty and safe to park).
#[pyfunction]
#[pyo3(name = "prepare_request_sleep")]
pub fn prepare_request_sleep() -> bool {
    if let Some(transport) = SHM_TRANSPORT.get() {
        return transport.prepare_request_sleep();
    }
    if let Some(bridge) = BatchBridge::get() {
        return bridge.prepare_request_sleep();
    }
    false
}

/// Cancel Dekker sleep on the request queue.
#[pyfunction]
#[pyo3(name = "cancel_request_sleep")]
pub fn cancel_request_sleep() {
    if let Some(transport) = SHM_TRANSPORT.get() {
        transport.cancel_request_sleep();
        return;
    }
    if let Some(bridge) = BatchBridge::get() {
        bridge.cancel_request_sleep();
    }
}

const INLINE_SUBMIT_THRESHOLD: usize = 65_536;

#[inline]
pub(crate) fn with_buffer_slice<R>(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    f: impl FnOnce(&[u8]) -> PyResult<R>,
) -> PyResult<R> {
    if let Ok(py_bytes) = obj.cast::<PyBytes>() {
        return f(py_bytes.as_bytes());
    }
    let py_buf = PyBuffer::<u8>::get(obj)?;
    if py_buf.is_c_contiguous() {
        let len = py_buf.item_count();
        let slice = if len == 0 {
            &[][..]
        } else {
            // SAFETY: `py_buf` is verified C-contiguous (`PyBuffer<u8>`), `len > 0`
            // ensures `buf_ptr()` is non-null and valid for `len` bytes, and `py_buf`
            // stays alive across `f(slice)` while the GIL is held.
            unsafe { std::slice::from_raw_parts(py_buf.buf_ptr() as *const u8, len) }
        };
        let res = f(slice);
        drop(py_buf);
        res
    } else {
        let owned = py_buf.to_vec(py)?;
        drop(py_buf);
        f(&owned)
    }
}

#[inline]
fn map_shm_submit_err((is_notify, err): (bool, TransportError)) -> PyErr {
    if is_notify {
        PyRuntimeError::new_err(format!("failed to notify supervisor: {err}"))
    } else {
        PyRuntimeError::new_err(format!("failed to send response: {err}"))
    }
}

#[inline]
fn send_shm_response_inline_or_detached(
    py: Python<'_>,
    transport: &Transport,
    pending_notification: &mut bool,
    request_id: u64,
    status: u16,
    headers_raw: &[u8],
    body: &[u8],
) -> Result<(), (bool, TransportError)> {
    let payload_len = headers_raw.len().saturating_add(body.len());
    if payload_len <= INLINE_SUBMIT_THRESHOLD {
        match transport.send_response(request_id, status, headers_raw, body) {
            Ok(()) => {
                *pending_notification = true;
                return Ok(());
            }
            Err(TransportError::RingFull) => {
                return py.detach(|| -> Result<(), (bool, TransportError)> {
                    transport.notify_response().map_err(|e| (true, e))?;
                    *pending_notification = false;
                    transport
                        .send_response_blocking(request_id, status, headers_raw, body)
                        .map_err(|e| (false, e))?;
                    *pending_notification = true;
                    Ok(())
                });
            }
            Err(err) => {
                flush_shm_response_notification(transport, pending_notification)
                    .map_err(|e| (true, e))?;
                return Err((false, err));
            }
        }
    }

    py.detach(|| -> Result<(), (bool, TransportError)> {
        send_shm_response_detached(
            transport,
            pending_notification,
            request_id,
            status,
            headers_raw,
            body,
        )
        .map_err(|e| (false, e))
    })
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
            let mut pending = false;
            send_shm_response_inline_or_detached(
                py,
                transport,
                &mut pending,
                request_id,
                status,
                headers_raw,
                body,
            )
            .map_err(map_shm_submit_err)?;
            flush_shm_response_notification(transport, &mut pending)
                .map_err(|e| map_shm_submit_err((true, e)))?;
            return Ok(());
        }

        let bridge = BatchBridge::get().ok_or_else(|| {
            PyRuntimeError::new_err("transport not initialized; call init_transport() first")
        })?;

        let payload_len = headers_raw.len().saturating_add(body.len());
        let resp = BridgeResponse {
            request_id,
            status,
            headers_raw: if headers_raw.is_empty() {
                Bytes::new()
            } else {
                Bytes::copy_from_slice(headers_raw)
            },
            body: if body.is_empty() {
                Bytes::new()
            } else {
                Bytes::copy_from_slice(body)
            },
        };
        let res = if payload_len <= INLINE_SUBMIT_THRESHOLD {
            bridge.submit_response(resp)
        } else {
            py.detach(|| bridge.submit_response(resp))
        };
        res.map_err(|e| PyRuntimeError::new_err(format!("failed to submit response: {e}")))?;
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
            let mut pending = false;
            for (req_id, status, headers_raw, body) in items {
                send_shm_response_inline_or_detached(
                    py,
                    transport,
                    &mut pending,
                    *req_id,
                    *status,
                    headers_raw,
                    body,
                )
                .map_err(map_shm_submit_err)?;
            }
            flush_shm_response_notification(transport, &mut pending)
                .map_err(|e| map_shm_submit_err((true, e)))?;
            return Ok(());
        }

        let bridge = BatchBridge::get().ok_or_else(|| {
            PyRuntimeError::new_err("transport not initialized; call init_transport() first")
        })?;

        let mut total_bytes = 0usize;
        let mut bridge_responses = Vec::with_capacity(items.len());
        for (req_id, status, headers_raw, body) in items {
            total_bytes = total_bytes
                .saturating_add(headers_raw.len())
                .saturating_add(body.len());
            bridge_responses.push(BridgeResponse {
                request_id: *req_id,
                status: *status,
                headers_raw: if headers_raw.is_empty() {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(headers_raw)
                },
                body: if body.is_empty() {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(body)
                },
            });
        }

        let res = if total_bytes <= INLINE_SUBMIT_THRESHOLD {
            bridge.submit_responses(bridge_responses)
        } else {
            py.detach(|| bridge.submit_responses(bridge_responses))
        };
        res.map_err(|e| PyRuntimeError::new_err(format!("failed to submit responses: {e}")))?;
        Ok(())
    }

    pub fn submit_stream_frame_inner(
        py: Python<'_>,
        request_id: u64,
        status: u16,
        flags: u16,
        headers_raw: &[u8],
        body: &[u8],
    ) -> PyResult<()> {
        if (flags & FLAG_STREAMING) == 0 {
            return Err(PyValueError::new_err(
                "stream frame flags must include STREAM_FLAG_STREAMING",
            ));
        }
        if headers_raw.len() > u16::MAX as usize {
            return Err(PyValueError::new_err(
                "stream frame headers exceed 65535 bytes",
            ));
        }

        if let Some(transport) = SHM_TRANSPORT.get() {
            return submit_shm_stream_frames(
                py,
                transport,
                request_id,
                status,
                flags,
                headers_raw,
                body,
            );
        }

        let bridge = BatchBridge::get().ok_or_else(|| {
            PyRuntimeError::new_err("transport not initialized; call init_transport() first")
        })?;

        let max_payload = default_max_stream_frame_payload();
        if max_payload == 0 || headers_raw.len() > max_payload {
            return Err(PyValueError::new_err(
                "stream frame headers exceed max_stream_frame_payload",
            ));
        }

        let delivery = py
            .detach(|| -> Result<StreamDelivery, TransportError> {
                let mut current_delivery = StreamDelivery::Accepted;
                for_each_stream_chunk(
                    flags,
                    headers_raw,
                    body,
                    max_payload,
                    |chunk_flags, chunk_headers, chunk_body| {
                        if current_delivery == StreamDelivery::Closed {
                            return Ok(());
                        }
                        let frame = ResponseStreamFrame {
                            request_id,
                            status,
                            flags: chunk_flags,
                            headers_raw: if chunk_headers.is_empty() {
                                Bytes::new()
                            } else {
                                Bytes::copy_from_slice(chunk_headers)
                            },
                            body: if chunk_body.is_empty() {
                                Bytes::new()
                            } else {
                                Bytes::copy_from_slice(chunk_body)
                            },
                        };
                        current_delivery = bridge.submit_stream_frame(frame)?;
                        Ok(())
                    },
                )?;
                Ok(current_delivery)
            })
            .map_err(|e| PyRuntimeError::new_err(format!("failed to submit stream frame: {e}")))?;

        match delivery {
            StreamDelivery::Accepted => Ok(()),
            StreamDelivery::Closed => Err(PyConnectionResetError::new_err("stream closed by peer")),
        }
    }
}

#[inline]
fn default_max_stream_frame_payload() -> usize {
    let tier = TieredRingConfig::default().xlarge;
    let layout = RingLayout::new(tier.slot_size, tier.slot_count, ChannelMode::Spsc);
    layout
        .slot_size
        .saturating_sub(8)
        .saturating_sub(STREAM_FRAME_WIRE_OVERHEAD)
}

#[inline]
fn transport_max_stream_frame_payload(transport: &Transport) -> usize {
    if let Backend::SharedMemory(rp) = transport.backend() {
        return rp.max_stream_frame_payload();
    }
    default_max_stream_frame_payload()
}

#[inline]
fn for_each_stream_chunk<E>(
    flags: u16,
    headers_raw: &[u8],
    body: &[u8],
    max_payload: usize,
    mut emit: impl FnMut(u16, &[u8], &[u8]) -> Result<(), E>,
) -> Result<(), E> {
    if headers_raw.len().saturating_add(body.len()) <= max_payload {
        return emit(flags, headers_raw, body);
    }

    let terminal_mask = FLAG_FINAL | FLAG_ERROR | FLAG_TRAILERS;
    let first_base_flags = (flags & !terminal_mask) | FLAG_STREAMING;
    let first_body_len = max_payload
        .saturating_sub(headers_raw.len())
        .min(body.len());
    let first_is_last = first_body_len == body.len();
    let first_flags = if first_is_last {
        first_base_flags | (flags & terminal_mask)
    } else {
        first_base_flags
    };
    emit(first_flags, headers_raw, &body[..first_body_len])?;

    let mut offset = first_body_len;
    let cont_base_flags = first_base_flags & !FLAG_HEADERS_SENT;
    while offset < body.len() {
        let end = (offset + max_payload).min(body.len());
        let is_last = end == body.len();
        let chunk_flags = if is_last {
            cont_base_flags | (flags & terminal_mask)
        } else {
            cont_base_flags
        };
        emit(chunk_flags, b"", &body[offset..end])?;
        offset = end;
    }
    Ok(())
}

/// Return the maximum combined compact-header and body byte payload of a single `VRSF` stream frame.
#[pyfunction]
#[pyo3(name = "max_stream_frame_payload")]
pub fn max_stream_frame_payload() -> usize {
    if let Some(transport) = SHM_TRANSPORT.get() {
        return transport_max_stream_frame_payload(transport);
    }
    default_max_stream_frame_payload()
}

/// Submit an ordered streaming response frame across the active embedded or SHM transport.
#[pyfunction]
#[pyo3(
    name = "submit_stream_frame",
    signature = (request_id, status, flags, headers, body)
)]
pub fn submit_stream_frame(
    py: Python<'_>,
    request_id: u64,
    status: u16,
    flags: u16,
    headers: Option<&Bound<'_, PyAny>>,
    body: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let mut headers_raw = Vec::new();
    if let Some(hdrs) = headers
        && !hdrs.is_none()
    {
        let mut has_cl = false;
        encode_py_headers_into(py, hdrs, &mut headers_raw, &mut has_cl)?;
    }
    with_buffer_slice(py, body, |body_slice| {
        ResponseSubmitter::submit_stream_frame_inner(
            py,
            request_id,
            status,
            flags,
            &headers_raw,
            body_slice,
        )
    })
}

/// Register or clear an in-process Python stream sink callback for unit testing.
#[pyfunction]
#[pyo3(name = "_set_stream_sink", signature = (sink=None))]
pub fn set_stream_sink_py(_py: Python<'_>, sink: Option<Py<PyAny>>) -> PyResult<()> {
    let bridge = BatchBridge::get().ok_or_else(|| {
        PyRuntimeError::new_err("transport not initialized; call init_transport() first")
    })?;
    let Some(cb) = sink else {
        bridge.clear_stream_sink();
        return Ok(());
    };
    bridge.set_stream_sink(Arc::new(move |frame: ResponseStreamFrame| {
        Python::attach(|py| {
            let pairs: Vec<(Bound<'_, PyBytes>, Bound<'_, PyBytes>)> = frame
                .iter_headers()
                .map(|(k, v)| (PyBytes::new(py, k), PyBytes::new(py, v)))
                .collect();
            let Ok(py_headers) = PyList::new(py, pairs) else {
                return StreamDelivery::Closed;
            };
            let py_body = PyBytes::new(py, &frame.body);
            match cb.call1(
                py,
                (
                    frame.request_id,
                    frame.status,
                    frame.flags,
                    py_headers,
                    py_body,
                ),
            ) {
                Ok(ret) => {
                    if ret.bind(py).is(PyBool::new(py, false)) {
                        StreamDelivery::Closed
                    } else {
                        StreamDelivery::Accepted
                    }
                }
                Err(_) => StreamDelivery::Closed,
            }
        })
    }));
    Ok(())
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
    body: &Bound<'_, PyAny>,
    headers_raw: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    with_buffer_slice(py, body, |body_slice| match headers_raw {
        Some(hdr) if !hdr.is_none() => with_buffer_slice(py, hdr, |hdr_slice| {
            ResponseSubmitter::submit_raw_inner(py, request_id, status, hdr_slice, body_slice)
        }),
        _ => ResponseSubmitter::submit_raw_inner(py, request_id, status, b"", body_slice),
    })
}

type RawBatchTupleParts<'py> = (u64, u16, Bound<'py, PyAny>, Option<Bound<'py, PyAny>>);

#[inline]
fn extract_batch_tuple_parts<'py>(item: &Bound<'py, PyAny>) -> PyResult<RawBatchTupleParts<'py>> {
    let tup = item
        .cast::<PyTuple>()
        .map_err(|_| PyTypeError::new_err("submit_batch_raw items must be tuples"))?;
    let len = tup.len();
    if len != 3 && len != 4 {
        return Err(PyValueError::new_err(
            "submit_batch_raw tuples must have 3 or 4 elements",
        ));
    }
    let req_id: u64 = tup.get_item(0)?.extract()?;
    let status: u16 = tup.get_item(1)?.extract()?;
    let body_obj = tup.get_item(2)?;
    let headers_obj = if len == 4 {
        let h = tup.get_item(3)?;
        if h.is_none() { None } else { Some(h) }
    } else {
        None
    };
    Ok((req_id, status, body_obj, headers_obj))
}

/// Submit a batch of raw responses directly without allocating `Response` pyclass instances.
#[pyfunction]
#[pyo3(name = "submit_batch_raw", signature = (items))]
pub fn submit_batch_raw(py: Python<'_>, items: &Bound<'_, PyAny>) -> PyResult<()> {
    if let Some(transport) = SHM_TRANSPORT.get() {
        let mut pending = false;
        let mut process_one = |item: Bound<'_, PyAny>| -> PyResult<()> {
            let (req_id, status, body_obj, headers_obj) = extract_batch_tuple_parts(&item)?;
            with_buffer_slice(py, &body_obj, |body_slice| match headers_obj {
                Some(ref hdr) => with_buffer_slice(py, hdr, |hdr_slice| {
                    send_shm_response_inline_or_detached(
                        py,
                        transport,
                        &mut pending,
                        req_id,
                        status,
                        hdr_slice,
                        body_slice,
                    )
                    .map_err(map_shm_submit_err)
                }),
                None => send_shm_response_inline_or_detached(
                    py,
                    transport,
                    &mut pending,
                    req_id,
                    status,
                    b"",
                    body_slice,
                )
                .map_err(map_shm_submit_err),
            })
        };

        if let Ok(list) = items.cast::<PyList>() {
            if list.is_empty() {
                return Ok(());
            }
            for item in list.iter() {
                if let Err(err) = process_one(item) {
                    let _ = flush_shm_response_notification(transport, &mut pending);
                    return Err(err);
                }
            }
        } else {
            for item in items.try_iter()? {
                if let Err(err) = process_one(item?) {
                    let _ = flush_shm_response_notification(transport, &mut pending);
                    return Err(err);
                }
            }
        }

        flush_shm_response_notification(transport, &mut pending)
            .map_err(|e| map_shm_submit_err((true, e)))?;
        return Ok(());
    }

    let bridge = BatchBridge::get().ok_or_else(|| {
        PyRuntimeError::new_err("transport not initialized; call init_transport() first")
    })?;

    let cap = items.len().unwrap_or(8);
    if cap == 0 && (items.cast::<PyList>().is_ok() || items.cast::<PyTuple>().is_ok()) {
        return Ok(());
    }

    let mut total_bytes = 0usize;
    let mut bridge_responses = Vec::with_capacity(cap);
    let mut collect_one = |item: Bound<'_, PyAny>| -> PyResult<()> {
        let (req_id, status, body_obj, headers_obj) = extract_batch_tuple_parts(&item)?;
        with_buffer_slice(py, &body_obj, |body_slice| {
            let mut push_with_hdr = |hdr_slice: &[u8]| -> PyResult<()> {
                total_bytes = total_bytes
                    .saturating_add(hdr_slice.len())
                    .saturating_add(body_slice.len());
                bridge_responses.push(BridgeResponse {
                    request_id: req_id,
                    status,
                    headers_raw: if hdr_slice.is_empty() {
                        Bytes::new()
                    } else {
                        Bytes::copy_from_slice(hdr_slice)
                    },
                    body: if body_slice.is_empty() {
                        Bytes::new()
                    } else {
                        Bytes::copy_from_slice(body_slice)
                    },
                });
                Ok(())
            };
            match headers_obj {
                Some(ref hdr) => with_buffer_slice(py, hdr, push_with_hdr),
                None => push_with_hdr(b""),
            }
        })
    };

    if let Ok(list) = items.cast::<PyList>() {
        for item in list.iter() {
            collect_one(item)?;
        }
    } else {
        for item in items.try_iter()? {
            collect_one(item?)?;
        }
    }

    if bridge_responses.is_empty() {
        return Ok(());
    }

    let res = if total_bytes <= INLINE_SUBMIT_THRESHOLD {
        bridge.submit_responses(bridge_responses)
    } else {
        py.detach(|| bridge.submit_responses(bridge_responses))
    };
    res.map_err(|e| PyRuntimeError::new_err(format!("failed to submit responses: {e}")))?;
    Ok(())
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
                // SAFETY: `py_buf` is C-contiguous (`owned_fallback` is `None`), `len > 0`
                // guarantees `buf_ptr()` is non-null and valid for `len` bytes, and `py_buf`
                // remains alive until the end of `submit_raw_response`.
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
            if (flags & FLAG_STREAMING) == 0 || (flags & FLAG_FINAL) != 0 {
                chunk_flags |= FLAG_FINAL;
            }
            chunk_flags |= flags & (FLAG_TRAILERS | FLAG_ERROR);
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

        let headers_raw = encode_headers_compact(
            &chunk_headers
                .iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .collect::<Vec<_>>(),
        );

        if let Some(transport) = SHM_TRANSPORT.get() {
            submit_shm_stream_frames(
                py,
                transport,
                request_id,
                status,
                chunk_flags,
                &headers_raw,
                chunk,
            )?;
        } else if let Some(bridge) = BatchBridge::get() {
            let stream_frame = ResponseStreamFrame {
                request_id,
                status,
                flags: chunk_flags,
                headers_raw: if headers_raw.is_empty() {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(&headers_raw)
                },
                body: if chunk.is_empty() {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(chunk)
                },
            };
            match py.detach(|| bridge.submit_stream_frame(stream_frame)) {
                Ok(StreamDelivery::Accepted) | Err(TransportError::Disconnected) => {}
                Ok(StreamDelivery::Closed) => {
                    return Err(PyConnectionResetError::new_err("stream closed by peer"));
                }
                Err(err) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "failed to submit stream frame: {err}"
                    )));
                }
            }
        }

        encoded_frames.push(frame);
        offset = end;
        is_first = false;
    }

    Ok(encoded_frames)
}

fn send_shm_stream_frame_detached(
    transport: &Transport,
    pending_notification: &mut bool,
    request_id: u64,
    status: u16,
    flags: u16,
    headers_raw: &[u8],
    body: &[u8],
) -> Result<(), TransportError> {
    let Backend::SharedMemory(rp) = transport.backend() else {
        return Err(TransportError::Disconnected);
    };
    match rp.send_stream_frame(request_id, status, flags, headers_raw, body) {
        Ok(()) => {
            *pending_notification = true;
            Ok(())
        }
        Err(vertebra_core::transport::ring_pair::RingPairError::Channel(
            vertebra_core::channel::ChannelError::Full,
        )) => {
            transport.notify_response()?;
            *pending_notification = false;
            rp.send_stream_frame_blocking(request_id, status, flags, headers_raw, body)
                .map_err(|_| TransportError::Disconnected)?;
            *pending_notification = true;
            Ok(())
        }
        Err(_) => {
            flush_shm_response_notification(transport, pending_notification)?;
            Err(TransportError::Disconnected)
        }
    }
}

fn submit_shm_stream_frames(
    py: Python<'_>,
    transport: &Transport,
    request_id: u64,
    status: u16,
    flags: u16,
    headers_raw: &[u8],
    body: &[u8],
) -> PyResult<()> {
    let max_payload = transport_max_stream_frame_payload(transport);
    if max_payload == 0 || headers_raw.len() > max_payload {
        return Err(PyValueError::new_err(
            "stream frame headers exceed max_stream_frame_payload",
        ));
    }
    py.detach(|| -> Result<(), (bool, TransportError)> {
        let mut pending = false;
        for_each_stream_chunk(
            flags,
            headers_raw,
            body,
            max_payload,
            |chunk_flags, chunk_headers, chunk_body| {
                send_shm_stream_frame_detached(
                    transport,
                    &mut pending,
                    request_id,
                    status,
                    chunk_flags,
                    chunk_headers,
                    chunk_body,
                )
                .map_err(|e| (false, e))
            },
        )?;
        flush_shm_response_notification(transport, &mut pending).map_err(|e| (true, e))?;
        Ok(())
    })
    .map_err(map_shm_submit_err)
}

fn flush_shm_response_notification(
    transport: &Transport,
    pending_notification: &mut bool,
) -> Result<(), TransportError> {
    if *pending_notification {
        let _ = transport.notify_response_if_sleeping()?;
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

                    if drained_ids_reader.lock().expect("lock ids").len() >= 2 {
                        break;
                    }

                    if pair_reader.prepare_response_sleep() {
                        assert!(
                            pair_reader
                                .response_notifier
                                .wait(5_000)
                                .expect("wait for response notification"),
                            "response notification should arrive"
                        );
                        pair_reader.cancel_response_sleep();
                        pair_reader
                            .response_notifier
                            .clear()
                            .expect("clear response notifier");
                    } else {
                        pair_reader.cancel_response_sleep();
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

    #[test]
    fn shm_stream_frames_split_above_max_payload_and_recover_ring_full() {
        let default_pair =
            RingPair::create(RingPairConfig::default()).expect("create default ring pair");
        assert_eq!(
            default_max_stream_frame_payload(),
            default_pair.max_stream_frame_payload()
        );
        drop(default_pair);

        let mut config = RingPairConfig::compact();
        config.response_tiers.xlarge.slot_size = 4096;
        config.response_tiers.xlarge.slot_count = 2;

        let pair = Arc::new(RingPair::create(config).expect("create compact ring pair"));
        let transport = Transport::new_shared_memory(Arc::clone(&pair));
        let max_payload = transport_max_stream_frame_payload(&transport);
        assert_eq!(max_payload, pair.max_stream_frame_payload());

        let headers_raw =
            encode_headers_compact(&[(b"content-type".to_vec(), b"text/event-stream".to_vec())]);
        let body: Vec<u8> = (0..(max_payload * 2 + 37))
            .map(|idx| (idx % 251) as u8)
            .collect();

        let captured = Arc::new(Mutex::new(Vec::<(u64, u16, u16, Vec<u8>, Vec<u8>)>::new()));
        let captured_reader = Arc::clone(&captured);
        let pair_reader = Arc::clone(&pair);

        pyo3::Python::attach(|py| {
            let reader = std::thread::spawn(move || {
                loop {
                    pair_reader
                        .drain_frames_with(8, |frame| {
                            captured_reader.lock().expect("lock frames").push((
                                frame.correlation_id,
                                frame.status,
                                frame.flags,
                                frame.headers_raw.to_vec(),
                                frame.body.to_vec(),
                            ));
                        })
                        .expect("drain stream frames");

                    if captured_reader
                        .lock()
                        .expect("lock frames")
                        .last()
                        .is_some_and(|(_, _, flags, _, _)| (*flags & FLAG_FINAL) != 0)
                    {
                        break;
                    }

                    if pair_reader.prepare_response_sleep() {
                        assert!(
                            pair_reader
                                .response_notifier
                                .wait(5_000)
                                .expect("wait for stream frame notification"),
                            "stream frame notification should arrive"
                        );
                        pair_reader.cancel_response_sleep();
                        pair_reader
                            .response_notifier
                            .clear()
                            .expect("clear response notifier");
                    } else {
                        pair_reader.cancel_response_sleep();
                    }
                }
            });

            submit_shm_stream_frames(
                py,
                &transport,
                77,
                200,
                FLAG_STREAMING | FLAG_HEADERS_SENT | FLAG_FINAL,
                &headers_raw,
                &body,
            )
            .expect("submit multi-chunk SHM stream frames");

            py.detach(|| {
                reader.join().expect("reader thread should exit");
            });
        });

        let frames = captured.lock().expect("lock captured frames");
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].0, 77);
        assert_eq!(frames[0].1, 200);
        assert_eq!(frames[0].2, FLAG_STREAMING | FLAG_HEADERS_SENT);
        assert_eq!(frames[0].3, headers_raw);

        assert_eq!(frames[1].2, FLAG_STREAMING);
        assert!(frames[1].3.is_empty());

        assert_eq!(frames[2].2, FLAG_STREAMING | FLAG_FINAL);
        assert!(frames[2].3.is_empty());

        let reassembled: Vec<u8> = frames.iter().flat_map(|f| f.4.iter().copied()).collect();
        assert_eq!(reassembled, body);
    }

    #[test]
    fn submit_stream_frame_validates_flags_and_closed_delivery() {
        pyo3::Python::attach(|py| {
            let err =
                ResponseSubmitter::submit_stream_frame_inner(py, 1, 200, FLAG_FINAL, b"", b"x")
                    .expect_err("missing FLAG_STREAMING should fail");
            assert!(err.is_instance_of::<PyValueError>(py));

            init_transport("inprocess", 4096, 64, 64).expect("init bridge");
            let bridge = BatchBridge::get().expect("bridge initialized");
            bridge.set_stream_sink(Arc::new(|_| StreamDelivery::Closed));

            let closed_err = ResponseSubmitter::submit_stream_frame_inner(
                py,
                9,
                200,
                FLAG_STREAMING | FLAG_HEADERS_SENT,
                b"",
                b"hello",
            )
            .expect_err("closed sink should raise ConnectionResetError");
            assert!(closed_err.is_instance_of::<PyConnectionResetError>(py));

            let noop_cb = py.None();
            set_stream_sink_py(py, Some(noop_cb)).expect("install py sink");
            set_stream_sink_py(py, None).expect("clear py sink");
            let unreg_err = ResponseSubmitter::submit_stream_frame_inner(
                py,
                10,
                200,
                FLAG_STREAMING | FLAG_HEADERS_SENT,
                b"",
                b"after-clear",
            )
            .expect_err("cleared sink should raise RuntimeError");
            assert!(unreg_err.is_instance_of::<PyRuntimeError>(py));

            bridge.set_stream_sink(Arc::new(|_| StreamDelivery::Accepted));
            ResponseSubmitter::submit_stream_frame_inner(
                py,
                11,
                200,
                FLAG_STREAMING | FLAG_HEADERS_SENT | FLAG_FINAL,
                b"",
                b"rust-reinstalled",
            )
            .expect("rust sink works after _set_stream_sink(None)");
            bridge.clear_stream_sink();
        });
    }
}
