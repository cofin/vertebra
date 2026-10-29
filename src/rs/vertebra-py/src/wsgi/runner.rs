//! Native Rust WSGI callable and batch runners (`run_wsgi_callable`, `drive_wsgi_batch`).

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTraceback, PyTuple};

use vertebra_core::transport::decode_headers_raw;

use crate::asgi::{
    ERROR_500_BODY, ERROR_500_HEADERS_RAW, FlushedProtocolResponse, PreparedRawBody,
    collect_batch_targets, encode_py_headers_into, log_app_exception, push_compact_header,
};
use crate::response::{Response, extract_buffer_bytes};
use crate::transport::ResponseSubmitter;
use crate::wsgi::environ::WsgiEnviron;
use crate::wsgi::input::WsgiFileWrapper;

#[derive(Default)]
struct StartResponseState {
    called: bool,
    headers_sent: bool,
    has_content_length: bool,
    status: u16,
    headers_raw: Vec<u8>,
    write_buf: Vec<u8>,
}

#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "_WsgiStartResponse", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "_WsgiStartResponse", immutable_type))]
#[derive(Default)]
pub struct WsgiStartResponse {
    state: Mutex<StartResponseState>,
}

#[pymethods]
impl WsgiStartResponse {
    #[pyo3(signature = (arg1, response_headers=None, exc_info=None))]
    pub fn __call__(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        arg1: &Bound<'_, PyAny>,
        response_headers: Option<&Bound<'_, PyAny>>,
        exc_info: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let Some(headers_obj) = response_headers else {
            slf.borrow().write(py, arg1)?;
            return Ok(py.None());
        };

        let has_exc = exc_info.as_ref().is_some_and(|e| !e.is_none());
        if has_exc {
            let headers_already_sent = slf
                .borrow()
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?
                .headers_sent;
            if headers_already_sent {
                let exc = exc_info.unwrap();
                let err_val = exc.get_item(1)?;
                if !err_val.is_none() {
                    let err = PyErr::from_value(err_val);
                    if let Ok(tb) = exc.get_item(2)
                        && !tb.is_none()
                        && let Ok(tb_bound) = tb.cast::<PyTraceback>()
                    {
                        err.set_traceback(py, Some(tb_bound.clone()));
                    }
                    return Err(err);
                }
            }
        } else {
            let already_called = slf
                .borrow()
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?
                .called;
            if already_called {
                return Err(PyRuntimeError::new_err(
                    "start_response called a second time without exc_info",
                ));
            }
        }

        let status: &str = arg1.extract()?;
        let code_str = status.split(' ').next().unwrap_or("");
        let status_code = code_str.parse::<u16>().map_err(|e| {
            PyRuntimeError::new_err(format!("invalid WSGI status line {status:?}: {e}"))
        })?;
        if !(100..=999).contains(&status_code) {
            return Err(PyRuntimeError::new_err(format!(
                "WSGI status code out of range: {status_code}"
            )));
        }

        let mut headers_raw = Vec::new();
        let mut has_content_length = false;
        encode_py_headers_into(py, headers_obj, &mut headers_raw, &mut has_content_length)?;

        let this = slf.borrow();
        let mut st = this
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
        st.called = true;
        st.status = status_code;
        st.headers_raw = headers_raw;
        st.has_content_length = has_content_length;
        drop(st);
        drop(this);

        Ok(slf.into_any().unbind())
    }

    pub fn write(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(b) = data.cast::<PyBytes>() {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
            if !st.called {
                return Err(PyRuntimeError::new_err(
                    "WSGI write() called before start_response",
                ));
            }
            st.headers_sent = true;
            let slice = b.as_bytes();
            if !slice.is_empty() {
                st.write_buf.extend_from_slice(slice);
            }
            return Ok(());
        }
        let extracted = extract_buffer_bytes(py, data)?;
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
        if !st.called {
            return Err(PyRuntimeError::new_err(
                "WSGI write() called before start_response",
            ));
        }
        st.headers_sent = true;
        if !extracted.is_empty() {
            st.write_buf.extend_from_slice(&extracted);
        }
        Ok(())
    }
}

#[inline]
fn append_chunk_bytes(py: Python<'_>, chunk: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    if let Ok(b) = chunk.cast::<PyBytes>() {
        let slice = b.as_bytes();
        if !slice.is_empty() {
            out.extend_from_slice(slice);
        }
        return Ok(());
    }
    let extracted = extract_buffer_bytes(py, chunk)?;
    if !extracted.is_empty() {
        out.extend_from_slice(&extracted);
    }
    Ok(())
}

fn execute_wsgi_into_raw(
    py: Python<'_>,
    app: &Bound<'_, PyAny>,
    environ: Py<WsgiEnviron>,
    start_resp: &Py<WsgiStartResponse>,
    request_id: u64,
) -> PyResult<FlushedProtocolResponse> {
    let result_iter = app.call1((environ, start_resp.clone_ref(py)))?;

    if let Ok(list) = result_iter.cast::<PyList>() {
        if list.len() == 1 {
            let first = list.get_item(0)?;
            if let Ok(py_b) = first.cast::<PyBytes>() {
                let start_ref = start_resp.borrow(py);
                let mut st = start_ref
                    .state
                    .lock()
                    .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
                if !st.called {
                    return Err(PyRuntimeError::new_err(
                        "WSGI application returned without calling start_response",
                    ));
                }
                st.headers_sent = true;
                if st.write_buf.is_empty() {
                    return Ok(FlushedProtocolResponse {
                        request_id,
                        status: st.status,
                        headers_raw: std::mem::take(&mut st.headers_raw),
                        body: PreparedRawBody::PyBytes(py_b.clone().unbind()),
                    });
                }
                let mut body_buf = std::mem::take(&mut st.write_buf);
                body_buf.extend_from_slice(py_b.as_bytes());
                return Ok(FlushedProtocolResponse {
                    request_id,
                    status: st.status,
                    headers_raw: std::mem::take(&mut st.headers_raw),
                    body: PreparedRawBody::Owned(body_buf),
                });
            }
        }
        let mut body_buf = {
            let start_ref = start_resp.borrow(py);
            let mut st = start_ref
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
            if !st.called {
                return Err(PyRuntimeError::new_err(
                    "WSGI application returned without calling start_response",
                ));
            }
            st.headers_sent = true;
            std::mem::take(&mut st.write_buf)
        };
        for item in list.iter() {
            append_chunk_bytes(py, &item, &mut body_buf)?;
        }
        let start_ref = start_resp.borrow(py);
        let mut st = start_ref
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
        return Ok(FlushedProtocolResponse {
            request_id,
            status: st.status,
            headers_raw: std::mem::take(&mut st.headers_raw),
            body: if body_buf.is_empty() {
                PreparedRawBody::Empty
            } else {
                PreparedRawBody::Owned(body_buf)
            },
        });
    }

    if let Ok(tup) = result_iter.cast::<PyTuple>() {
        if tup.len() == 1 {
            let first = tup.get_item(0)?;
            if let Ok(py_b) = first.cast::<PyBytes>() {
                let start_ref = start_resp.borrow(py);
                let mut st = start_ref
                    .state
                    .lock()
                    .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
                if !st.called {
                    return Err(PyRuntimeError::new_err(
                        "WSGI application returned without calling start_response",
                    ));
                }
                st.headers_sent = true;
                if st.write_buf.is_empty() {
                    return Ok(FlushedProtocolResponse {
                        request_id,
                        status: st.status,
                        headers_raw: std::mem::take(&mut st.headers_raw),
                        body: PreparedRawBody::PyBytes(py_b.clone().unbind()),
                    });
                }
                let mut body_buf = std::mem::take(&mut st.write_buf);
                body_buf.extend_from_slice(py_b.as_bytes());
                return Ok(FlushedProtocolResponse {
                    request_id,
                    status: st.status,
                    headers_raw: std::mem::take(&mut st.headers_raw),
                    body: PreparedRawBody::Owned(body_buf),
                });
            }
        }
        let mut body_buf = {
            let start_ref = start_resp.borrow(py);
            let mut st = start_ref
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
            if !st.called {
                return Err(PyRuntimeError::new_err(
                    "WSGI application returned without calling start_response",
                ));
            }
            st.headers_sent = true;
            std::mem::take(&mut st.write_buf)
        };
        for item in tup.iter() {
            append_chunk_bytes(py, &item, &mut body_buf)?;
        }
        let start_ref = start_resp.borrow(py);
        let mut st = start_ref
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
        return Ok(FlushedProtocolResponse {
            request_id,
            status: st.status,
            headers_raw: std::mem::take(&mut st.headers_raw),
            body: if body_buf.is_empty() {
                PreparedRawBody::Empty
            } else {
                PreparedRawBody::Owned(body_buf)
            },
        });
    }

    if let Ok(wrapper) = result_iter.cast::<WsgiFileWrapper>() {
        let wrapper_ref = wrapper.borrow();
        let is_file_path = wrapper_ref.path().is_some();
        let mut body_buf: Vec<u8> = if let Some(ref file_path) = wrapper_ref.path() {
            let path_clone = file_path.clone();
            py.detach(|| std::fs::read(&path_clone)).map_err(|e| {
                PyRuntimeError::new_err(format!("failed to read file {path_clone}: {e}"))
            })?
        } else {
            let mut chunks = Vec::new();
            for item in result_iter.try_iter()? {
                let chunk = item?;
                append_chunk_bytes(py, &chunk, &mut chunks)?;
            }
            chunks
        };
        let _ = wrapper_ref.close(py);
        let start_ref = start_resp.borrow(py);
        let mut st = start_ref
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
        if !st.called {
            return Err(PyRuntimeError::new_err(
                "WSGI application returned without calling start_response",
            ));
        }
        if !st.write_buf.is_empty() {
            let mut combined = std::mem::take(&mut st.write_buf);
            combined.extend_from_slice(&body_buf);
            body_buf = combined;
        }
        if is_file_path && !st.has_content_length {
            let len_str = body_buf.len().to_string();
            push_compact_header(&mut st.headers_raw, b"content-length", len_str.as_bytes());
            st.has_content_length = true;
        }
        return Ok(FlushedProtocolResponse {
            request_id,
            status: st.status,
            headers_raw: std::mem::take(&mut st.headers_raw),
            body: PreparedRawBody::Owned(body_buf),
        });
    }

    let mut body_buf: Vec<u8> = Vec::new();
    let iter_res: PyResult<()> = (|| {
        let py_iter = result_iter.try_iter()?;
        for item in py_iter {
            let chunk = item?;
            {
                let start_ref = start_resp.borrow(py);
                let mut st = start_ref
                    .state
                    .lock()
                    .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
                if !st.called {
                    return Err(PyRuntimeError::new_err(
                        "WSGI application yielded body bytes before calling start_response",
                    ));
                }
                st.headers_sent = true;
                if !st.write_buf.is_empty() {
                    body_buf.extend_from_slice(&st.write_buf);
                    st.write_buf.clear();
                }
            }
            append_chunk_bytes(py, &chunk, &mut body_buf)?;
        }
        Ok(())
    })();

    if result_iter.hasattr(pyo3::intern!(py, "close"))? {
        let _ = result_iter.call_method0(pyo3::intern!(py, "close"));
    }
    iter_res?;

    let start_ref = start_resp.borrow(py);
    let mut st = start_ref
        .state
        .lock()
        .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;
    if !st.called {
        return Err(PyRuntimeError::new_err(
            "WSGI application returned without calling start_response",
        ));
    }
    if !st.write_buf.is_empty() {
        let mut combined = std::mem::take(&mut st.write_buf);
        combined.extend_from_slice(&body_buf);
        body_buf = combined;
    }

    Ok(FlushedProtocolResponse {
        request_id,
        status: st.status,
        headers_raw: std::mem::take(&mut st.headers_raw),
        body: if body_buf.is_empty() {
            PreparedRawBody::Empty
        } else {
            PreparedRawBody::Owned(body_buf)
        },
    })
}

/// Invoke a PEP 3333 WSGI application callable in Rust and construct a native `Response`.
#[pyfunction]
#[pyo3(name = "run_wsgi_callable", signature = (app, environ, request_id=0))]
pub fn run_wsgi_callable(
    py: Python<'_>,
    app: &Bound<'_, PyAny>,
    environ: Py<WsgiEnviron>,
    request_id: u64,
) -> PyResult<Response> {
    let start_resp = Py::new(py, WsgiStartResponse::default())?;
    let flushed = execute_wsgi_into_raw(py, app, environ, &start_resp, request_id)?;
    let body = match flushed.body {
        PreparedRawBody::Empty => Bytes::new(),
        PreparedRawBody::PyBytes(py_b) => Bytes::copy_from_slice(py_b.bind(py).as_bytes()),
        PreparedRawBody::Owned(vec) => Bytes::from(vec),
    };
    let headers = decode_headers_raw(&flushed.headers_raw);
    Ok(Response {
        status: flushed.status,
        body,
        headers,
        request_id: flushed.request_id,
    })
}

/// Drive a batch of PEP 3333 WSGI requests in Rust and submit all responses in one batch call.
#[pyfunction]
#[pyo3(
    name = "drive_wsgi_batch",
    signature = (app, batch, *, root_path="", server_name="localhost", server_port=8000, url_scheme="http", indices=None)
)]
#[allow(clippy::too_many_arguments)]
pub fn drive_wsgi_batch(
    py: Python<'_>,
    app: &Bound<'_, PyAny>,
    batch: &Bound<'_, PyAny>,
    root_path: &str,
    server_name: &str,
    server_port: u16,
    url_scheme: &str,
    indices: Option<Vec<usize>>,
) -> PyResult<usize> {
    let targets = collect_batch_targets(batch, indices)?;
    if targets.is_empty() {
        return Ok(0);
    }

    let mut completed: Vec<FlushedProtocolResponse> = Vec::with_capacity(targets.len());
    targets.for_each(|snap, idx| {
        let request_id = snap.node(idx).correlation_id;
        let environ = Py::new(
            py,
            WsgiEnviron::with_root_path(
                Arc::clone(snap),
                idx,
                root_path.to_string(),
                server_name.to_string(),
                server_port,
                url_scheme.to_string(),
            ),
        )?;
        let start_resp = Py::new(py, WsgiStartResponse::default())?;
        match execute_wsgi_into_raw(py, app, environ, &start_resp, request_id) {
            Ok(flushed) => completed.push(flushed),
            Err(err) => {
                log_app_exception(py, "WSGI", &err);
                completed.push(FlushedProtocolResponse {
                    request_id,
                    status: 500,
                    headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
                    body: PreparedRawBody::Owned(ERROR_500_BODY.to_vec()),
                });
            }
        }
        Ok(())
    })?;

    if !completed.is_empty() {
        let mut raw_refs: Vec<(u64, u16, &[u8], &[u8])> = Vec::with_capacity(completed.len());
        for item in &completed {
            raw_refs.push((
                item.request_id,
                item.status,
                item.headers_raw.as_slice(),
                item.body.as_slice(py),
            ));
        }
        ResponseSubmitter::submit_batch_raw_inner(py, &raw_refs)?;
    }

    Ok(completed.len())
}
