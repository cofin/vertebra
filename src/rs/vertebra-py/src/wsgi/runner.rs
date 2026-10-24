//! Native Rust WSGI callable runner (`run_wsgi_callable`) with zero-copy `WsgiFileWrapper` support.

use std::sync::Mutex;

use bytes::Bytes;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::response::Response;
use crate::wsgi::environ::WsgiEnviron;
use crate::wsgi::input::WsgiFileWrapper;

#[derive(Default)]
struct StartResponseState {
    called: bool,
    status: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
}

#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "_WsgiStartResponse", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "_WsgiStartResponse", immutable_type))]
pub struct WsgiStartResponse {
    state: Mutex<StartResponseState>,
}

#[pymethods]
impl WsgiStartResponse {
    #[pyo3(signature = (status, response_headers, exc_info=None))]
    pub fn __call__(
        &self,
        py: Python<'_>,
        status: &str,
        response_headers: Vec<(String, String)>,
        exc_info: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let err_opt = if let Some(ref exc) = exc_info {
            if !exc.is_none() {
                Some(exc.get_item(1)?)
            } else {
                None
            }
        } else {
            None
        };

        let code_str = status.split(' ').next().unwrap_or("");
        let status_code = code_str.parse::<u16>().map_err(|e| {
            PyRuntimeError::new_err(format!("invalid WSGI status line {status:?}: {e}"))
        })?;

        let encoded_headers: Vec<(Vec<u8>, Vec<u8>)> = response_headers
            .into_iter()
            .map(|(k, v)| (k.to_ascii_lowercase().into_bytes(), v.into_bytes()))
            .collect();

        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("start_response lock poisoned"))?;

        if let Some(err_val) = err_opt
            && st.called
        {
            return Err(PyErr::from_value(err_val));
        }

        st.called = true;
        st.status = status_code;
        st.headers = encoded_headers;
        Ok(py.None())
    }
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
    let start_resp = Py::new(
        py,
        WsgiStartResponse {
            state: Mutex::new(StartResponseState::default()),
        },
    )?;
    let result_iter = app.call1((environ, start_resp.clone_ref(py)))?;

    let mut body_buf: Vec<u8> = Vec::new();

    if let Ok(wrapper) = result_iter.cast::<WsgiFileWrapper>() {
        let wrapper_ref = wrapper.borrow();
        if let Some(ref file_path) = wrapper_ref.path() {
            let path_clone = file_path.clone();
            let file_bytes = py.detach(|| std::fs::read(&path_clone)).map_err(|e| {
                PyRuntimeError::new_err(format!("failed to read file {path_clone}: {e}"))
            })?;
            body_buf = file_bytes;
        } else {
            for item in result_iter.try_iter()? {
                let chunk = item?;
                if let Ok(b) = chunk.cast::<PyBytes>() {
                    body_buf.extend_from_slice(b.as_bytes());
                } else {
                    body_buf.extend_from_slice(&chunk.extract::<Vec<u8>>()?);
                }
            }
        }
        let _ = wrapper_ref.close(py);
    } else {
        let iter_res = result_iter.try_iter()?;
        for item in iter_res {
            let chunk = item?;
            if let Ok(b) = chunk.cast::<PyBytes>() {
                body_buf.extend_from_slice(b.as_bytes());
            } else {
                body_buf.extend_from_slice(&chunk.extract::<Vec<u8>>()?);
            }
        }
        if result_iter.hasattr("close")? {
            result_iter.call_method0("close")?;
        }
    }

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

    Ok(Response {
        status: st.status,
        body: Bytes::from(body_buf),
        headers: std::mem::take(&mut st.headers),
        request_id,
    })
}
