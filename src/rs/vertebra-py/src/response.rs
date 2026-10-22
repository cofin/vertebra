//! Response builder for the Vertebra batch runner.

use bytes::Bytes;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyIOError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyString, PyTuple};

#[inline]
fn extract_header_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        Ok(b.as_bytes().to_vec())
    } else if let Ok(s) = obj.cast::<PyString>() {
        Ok(s.to_str()?.as_bytes().to_vec())
    } else {
        obj.extract::<Vec<u8>>()
    }
}

#[inline]
fn extract_header_entry(item: &Bound<'_, PyAny>) -> PyResult<(Vec<u8>, Vec<u8>)> {
    let (k_obj, v_obj) = if let Ok(t) = item.cast::<PyTuple>() {
        (t.get_item(0)?, t.get_item(1)?)
    } else if let Ok(l) = item.cast::<PyList>() {
        (l.get_item(0)?, l.get_item(1)?)
    } else {
        return Err(PyTypeError::new_err(format!(
            "header entry must be a tuple or list, got {}",
            item.get_type().name()?
        )));
    };
    Ok((extract_header_bytes(&k_obj)?, extract_header_bytes(&v_obj)?))
}

pub(crate) fn extract_buffer_bytes(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(py_bytes) = obj.cast::<PyBytes>() {
        return Ok(Bytes::copy_from_slice(py_bytes.as_bytes()));
    }
    if let Ok(py_str) = obj.cast::<PyString>() {
        return Ok(Bytes::copy_from_slice(py_str.to_str()?.as_bytes()));
    }
    if let Ok(py_buf) = PyBuffer::<u8>::get(obj) {
        if py_buf.is_c_contiguous() {
            let len = py_buf.item_count();
            let slice = if len == 0 {
                &[][..]
            } else {
                unsafe { std::slice::from_raw_parts(py_buf.buf_ptr() as *const u8, len) }
            };
            return Ok(Bytes::copy_from_slice(slice));
        }
        return Ok(Bytes::from(py_buf.to_vec(py)?));
    }
    Ok(Bytes::from(obj.extract::<Vec<u8>>()?))
}

pub(crate) fn extract_headers_list(
    headers: Option<Bound<'_, PyAny>>,
) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut header_vec: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    if let Some(h) = headers {
        if let Ok(dict) = h.cast::<PyDict>() {
            header_vec.reserve(dict.len());
            for (k, v) in dict.iter() {
                header_vec.push((extract_header_bytes(&k)?, extract_header_bytes(&v)?));
            }
        } else if let Ok(list) = h.cast::<PyList>() {
            header_vec.reserve(list.len());
            for item in list.iter() {
                header_vec.push(extract_header_entry(&item)?);
            }
        } else if let Ok(tuple) = h.cast::<PyTuple>() {
            header_vec.reserve(tuple.len());
            for item in tuple.iter() {
                header_vec.push(extract_header_entry(&item)?);
            }
        }
    }
    Ok(header_vec)
}

#[pyclass(name = "Response", freelist = 128)]
#[derive(Clone)]
pub struct Response {
    pub status: u16,
    pub body: Bytes,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub request_id: u64,
}

#[pymethods]
impl Response {
    #[new]
    #[pyo3(signature = (status, body=None, headers=None, request_id=0))]
    pub fn new(
        py: Python<'_>,
        status: u16,
        body: Option<Bound<'_, PyAny>>,
        headers: Option<Bound<'_, PyAny>>,
        request_id: u64,
    ) -> PyResult<Self> {
        let body_bytes = match body {
            Some(b) => extract_buffer_bytes(py, &b)?,
            None => Bytes::new(),
        };

        let header_vec = extract_headers_list(headers)?;

        Ok(Self {
            status,
            body: body_bytes,
            headers: header_vec,
            request_id,
        })
    }

    /// Construct a `Response` from standard error status code with interned error text.
    #[staticmethod]
    #[pyo3(signature = (status, request_id=0))]
    pub fn error(status: u16, request_id: u64) -> Self {
        let body = match status {
            400 => Bytes::from_static(b"bad request"),
            401 => Bytes::from_static(b"unauthorized"),
            403 => Bytes::from_static(b"forbidden"),
            404 => Bytes::from_static(b"not found"),
            405 => Bytes::from_static(b"method not allowed"),
            500 => Bytes::from_static(b"internal server error"),
            502 => Bytes::from_static(b"bad gateway"),
            503 => Bytes::from_static(b"service unavailable"),
            504 => Bytes::from_static(b"gateway timeout"),
            _ => Bytes::from(format!("HTTP error {status}")),
        };
        let headers = vec![
            (
                b"content-type".to_vec(),
                b"text/plain; charset=utf-8".to_vec(),
            ),
            (
                b"content-length".to_vec(),
                body.len().to_string().into_bytes(),
            ),
        ];
        Self {
            status,
            body,
            headers,
            request_id,
        }
    }

    /// Construct a `Response` by reading a file or byte range in detached threads.
    #[staticmethod]
    #[pyo3(signature = (path, status=200, headers=None, request_id=0, offset=None, length=None))]
    pub fn from_file(
        py: Python<'_>,
        path: &str,
        status: u16,
        headers: Option<Bound<'_, PyAny>>,
        request_id: u64,
        offset: Option<u64>,
        length: Option<u64>,
    ) -> PyResult<Self> {
        let path_owned = path.to_string();
        let end = offset.and_then(|o| length.map(|l| o + l));
        let body_bytes = py
            .detach(|| crate::file_io::read_file_range_sync(&path_owned, offset, end))
            .map_err(|e| PyIOError::new_err(format!("failed to read file {path}: {e}")))?;

        let mut header_vec = extract_headers_list(headers)?;
        if !header_vec
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case(b"content-length"))
        {
            header_vec.push((
                b"content-length".to_vec(),
                body_bytes.len().to_string().into_bytes(),
            ));
        }

        Ok(Self {
            status,
            body: body_bytes,
            headers: header_vec,
            request_id,
        })
    }

    /// Construct a `Response` directly from any CPython buffer-protocol object (`bytes`, `bytearray`, `memoryview`).
    #[staticmethod]
    #[pyo3(signature = (status, body, headers=None, request_id=0))]
    pub fn from_buffer(
        py: Python<'_>,
        status: u16,
        body: Bound<'_, PyAny>,
        headers: Option<Bound<'_, PyAny>>,
        request_id: u64,
    ) -> PyResult<Self> {
        Self::new(py, status, Some(body), headers, request_id)
    }

    #[getter]
    pub fn status(&self) -> u16 {
        self.status
    }

    #[getter]
    pub fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.body)
    }

    #[getter]
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    #[setter]
    pub fn set_request_id(&mut self, request_id: u64) {
        self.request_id = request_id;
    }

    #[getter]
    pub fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(self.headers.len());
        for (name, value) in &self.headers {
            let t = PyTuple::new(
                py,
                [
                    PyBytes::new(py, name).into_any(),
                    PyBytes::new(py, value).into_any(),
                ],
            )?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    /// Format this response as HTTP/1.1 wire bytes.
    pub fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let mut buf = Vec::with_capacity(64 + self.body.len());
        buf.extend_from_slice(b"HTTP/1.1 ");
        buf.extend_from_slice(self.status.to_string().as_bytes());
        buf.extend_from_slice(b" OK\r\n");
        for (k, v) in &self.headers {
            buf.extend_from_slice(k);
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(v);
            buf.extend_from_slice(b"\r\n");
        }
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(&self.body);
        PyBytes::new(py, &buf)
    }

    fn __repr__(&self) -> String {
        format!(
            "Response(status={}, body_len={}, request_id={})",
            self.status,
            self.body.len(),
            self.request_id,
        )
    }
}
