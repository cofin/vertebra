//! Lazy headers proxy for `Request`.

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

pub(crate) use vertebra_core::transport::{encode_headers_compact, iter_raw_headers};

#[inline]
fn intern_or_alloc_header_key<'py>(py: Python<'py>, name: &[u8]) -> Bound<'py, PyString> {
    if name.eq_ignore_ascii_case(b"content-type") {
        return pyo3::intern!(py, "content-type").clone();
    }
    if name.eq_ignore_ascii_case(b"content-length") {
        return pyo3::intern!(py, "content-length").clone();
    }
    if name.eq_ignore_ascii_case(b"authorization") {
        return pyo3::intern!(py, "authorization").clone();
    }
    if name.eq_ignore_ascii_case(b"accept") {
        return pyo3::intern!(py, "accept").clone();
    }
    if name.eq_ignore_ascii_case(b"host") {
        return pyo3::intern!(py, "host").clone();
    }
    if name.eq_ignore_ascii_case(b"user-agent") {
        return pyo3::intern!(py, "user-agent").clone();
    }
    if name.eq_ignore_ascii_case(b"cookie") {
        return pyo3::intern!(py, "cookie").clone();
    }
    if name.eq_ignore_ascii_case(b"upgrade") {
        return pyo3::intern!(py, "upgrade").clone();
    }
    if name.eq_ignore_ascii_case(b"connection") {
        return pyo3::intern!(py, "connection").clone();
    }
    let name_str = String::from_utf8_lossy(name).to_ascii_lowercase();
    PyString::new(py, &name_str)
}

/// Decode raw compact headers directly into an ASGI-compliant list of `(bytes, bytes)` tuples
/// with lowercased header names, avoiding intermediate `Bytes` or `String` allocations.
pub(crate) fn decode_asgi_headers<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<Bound<'py, PyList>> {
    let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(8);
    for (name, value) in iter_raw_headers(data) {
        let name_py = if name.iter().any(|b| b.is_ascii_uppercase()) {
            let lower = name.to_ascii_lowercase();
            PyBytes::new(py, &lower)
        } else {
            PyBytes::new(py, name)
        };

        let t = PyTuple::new(py, [name_py.into_any(), PyBytes::new(py, value).into_any()])?;
        tuples.push(t.into_any());
    }
    PyList::new(py, &tuples)
}

#[pyclass(name = "Headers", frozen, freelist = 128, from_py_object)]
#[derive(Clone)]
pub struct Headers {
    pub(crate) raw: Bytes,
}

impl Headers {
    pub fn new(raw: Bytes) -> Self {
        Self { raw }
    }

    pub fn raw_bytes(&self) -> &Bytes {
        &self.raw
    }
}

#[pymethods]
impl Headers {
    /// Construct a `Headers` instance from `(name, value)` string tuples using compact binary encoding.
    #[staticmethod]
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Self {
        let raw_pairs: Vec<(Vec<u8>, Vec<u8>)> = pairs
            .into_iter()
            .map(|(k, v)| (k.into_bytes(), v.into_bytes()))
            .collect();
        Self {
            raw: Bytes::from(encode_headers_compact(&raw_pairs)),
        }
    }

    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyString>> {
        let key_bytes = key.as_bytes();
        for (name, value) in iter_raw_headers(&self.raw) {
            if name.eq_ignore_ascii_case(key_bytes) {
                let val_str = String::from_utf8_lossy(value);
                return Ok(PyString::new(py, &val_str));
            }
        }
        Err(pyo3::exceptions::PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&self, key: &str) -> bool {
        let key_bytes = key.as_bytes();
        iter_raw_headers(&self.raw).any(|(name, _)| name.eq_ignore_ascii_case(key_bytes))
    }

    fn __len__(&self) -> usize {
        iter_raw_headers(&self.raw).count()
    }

    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        match self.__getitem__(py, key) {
            Ok(v) => v.unbind().into(),
            Err(_) => default.unwrap_or_else(|| py.None()),
        }
    }

    /// Return all values matching `key` as a list of strings.
    pub fn get_all(&self, key: &str) -> Vec<String> {
        let key_bytes = key.as_bytes();
        let mut out = Vec::new();
        for (name, value) in iter_raw_headers(&self.raw) {
            if name.eq_ignore_ascii_case(key_bytes) {
                out.push(String::from_utf8_lossy(value).into_owned());
            }
        }
        out
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut names: Vec<Bound<'py, PyAny>> = Vec::new();
        for (name, _) in iter_raw_headers(&self.raw) {
            names.push(intern_or_alloc_header_key(py, name).into_any());
        }
        PyList::new(py, &names)
    }

    fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut vals: Vec<Bound<'py, PyAny>> = Vec::new();
        for (_, value) in iter_raw_headers(&self.raw) {
            let val_str = String::from_utf8_lossy(value);
            vals.push(PyString::new(py, &val_str).into_any());
        }
        PyList::new(py, &vals)
    }

    fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(8);
        for (name, value) in iter_raw_headers(&self.raw) {
            let val_str = String::from_utf8_lossy(value);
            let t = PyTuple::new(
                py,
                [
                    intern_or_alloc_header_key(py, name).into_any(),
                    PyString::new(py, &val_str).into_any(),
                ],
            )?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    /// Optimized path for ASGI: returns headers as a list of (bytes, bytes)
    /// where keys are lowercased.
    pub fn asgi_items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        decode_asgi_headers(py, &self.raw)
    }

    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        for (name, value) in iter_raw_headers(&self.raw) {
            let name_str = String::from_utf8_lossy(name);
            let val_str = String::from_utf8_lossy(value);
            d.set_item(name_str.as_ref(), val_str.as_ref())?;
        }
        Ok(d)
    }

    fn __iter__(&self) -> HeadersIterator {
        HeadersIterator {
            raw: self.raw.clone(),
            pos: 0,
        }
    }

    fn __repr__(&self) -> String {
        format!("Headers(raw_len={})", self.raw.len())
    }
}

#[pyclass(name = "HeadersIterator")]
pub struct HeadersIterator {
    raw: Bytes,
    pos: usize,
}

#[pymethods]
impl HeadersIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<String> {
        if self.pos >= self.raw.len() {
            return None;
        }
        let name_len = self.raw[self.pos] as usize;
        self.pos += 1;
        if self.pos + name_len > self.raw.len() {
            return None;
        }
        let name =
            String::from_utf8_lossy(&self.raw[self.pos..self.pos + name_len]).to_ascii_lowercase();
        self.pos += name_len;

        if self.pos + 2 > self.raw.len() {
            return None;
        }
        let value_len = u16::from_le_bytes([self.raw[self.pos], self.raw[self.pos + 1]]) as usize;
        self.pos += 2 + value_len;

        Some(name)
    }
}
