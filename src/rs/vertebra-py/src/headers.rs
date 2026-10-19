//! Lazy headers proxy for `VertebraRequest`.

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

/// Decode raw compact headers directly into an ASGI-compliant list of `(bytes, bytes)` tuples
/// with lowercased header names, avoiding intermediate `Bytes` or `String` allocations.
pub(crate) fn decode_asgi_headers<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<Bound<'py, PyList>> {
    let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(8);
    let mut pos = 0;
    while pos < data.len() {
        let name_len = data[pos] as usize;
        pos += 1;
        if pos + name_len > data.len() {
            break;
        }
        let name = &data[pos..pos + name_len];
        pos += name_len;

        if pos + 2 > data.len() {
            break;
        }
        let value_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + value_len > data.len() {
            break;
        }
        let value = &data[pos..pos + value_len];
        pos += value_len;

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

/// Iterate raw `(name, value)` byte slices from compact header encoding.
#[inline]
pub(crate) fn iter_raw_headers(data: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
    let mut pos = 0;
    std::iter::from_fn(move || {
        if pos >= data.len() {
            return None;
        }
        let name_len = data[pos] as usize;
        pos += 1;
        if pos + name_len > data.len() {
            return None;
        }
        let name = &data[pos..pos + name_len];
        pos += name_len;

        if pos + 2 > data.len() {
            return None;
        }
        let value_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + value_len > data.len() {
            return None;
        }
        let value = &data[pos..pos + value_len];
        pos += value_len;

        Some((name, value))
    })
}

#[pyclass(name = "VertebraHeaders", frozen, from_py_object)]
#[derive(Clone)]
pub struct VertebraHeaders {
    raw: Bytes,
}

impl VertebraHeaders {
    pub fn new(raw: Bytes) -> Self {
        Self { raw }
    }
}

#[pymethods]
impl VertebraHeaders {
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

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut names: Vec<Bound<'py, PyAny>> = Vec::new();
        for (name, _) in iter_raw_headers(&self.raw) {
            let name_str = String::from_utf8_lossy(name);
            names.push(PyString::new(py, &name_str).into_any());
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
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::new();
        for (name, value) in iter_raw_headers(&self.raw) {
            let name_str = String::from_utf8_lossy(name);
            let val_str = String::from_utf8_lossy(value);
            let t = PyTuple::new(
                py,
                [
                    PyString::new(py, &name_str).into_any(),
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

    fn __iter__(&self) -> VertebraHeadersIter {
        VertebraHeadersIter {
            raw: self.raw.clone(),
            pos: 0,
        }
    }

    fn __repr__(&self) -> String {
        format!("VertebraHeaders(raw_len={})", self.raw.len())
    }
}

#[pyclass]
pub struct VertebraHeadersIter {
    raw: Bytes,
    pos: usize,
}

#[pymethods]
impl VertebraHeadersIter {
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
        let name = String::from_utf8_lossy(&self.raw[self.pos..self.pos + name_len]).into_owned();
        self.pos += name_len;

        if self.pos + 2 > self.raw.len() {
            return None;
        }
        let value_len = u16::from_le_bytes([self.raw[self.pos], self.raw[self.pos + 1]]) as usize;
        self.pos += 2 + value_len;

        Some(name)
    }
}
