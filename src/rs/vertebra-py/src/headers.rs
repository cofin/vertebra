//! Lazy headers proxy for `VertebraRequest`.

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

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
        let key_lower = key.to_ascii_lowercase();
        for (name, value) in self.iter_decoded() {
            if name.to_ascii_lowercase() == key_lower {
                return Ok(PyString::new(py, &value));
            }
        }
        Err(pyo3::exceptions::PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&self, key: &str) -> bool {
        let key_lower = key.to_ascii_lowercase();
        self.iter_decoded()
            .any(|(name, _)| name.to_ascii_lowercase() == key_lower)
    }

    fn __len__(&self) -> usize {
        let mut count = 0;
        let mut pos = 0;
        let data = &self.raw;
        while pos < data.len() {
            let name_len = data[pos] as usize;
            pos += 1 + name_len;
            if pos + 2 > data.len() {
                break;
            }
            let value_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2 + value_len;
            count += 1;
        }
        count
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
        for (name, _) in self.iter_decoded() {
            names.push(PyString::new(py, &name).into_any());
        }
        PyList::new(py, &names)
    }

    fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut vals: Vec<Bound<'py, PyAny>> = Vec::new();
        for (_, value) in self.iter_decoded() {
            vals.push(PyString::new(py, &value).into_any());
        }
        PyList::new(py, &vals)
    }

    fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::new();
        for (name, value) in self.iter_decoded() {
            let t = PyTuple::new(
                py,
                [
                    PyString::new(py, &name).into_any(),
                    PyString::new(py, &value).into_any(),
                ],
            )?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    /// Optimized path for ASGI: returns headers as a list of (bytes, bytes)
    /// where keys are lowercased.
    pub fn asgi_items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::new();
        let data = &self.raw;
        let mut pos = 0;
        while pos < data.len() {
            let name_len = data[pos] as usize;
            pos += 1;
            if pos + name_len > data.len() {
                break;
            }
            let name = &data[pos..pos + name_len];
            // Lowercase the key for ASGI spec compliance
            let name_lower = name.to_ascii_lowercase();
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

            let t = PyTuple::new(
                py,
                [
                    PyBytes::new(py, &name_lower).into_any(),
                    PyBytes::new(py, value).into_any(),
                ],
            )?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        for (name, value) in self.iter_decoded() {
            d.set_item(name, value)?;
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

impl VertebraHeaders {
    fn iter_decoded(&self) -> impl Iterator<Item = (String, String)> + '_ {
        let mut pos = 0;
        let data = &self.raw;
        std::iter::from_fn(move || {
            if pos >= data.len() {
                return None;
            }
            let name_len = data[pos] as usize;
            pos += 1;
            if pos + name_len > data.len() {
                return None;
            }
            let name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
            pos += name_len;

            if pos + 2 > data.len() {
                return None;
            }
            let value_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            if pos + value_len > data.len() {
                return None;
            }
            let value = String::from_utf8_lossy(&data[pos..pos + value_len]).to_string();
            pos += value_len;

            Some((name, value))
        })
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
        let name = String::from_utf8_lossy(&self.raw[self.pos..self.pos + name_len]).to_string();
        self.pos += name_len;

        if self.pos + 2 > self.raw.len() {
            return None;
        }
        let value_len = u16::from_le_bytes([self.raw[self.pos], self.raw[self.pos + 1]]) as usize;
        self.pos += 2 + value_len;

        Some(name)
    }
}
