//! Lazy headers proxy for `VertebraRequest`.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

#[pyclass(name = "VertebraHeaders", frozen, from_py_object)]
#[derive(Clone)]
pub struct VertebraHeaders {
    pairs: Vec<(String, String)>,
}

impl VertebraHeaders {
    pub fn new(pairs: Vec<(String, String)>) -> Self {
        Self { pairs }
    }
}

#[pymethods]
impl VertebraHeaders {
    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyString>> {
        let key_lower = key.to_ascii_lowercase();
        for (name, value) in &self.pairs {
            if name.to_ascii_lowercase() == key_lower {
                return Ok(PyString::new(py, value));
            }
        }
        Err(pyo3::exceptions::PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&self, key: &str) -> bool {
        let key_lower = key.to_ascii_lowercase();
        self.pairs
            .iter()
            .any(|(name, _)| name.to_ascii_lowercase() == key_lower)
    }

    fn __len__(&self) -> usize {
        self.pairs.len()
    }

    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        match self.__getitem__(py, key) {
            Ok(v) => v.unbind().into(),
            Err(_) => default.unwrap_or_else(|| py.None()),
        }
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let names: Vec<Bound<'py, PyAny>> = self
            .pairs
            .iter()
            .map(|(name, _)| PyString::new(py, name).into_any())
            .collect();
        PyList::new(py, &names)
    }

    fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let vals: Vec<Bound<'py, PyAny>> = self
            .pairs
            .iter()
            .map(|(_, value)| PyString::new(py, value).into_any())
            .collect();
        PyList::new(py, &vals)
    }

    fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(self.pairs.len());
        for (name, value) in &self.pairs {
            let t = PyTuple::new(
                py,
                [
                    PyString::new(py, name).into_any(),
                    PyString::new(py, value).into_any(),
                ],
            )?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        for (name, value) in &self.pairs {
            d.set_item(name, value)?;
        }
        Ok(d)
    }

    fn __iter__(&self) -> VertebraHeadersIter {
        VertebraHeadersIter {
            names: self.pairs.iter().map(|(n, _)| n.clone()).collect(),
            index: 0,
        }
    }

    fn __repr__(&self) -> String {
        format!("VertebraHeaders(count={})", self.pairs.len())
    }
}

#[pyclass]
pub struct VertebraHeadersIter {
    names: Vec<String>,
    index: usize,
}

#[pymethods]
impl VertebraHeadersIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<String> {
        if self.index < self.names.len() {
            let name = self.names[self.index].clone();
            self.index += 1;
            Some(name)
        } else {
            None
        }
    }
}
