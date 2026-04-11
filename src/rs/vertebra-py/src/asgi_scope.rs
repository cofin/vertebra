//! Lazy ASGI scope backed by a `BatchSnapshot` + index.
//!
//! Replaces the `_SCOPE_TEMPLATE.copy()` dict path in
//! `server/_asgi/_scope.py` with a `#[pyclass(mapping)]` that materializes
//! known keys on demand. Keeps the `VertebraRequest` arena alive via an
//! `Arc<BatchSnapshot>` so path/query/headers slices stay valid for as long
//! as middleware holds the scope.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use vertebra_core::transport::BatchSnapshot;

use crate::headers::VertebraHeaders;

/// Known ASGI HTTP scope keys per the ASGI 3.0 spec.
const KNOWN_KEYS: &[&str] = &[
    "type",
    "asgi",
    "http_version",
    "method",
    "path",
    "raw_path",
    "query_string",
    "root_path",
    "scheme",
    "headers",
    "state",
    "extensions",
    "server",
    "client",
];

/// Lazy ASGI scope. Implements the mapping protocol so framework middleware
/// that expects a dict can read it without the scope having to materialize
/// every key up-front.
#[pyclass(mapping, name = "AsgiScope")]
pub struct AsgiScope {
    batch: Arc<BatchSnapshot>,
    index: usize,
    root_path: String,
    /// Cached materialized values for known keys.
    cached: HashMap<String, Py<PyAny>>,
    /// User-set keys (via `__setitem__` or `update`).
    extras: HashMap<String, Py<PyAny>>,
    /// Keys that have been removed via `__delitem__`.
    deleted: HashSet<String>,
}

impl AsgiScope {
    /// Build a fresh scope. The caller owns the `Arc<BatchSnapshot>` slot.
    pub fn new(batch: Arc<BatchSnapshot>, index: usize, root_path: String) -> Self {
        Self {
            batch,
            index,
            root_path,
            cached: HashMap::new(),
            extras: HashMap::new(),
            deleted: HashSet::new(),
        }
    }

    fn build_known(&self, py: Python<'_>, key: &str) -> PyResult<Option<Py<PyAny>>> {
        let node = self.batch.node(self.index);
        let obj: Py<PyAny> = match key {
            "type" => PyString::intern(py, "http").into_any().unbind(),
            "asgi" => {
                let d = PyDict::new(py);
                d.set_item("version", "3.0")?;
                d.set_item("spec_version", "2.3")?;
                d.into_any().unbind()
            }
            "http_version" => PyString::intern(py, node.http_version()).into_any().unbind(),
            "method" => {
                let method = node.method_name().to_ascii_uppercase();
                PyString::new(py, &method).into_any().unbind()
            }
            "path" => PyString::new(py, self.batch.path(self.index))
                .into_any()
                .unbind(),
            "raw_path" => PyBytes::new(py, self.batch.path(self.index).as_bytes())
                .into_any()
                .unbind(),
            "query_string" => {
                let q = self.batch.query(self.index).unwrap_or("");
                PyBytes::new(py, q.as_bytes()).into_any().unbind()
            }
            "root_path" => PyString::new(py, &self.root_path).into_any().unbind(),
            "scheme" => PyString::intern(py, "http").into_any().unbind(),
            "headers" => {
                let raw = self.batch.headers_raw(self.index);
                let hdrs = VertebraHeaders::new(bytes::Bytes::copy_from_slice(raw));
                hdrs.asgi_items(py)?.into_any().unbind()
            }
            "state" => PyDict::new(py).into_any().unbind(),
            "extensions" => PyDict::new(py).into_any().unbind(),
            "server" | "client" => py.None(),
            _ => return Ok(None),
        };
        Ok(Some(obj))
    }

    fn collect_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = Vec::with_capacity(KNOWN_KEYS.len() + self.extras.len());
        for k in KNOWN_KEYS {
            if !self.deleted.contains(*k) {
                keys.push((*k).to_string());
            }
        }
        for k in self.extras.keys() {
            if !KNOWN_KEYS.contains(&k.as_str()) && !self.deleted.contains(k) {
                keys.push(k.clone());
            }
        }
        keys
    }
}

#[pymethods]
impl AsgiScope {
    fn __getitem__(&mut self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        if self.deleted.contains(key) {
            return Err(PyKeyError::new_err(key.to_string()));
        }
        if let Some(v) = self.extras.get(key) {
            return Ok(v.clone_ref(py));
        }
        if let Some(v) = self.cached.get(key) {
            return Ok(v.clone_ref(py));
        }
        if let Some(v) = self.build_known(py, key)? {
            let clone = v.clone_ref(py);
            self.cached.insert(key.to_string(), v);
            return Ok(clone);
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __setitem__(&mut self, key: String, value: Py<PyAny>) {
        self.deleted.remove(&key);
        self.extras.insert(key, value);
    }

    fn __delitem__(&mut self, key: &str) -> PyResult<()> {
        let in_extras = self.extras.remove(key).is_some();
        let is_known = KNOWN_KEYS.contains(&key);
        if in_extras || is_known {
            self.cached.remove(key);
            self.deleted.insert(key.to_string());
            return Ok(());
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&self, key: &str) -> bool {
        if self.deleted.contains(key) {
            return false;
        }
        if self.extras.contains_key(key) {
            return true;
        }
        KNOWN_KEYS.contains(&key)
    }

    fn __len__(&self) -> usize {
        let mut n = 0usize;
        for k in KNOWN_KEYS {
            if !self.deleted.contains(*k) {
                n += 1;
            }
        }
        for k in self.extras.keys() {
            if !KNOWN_KEYS.contains(&k.as_str()) && !self.deleted.contains(k) {
                n += 1;
            }
        }
        n
    }

    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<AsgiScopeKeyIter>> {
        let keys = slf.collect_keys();
        Py::new(py, AsgiScopeKeyIter { keys, pos: 0 })
    }

    #[pyo3(signature = (key, default=None))]
    fn get(
        &mut self,
        py: Python<'_>,
        key: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        match self.__getitem__(py, key) {
            Ok(v) => Ok(v),
            Err(_) => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    #[pyo3(signature = (key, default=None))]
    fn setdefault(
        &mut self,
        py: Python<'_>,
        key: String,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        match self.__getitem__(py, &key) {
            Ok(v) => Ok(v),
            Err(_) => {
                let val = default.unwrap_or_else(|| py.None());
                let clone = val.clone_ref(py);
                self.deleted.remove(&key);
                self.extras.insert(key, val);
                Ok(clone)
            }
        }
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let keys = self.collect_keys();
        let items: Vec<Bound<'py, PyAny>> = keys
            .into_iter()
            .map(|k| PyString::new(py, &k).into_any())
            .collect();
        PyList::new(py, &items)
    }

    fn values<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let keys = self.collect_keys();
        let mut items: Vec<Bound<'py, PyAny>> = Vec::with_capacity(keys.len());
        for k in &keys {
            let v = self.__getitem__(py, k)?;
            items.push(v.into_bound(py));
        }
        PyList::new(py, &items)
    }

    fn items<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let keys = self.collect_keys();
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(keys.len());
        for k in &keys {
            let v = self.__getitem__(py, k)?;
            let t = PyTuple::new(
                py,
                [PyString::new(py, k).into_any(), v.into_bound(py)],
            )?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    fn update(&mut self, other: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(dict) = other.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                self.__setitem__(key, v.unbind());
            }
            return Ok(());
        }
        // Fall back to .items() for dict-like mappings.
        let items = other.call_method0("items")?;
        let iter = items.try_iter()?;
        for item in iter {
            let item = item?;
            let t = item.cast::<PyTuple>()?;
            let key: String = t.get_item(0)?.extract()?;
            let val: Py<PyAny> = t.get_item(1)?.unbind();
            self.__setitem__(key, val);
        }
        Ok(())
    }

    fn copy<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        let keys = self.collect_keys();
        for k in &keys {
            let v = self.__getitem__(py, k)?;
            d.set_item(k, v)?;
        }
        Ok(d)
    }

    fn __eq__(&mut self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if !other.hasattr("keys")? {
            return Ok(false);
        }
        let my_keys = self.collect_keys();
        let keys_call = other.call_method0("keys")?;
        let iter = keys_call.try_iter()?;
        let mut other_count = 0usize;
        for k in iter {
            let k = k?;
            let ks: String = k.extract()?;
            if !my_keys.contains(&ks) {
                return Ok(false);
            }
            let other_val = other.get_item(&ks)?;
            let my_val = self.__getitem__(py, &ks)?;
            if !my_val.into_bound(py).eq(&other_val)? {
                return Ok(false);
            }
            other_count += 1;
        }
        Ok(other_count == my_keys.len())
    }

    fn __repr__(&self) -> String {
        format!("AsgiScope({} keys)", self.collect_keys().len())
    }
}

/// Iterator over the scope's live keys.
#[pyclass]
pub struct AsgiScopeKeyIter {
    keys: Vec<String>,
    pos: usize,
}

#[pymethods]
impl AsgiScopeKeyIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self) -> Option<String> {
        if self.pos < self.keys.len() {
            let k = self.keys[self.pos].clone();
            self.pos += 1;
            Some(k)
        } else {
            None
        }
    }
}
