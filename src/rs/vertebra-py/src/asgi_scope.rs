//! Lazy ASGI scope backed by a `BatchSnapshot` + index.
//!
//! Replaces the `_SCOPE_TEMPLATE.copy()` dict path in
//! `server/_asgi/_scope.py` with a `#[pyclass(mapping)]` that materializes
//! known keys on demand using a fixed-size inline slot table and bitmask.
//! Keeps the `VertebraRequest` arena alive via an `Arc<BatchSnapshot>` so
//! path/query/headers slices stay valid for as long as middleware holds the scope.

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use vertebra_core::transport::BatchSnapshot;

use crate::headers::decode_asgi_headers;

/// Known ASGI HTTP scope keys per the ASGI 3.0 spec.
const KNOWN_KEYS: [&str; 14] = [
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

#[inline]
fn known_key_index(key: &str) -> Option<usize> {
    match key {
        "type" => Some(0),
        "asgi" => Some(1),
        "http_version" => Some(2),
        "method" => Some(3),
        "path" => Some(4),
        "raw_path" => Some(5),
        "query_string" => Some(6),
        "root_path" => Some(7),
        "scheme" => Some(8),
        "headers" => Some(9),
        "state" => Some(10),
        "extensions" => Some(11),
        "server" => Some(12),
        "client" => Some(13),
        _ => None,
    }
}

/// Lazy ASGI scope. Implements the mapping protocol so framework middleware
/// that expects a dict can read it without the scope having to materialize
/// every key up-front.
#[pyclass(mapping, name = "AsgiScope")]
pub struct AsgiScope {
    batch: Arc<BatchSnapshot>,
    index: usize,
    root_path: String,
    /// Inline cache for the 14 standard ASGI HTTP scope keys.
    cached: [Option<Py<PyAny>>; KNOWN_KEYS.len()],
    /// Bitmask of deleted known keys (bit `i` == 1 means `KNOWN_KEYS[i]` was deleted).
    deleted_known: u16,
    /// Lazily allocated map for non-standard keys added via `__setitem__` / `update`.
    extras: Option<HashMap<String, Py<PyAny>>>,
}

impl AsgiScope {
    /// Build a fresh scope. The caller owns the `Arc<BatchSnapshot>` slot.
    pub fn new(batch: Arc<BatchSnapshot>, index: usize, root_path: String) -> Self {
        Self {
            batch,
            index,
            root_path,
            cached: [const { None }; KNOWN_KEYS.len()],
            deleted_known: 0,
            extras: None,
        }
    }

    #[inline]
    fn is_known_deleted(&self, idx: usize) -> bool {
        (self.deleted_known & (1u16 << idx)) != 0
    }

    fn build_known_by_index(&self, py: Python<'_>, idx: usize) -> PyResult<Py<PyAny>> {
        let node = self.batch.node(self.index);
        let obj: Py<PyAny> = match idx {
            0 => PyString::intern(py, "http").into_any().unbind(),
            1 => {
                let d = PyDict::new(py);
                d.set_item("version", "3.0")?;
                d.set_item("spec_version", "2.3")?;
                d.into_any().unbind()
            }
            2 => PyString::intern(py, node.http_version())
                .into_any()
                .unbind(),
            3 => PyString::intern(py, node.method_name()).into_any().unbind(),
            4 => PyString::new(py, self.batch.path(self.index))
                .into_any()
                .unbind(),
            5 => PyBytes::new(py, self.batch.path(self.index).as_bytes())
                .into_any()
                .unbind(),
            6 => {
                let q = self.batch.query(self.index).unwrap_or("");
                PyBytes::new(py, q.as_bytes()).into_any().unbind()
            }
            7 => PyString::new(py, &self.root_path).into_any().unbind(),
            8 => PyString::intern(py, "http").into_any().unbind(),
            9 => decode_asgi_headers(py, self.batch.headers_raw(self.index))?
                .into_any()
                .unbind(),
            10 | 11 => PyDict::new(py).into_any().unbind(),
            12 | 13 => py.None(),
            _ => unreachable!(),
        };
        Ok(obj)
    }

    fn collect_keys(&self) -> Vec<String> {
        let extra_len = self.extras.as_ref().map_or(0, HashMap::len);
        let mut keys: Vec<String> = Vec::with_capacity(KNOWN_KEYS.len() + extra_len);
        for (idx, k) in KNOWN_KEYS.iter().enumerate() {
            if !self.is_known_deleted(idx) {
                keys.push((*k).to_string());
            }
        }
        if let Some(ref extras) = self.extras {
            for k in extras.keys() {
                keys.push(k.clone());
            }
        }
        keys
    }
}

#[pymethods]
impl AsgiScope {
    fn __getitem__(&mut self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        if let Some(idx) = known_key_index(key) {
            if self.is_known_deleted(idx) {
                return Err(PyKeyError::new_err(key.to_string()));
            }
            if let Some(ref v) = self.cached[idx] {
                return Ok(v.clone_ref(py));
            }
            let v = self.build_known_by_index(py, idx)?;
            let clone = v.clone_ref(py);
            self.cached[idx] = Some(v);
            return Ok(clone);
        }
        if let Some(ref extras) = self.extras
            && let Some(v) = extras.get(key)
        {
            return Ok(v.clone_ref(py));
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __setitem__(&mut self, key: String, value: Py<PyAny>) {
        if let Some(idx) = known_key_index(&key) {
            self.deleted_known &= !(1u16 << idx);
            self.cached[idx] = Some(value);
        } else {
            self.extras
                .get_or_insert_with(HashMap::new)
                .insert(key, value);
        }
    }

    fn __delitem__(&mut self, key: &str) -> PyResult<()> {
        if let Some(idx) = known_key_index(key) {
            if self.is_known_deleted(idx) {
                return Err(PyKeyError::new_err(key.to_string()));
            }
            self.deleted_known |= 1u16 << idx;
            self.cached[idx] = None;
            return Ok(());
        }
        if let Some(ref mut extras) = self.extras
            && extras.remove(key).is_some()
        {
            return Ok(());
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&self, key: &str) -> bool {
        if let Some(idx) = known_key_index(key) {
            return !self.is_known_deleted(idx);
        }
        self.extras
            .as_ref()
            .is_some_and(|extras| extras.contains_key(key))
    }

    fn __len__(&self) -> usize {
        let live_known = KNOWN_KEYS.len() - (self.deleted_known.count_ones() as usize);
        let extra_len = self.extras.as_ref().map_or(0, HashMap::len);
        live_known + extra_len
    }

    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<AsgiScopeKeyIter>> {
        let extras = slf
            .extras
            .as_ref()
            .map(|e| e.keys().cloned().collect())
            .unwrap_or_default();
        Py::new(
            py,
            AsgiScopeKeyIter {
                deleted_known: slf.deleted_known,
                known_pos: 0,
                extras,
                extra_pos: 0,
            },
        )
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
                self.__setitem__(key, val);
                Ok(clone)
            }
        }
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let extra_len = self.extras.as_ref().map_or(0, HashMap::len);
        let mut items: Vec<Bound<'py, PyAny>> = Vec::with_capacity(KNOWN_KEYS.len() + extra_len);
        for (idx, k) in KNOWN_KEYS.iter().enumerate() {
            if !self.is_known_deleted(idx) {
                items.push(PyString::intern(py, k).into_any());
            }
        }
        if let Some(ref extras) = self.extras {
            for k in extras.keys() {
                items.push(PyString::new(py, k).into_any());
            }
        }
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
            let t = PyTuple::new(py, [PyString::new(py, k).into_any(), v.into_bound(py)])?;
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
        let my_len = self.__len__();
        let keys_call = other.call_method0("keys")?;
        let iter = keys_call.try_iter()?;
        let mut other_count = 0usize;
        for k in iter {
            let k = k?;
            let ks: String = k.extract()?;
            if !self.__contains__(&ks) {
                return Ok(false);
            }
            let other_val = other.get_item(&ks)?;
            let my_val = self.__getitem__(py, &ks)?;
            if !my_val.into_bound(py).eq(&other_val)? {
                return Ok(false);
            }
            other_count += 1;
        }
        Ok(other_count == my_len)
    }

    fn __repr__(&self) -> String {
        format!("AsgiScope({} keys)", self.__len__())
    }
}

/// Iterator over the scope's live keys.
#[pyclass]
pub struct AsgiScopeKeyIter {
    deleted_known: u16,
    known_pos: usize,
    extras: Vec<String>,
    extra_pos: usize,
}

#[pymethods]
impl AsgiScopeKeyIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        while self.known_pos < KNOWN_KEYS.len() {
            let idx = self.known_pos;
            self.known_pos += 1;
            if (self.deleted_known & (1u16 << idx)) == 0 {
                return Some(PyString::intern(py, KNOWN_KEYS[idx]));
            }
        }
        if self.extra_pos < self.extras.len() {
            let k = &self.extras[self.extra_pos];
            self.extra_pos += 1;
            Some(PyString::new(py, k))
        } else {
            None
        }
    }
}

/// Domain-oriented alias for [`AsgiScopeKeyIter`].
pub type AsgiScopeIterator = AsgiScopeKeyIter;
