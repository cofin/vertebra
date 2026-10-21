//! Lazy ASGI 3.0 scope with a 20-slot inline key table (`0..14` ASGI + `14..20` Litestar v2/v3).

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use vertebra_core::transport::{BatchSnapshot, TransportNode, encode_headers_compact};

use crate::headers::decode_asgi_headers;

/// 20 inline ASGI scope slots:
/// - `0..14`: standard ASGI 3.0 HTTP connection scope keys
/// - `14..20`: Litestar v2 + v3 hot-path scope keys (`app`, `litestar_app`, `path_params`, `route_handler`, `path_template`, `connection`)
pub const KNOWN_KEYS: [&str; 20] = [
    "type",
    "asgi",
    "http_version",
    "method",
    "scheme",
    "path",
    "raw_path",
    "query_string",
    "root_path",
    "headers",
    "server",
    "client",
    "state",
    "extensions",
    "app",
    "litestar_app",
    "path_params",
    "route_handler",
    "path_template",
    "connection",
];

const IDX_STATE: usize = 12;
const IDX_EXTENSIONS: usize = 13;
/// Slots `14..20` begin unset until populated by framework routing or middleware.
const INITIAL_UNSET_MASK: u32 = 0b11_1111 << 14;

#[inline]
pub fn known_key_index(key: &str) -> Option<usize> {
    match key {
        "type" => Some(0),
        "asgi" => Some(1),
        "http_version" => Some(2),
        "method" => Some(3),
        "scheme" => Some(4),
        "path" => Some(5),
        "raw_path" => Some(6),
        "query_string" => Some(7),
        "root_path" => Some(8),
        "headers" => Some(9),
        "server" => Some(10),
        "client" => Some(11),
        "state" => Some(12),
        "extensions" => Some(13),
        "app" => Some(14),
        "litestar_app" => Some(15),
        "path_params" => Some(16),
        "route_handler" => Some(17),
        "path_template" => Some(18),
        "connection" => Some(19),
        _ => None,
    }
}

#[inline]
fn intern_known_key<'py>(py: Python<'py>, idx: usize) -> &'py Bound<'py, PyString> {
    match idx {
        0 => pyo3::intern!(py, "type"),
        1 => pyo3::intern!(py, "asgi"),
        2 => pyo3::intern!(py, "http_version"),
        3 => pyo3::intern!(py, "method"),
        4 => pyo3::intern!(py, "scheme"),
        5 => pyo3::intern!(py, "path"),
        6 => pyo3::intern!(py, "raw_path"),
        7 => pyo3::intern!(py, "query_string"),
        8 => pyo3::intern!(py, "root_path"),
        9 => pyo3::intern!(py, "headers"),
        10 => pyo3::intern!(py, "server"),
        11 => pyo3::intern!(py, "client"),
        12 => pyo3::intern!(py, "state"),
        13 => pyo3::intern!(py, "extensions"),
        14 => pyo3::intern!(py, "app"),
        15 => pyo3::intern!(py, "litestar_app"),
        16 => pyo3::intern!(py, "path_params"),
        17 => pyo3::intern!(py, "route_handler"),
        18 => pyo3::intern!(py, "path_template"),
        19 => pyo3::intern!(py, "connection"),
        _ => unreachable!(),
    }
}

fn build_extensions_dict<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let ext = PyDict::new(py);
    ext.set_item(pyo3::intern!(py, "http.response.pathsend"), PyDict::new(py))?;
    ext.set_item(
        pyo3::intern!(py, "http.response.zerocopysend"),
        PyDict::new(py),
    )?;
    ext.set_item(pyo3::intern!(py, "http.response.trailers"), PyDict::new(py))?;
    ext.set_item(
        pyo3::intern!(py, "http.response.early_hint"),
        PyDict::new(py),
    )?;
    ext.set_item(pyo3::intern!(py, "http.response.debug"), PyDict::new(py))?;
    ext.set_item(pyo3::intern!(py, "http.response.sse"), PyDict::new(py))?;
    ext.set_item(
        pyo3::intern!(py, "websocket.http.response"),
        PyDict::new(py),
    )?;
    Ok(ext)
}

/// Lazy ASGI 3.0 scope with a 20-slot inline cache and freelist pooling.
#[pyclass(mapping, name = "AsgiScope", freelist = 128)]
pub struct AsgiScope {
    batch: Arc<BatchSnapshot>,
    index: usize,
    root_path: String,
    cached: [Option<Py<PyAny>>; KNOWN_KEYS.len()],
    state_cache: Option<Py<PyDict>>,
    deleted_known: u32,
    extras: Option<HashMap<String, Py<PyAny>>>,
}

impl AsgiScope {
    /// Build a fresh `AsgiScope` over a `BatchSnapshot` entry.
    pub fn new(batch: Arc<BatchSnapshot>, index: usize, root_path: String) -> Self {
        Self {
            batch,
            index,
            root_path,
            cached: [const { None }; KNOWN_KEYS.len()],
            state_cache: None,
            deleted_known: INITIAL_UNSET_MASK,
            extras: None,
        }
    }

    #[inline]
    fn is_known_deleted(&self, idx: usize) -> bool {
        (self.deleted_known & (1u32 << idx)) != 0
    }

    fn build_known_by_index(&mut self, py: Python<'_>, idx: usize) -> PyResult<Py<PyAny>> {
        let node = self.batch.node(self.index);
        let obj: Py<PyAny> = match idx {
            0 => pyo3::intern!(py, "http").clone().into_any().unbind(),
            1 => {
                let d = PyDict::new(py);
                d.set_item(pyo3::intern!(py, "version"), pyo3::intern!(py, "3.0"))?;
                d.set_item(pyo3::intern!(py, "spec_version"), pyo3::intern!(py, "2.3"))?;
                d.into_any().unbind()
            }
            2 => PyString::intern(py, node.http_version())
                .into_any()
                .unbind(),
            3 => PyString::intern(py, node.method_name()).into_any().unbind(),
            4 => pyo3::intern!(py, "http").clone().into_any().unbind(),
            5 => PyString::new(py, self.batch.path(self.index))
                .into_any()
                .unbind(),
            6 => PyBytes::new(py, self.batch.path(self.index).as_bytes())
                .into_any()
                .unbind(),
            7 => {
                let q = self.batch.query(self.index).unwrap_or("");
                PyBytes::new(py, q.as_bytes()).into_any().unbind()
            }
            8 => PyString::new(py, &self.root_path).into_any().unbind(),
            9 => decode_asgi_headers(py, self.batch.headers_raw(self.index))?
                .into_any()
                .unbind(),
            10 | 11 => py.None(),
            IDX_STATE => {
                if let Some(ref st) = self.state_cache {
                    st.clone_ref(py).into_any()
                } else {
                    let d = PyDict::new(py).unbind();
                    let out = d.clone_ref(py).into_any();
                    self.state_cache = Some(d);
                    out
                }
            }
            IDX_EXTENSIONS => build_extensions_dict(py)?.into_any().unbind(),
            _ => return Err(PyKeyError::new_err(KNOWN_KEYS[idx].to_string())),
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
    /// Construct a standalone `AsgiScope` with a default `GET /` HTTP/1.1 snapshot.
    #[new]
    #[pyo3(signature = (root_path=None))]
    pub fn py_new(root_path: Option<String>) -> Self {
        Self::from_parts(
            "GET",
            "/",
            "",
            None,
            root_path.as_deref().unwrap_or(""),
            "1.1",
        )
    }

    /// Construct a standalone `AsgiScope` directly from HTTP components for unit tests and direct dispatch.
    #[staticmethod]
    #[pyo3(signature = (*, method="GET", path="/", query_string="", headers=None, root_path="", http_version="1.1"))]
    pub fn from_parts(
        method: &str,
        path: &str,
        query_string: &str,
        headers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
        root_path: &str,
        http_version: &str,
    ) -> Self {
        let method_tag = match method {
            "GET" => 0,
            "POST" => 1,
            "PUT" => 2,
            "DELETE" => 3,
            "PATCH" => 4,
            "HEAD" => 5,
            "OPTIONS" => 6,
            "TRACE" => 7,
            "CONNECT" => 8,
            _ => 0,
        };
        let http_version_tag = match http_version {
            "1.0" => 0,
            "1.1" => 1,
            "2" => 2,
            "3" => 3,
            _ => 1,
        };
        let raw_headers = encode_headers_compact(&headers.unwrap_or_default());
        let mut slab = Vec::with_capacity(path.len() + query_string.len() + raw_headers.len());
        let path_offset = 0u32;
        let path_len = path.len() as u32;
        slab.extend_from_slice(path.as_bytes());
        let query_offset = slab.len() as u32;
        let query_len = query_string.len() as u32;
        slab.extend_from_slice(query_string.as_bytes());
        let headers_offset = slab.len() as u32;
        let headers_len = raw_headers.len() as u32;
        slab.extend_from_slice(&raw_headers);

        let node = TransportNode {
            method_tag,
            http_version_tag,
            _pad: [0; 2],
            handler_id: 0,
            correlation_id: 0,
            path_offset,
            path_len,
            query_offset,
            query_len,
            headers_offset,
            headers_len,
            body_offset: 0,
            body_len: 0,
        };
        let snapshot = Arc::new(BatchSnapshot::new(slab, vec![node]));
        Self::new(snapshot, 0, root_path.to_string())
    }

    /// Return the 20 inline known scope keys.
    #[staticmethod]
    pub fn known_keys<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<Bound<'py, PyAny>> = (0..KNOWN_KEYS.len())
            .map(|idx| intern_known_key(py, idx).clone().into_any())
            .collect();
        PyList::new(py, &items)
    }

    pub fn has_extras(&self) -> bool {
        self.extras.as_ref().is_some_and(|e| !e.is_empty())
    }

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

    fn __setitem__(&mut self, py: Python<'_>, key: String, value: Py<PyAny>) {
        if let Some(idx) = known_key_index(&key) {
            self.deleted_known &= !(1u32 << idx);
            if idx == IDX_STATE {
                if let Ok(dict) = value.bind(py).cast::<PyDict>() {
                    self.state_cache = Some(dict.clone().unbind());
                } else {
                    self.state_cache = None;
                }
            }
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
            self.deleted_known |= 1u32 << idx;
            self.cached[idx] = None;
            if idx == IDX_STATE {
                self.state_cache = None;
            }
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
        let mut live_known = 0usize;
        for idx in 0..KNOWN_KEYS.len() {
            if !self.is_known_deleted(idx) {
                live_known += 1;
            }
        }
        let extra_len = self.extras.as_ref().map_or(0, HashMap::len);
        live_known + extra_len
    }

    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<AsgiScopeIterator>> {
        let extras = slf
            .extras
            .as_ref()
            .map(|e| e.keys().cloned().collect())
            .unwrap_or_default();
        Py::new(
            py,
            AsgiScopeIterator {
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
        if key == "state" {
            if !self.is_known_deleted(IDX_STATE) {
                if let Some(ref cached_state) = self.state_cache {
                    return Ok(cached_state.clone_ref(py).into_any());
                }
                if let Some(ref existing) = self.cached[IDX_STATE] {
                    return Ok(existing.clone_ref(py));
                }
            }
            if let Some(def_val) = default {
                let bound = def_val.bind(py);
                if let Ok(dict) = bound.cast::<PyDict>() {
                    let stored_dict = dict.clone().unbind();
                    let any_ref = stored_dict.clone_ref(py).into_any();
                    let ret_ref = stored_dict.clone_ref(py).into_any();
                    self.deleted_known &= !(1u32 << IDX_STATE);
                    self.state_cache = Some(stored_dict);
                    self.cached[IDX_STATE] = Some(any_ref);
                    return Ok(ret_ref);
                }
                let clone = def_val.clone_ref(py);
                self.__setitem__(py, key, def_val);
                return Ok(clone);
            }
        }
        match self.__getitem__(py, &key) {
            Ok(v) => Ok(v),
            Err(_) => {
                let val = default.unwrap_or_else(|| py.None());
                let clone = val.clone_ref(py);
                self.__setitem__(py, key, val);
                Ok(clone)
            }
        }
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let extra_len = self.extras.as_ref().map_or(0, HashMap::len);
        let mut items: Vec<Bound<'py, PyAny>> = Vec::with_capacity(KNOWN_KEYS.len() + extra_len);
        for idx in 0..KNOWN_KEYS.len() {
            if !self.is_known_deleted(idx) {
                items.push(intern_known_key(py, idx).clone().into_any());
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
            let key_obj = if let Some(idx) = known_key_index(k) {
                intern_known_key(py, idx).clone().into_any()
            } else {
                PyString::new(py, k).into_any()
            };
            let t = PyTuple::new(py, [key_obj, v.into_bound(py)])?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    fn update(&mut self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(dict) = other.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                self.__setitem__(py, key, v.unbind());
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
            self.__setitem__(py, key, val);
        }
        Ok(())
    }

    fn copy<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        let keys = self.collect_keys();
        for k in &keys {
            let v = self.__getitem__(py, k)?;
            if let Some(idx) = known_key_index(k) {
                d.set_item(intern_known_key(py, idx), v)?;
            } else {
                d.set_item(k, v)?;
            }
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
#[pyclass(name = "AsgiScopeIterator", freelist = 128)]
pub struct AsgiScopeIterator {
    deleted_known: u32,
    known_pos: usize,
    extras: Vec<String>,
    extra_pos: usize,
}

#[pymethods]
impl AsgiScopeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        while self.known_pos < KNOWN_KEYS.len() {
            let idx = self.known_pos;
            self.known_pos += 1;
            if (self.deleted_known & (1u32 << idx)) == 0 {
                return Some(intern_known_key(py, idx).clone());
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
