//! Lazy WSGI environ backed by a `BatchSnapshot` + index.
//!
//! Parallels `asgi_scope::AsgiScope` but produces the PEP 3333 CGI-style
//! keys (`REQUEST_METHOD`, `SCRIPT_NAME`, `PATH_INFO`, `QUERY_STRING`, etc.)
//! and surfaces request headers via the `HTTP_*` prefix on demand. Keeps
//! the `Arc<BatchSnapshot>` alive so path/header slices stay valid for as
//! long as the WSGI app holds the environ.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use vertebra_core::transport::BatchSnapshot;

use crate::headers::iter_raw_headers;

static BYTES_IO: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static SYS_STDERR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

#[inline]
fn eq_http_header_name(header_name: &[u8], cgi_rest: &[u8]) -> bool {
    header_name.len() == cgi_rest.len()
        && header_name
            .iter()
            .zip(cgi_rest)
            .all(|(&h, &k)| (h == b'-' && k == b'_') || h.eq_ignore_ascii_case(&k))
}

/// Fixed-layout WSGI keys that every environ must expose.
const STATIC_KEYS: &[&str] = &[
    "REQUEST_METHOD",
    "SCRIPT_NAME",
    "PATH_INFO",
    "QUERY_STRING",
    "SERVER_NAME",
    "SERVER_PORT",
    "SERVER_PROTOCOL",
    "wsgi.version",
    "wsgi.url_scheme",
    "wsgi.input",
    "wsgi.errors",
    "wsgi.multithread",
    "wsgi.multiprocess",
    "wsgi.run_once",
];

/// Lazy WSGI environ. Implements the mapping protocol so existing WSGI
/// apps treat it as a dict without the environ having to materialize every
/// key up-front.
#[pyclass(mapping, name = "WsgiEnviron")]
pub struct WsgiEnviron {
    batch: Arc<BatchSnapshot>,
    index: usize,
    server_name: String,
    server_port: u16,
    url_scheme: String,
    /// Cached materialized values for static and computed keys.
    cached: HashMap<String, Py<PyAny>>,
    /// User-set keys (via `__setitem__` / middleware).
    extras: Option<HashMap<String, Py<PyAny>>>,
    /// Keys that have been removed via `__delitem__`.
    deleted: Option<HashSet<String>>,
    /// Lazily decoded header map (lowercase name -> value). Built on first
    /// full key enumeration.
    header_cache: Option<HashMap<String, String>>,
}

impl WsgiEnviron {
    pub fn new(
        batch: Arc<BatchSnapshot>,
        index: usize,
        server_name: String,
        server_port: u16,
        url_scheme: String,
    ) -> Self {
        Self {
            batch,
            index,
            server_name,
            server_port,
            url_scheme,
            cached: HashMap::new(),
            extras: None,
            deleted: None,
            header_cache: None,
        }
    }

    #[inline]
    fn is_deleted(&self, key: &str) -> bool {
        self.deleted
            .as_ref()
            .is_some_and(|deleted| deleted.contains(key))
    }

    fn ensure_headers(&mut self) -> &HashMap<String, String> {
        if self.header_cache.is_none() {
            let raw = self.batch.headers_raw(self.index);
            let mut map = HashMap::new();
            for (name, value) in iter_raw_headers(raw) {
                let name_lower = String::from_utf8_lossy(name).to_ascii_lowercase();
                let val_str = String::from_utf8_lossy(value).into_owned();
                map.insert(name_lower, val_str);
            }
            self.header_cache = Some(map);
        }
        self.header_cache.as_ref().unwrap()
    }

    fn build_known(&mut self, py: Python<'_>, key: &str) -> PyResult<Option<Py<PyAny>>> {
        let node = self.batch.node(self.index);
        let obj: Py<PyAny> = match key {
            "REQUEST_METHOD" => PyString::intern(py, node.method_name()).into_any().unbind(),
            "SCRIPT_NAME" => PyString::intern(py, "").into_any().unbind(),
            "PATH_INFO" => PyString::new(py, self.batch.path(self.index))
                .into_any()
                .unbind(),
            "QUERY_STRING" => {
                let q = self.batch.query(self.index).unwrap_or("");
                PyString::new(py, q).into_any().unbind()
            }
            "SERVER_NAME" => PyString::new(py, &self.server_name).into_any().unbind(),
            "SERVER_PORT" => PyString::new(py, &self.server_port.to_string())
                .into_any()
                .unbind(),
            "SERVER_PROTOCOL" => PyString::intern(py, node.http_version())
                .into_any()
                .unbind(),
            "wsgi.version" => {
                let t = PyTuple::new(py, [1i32.into_pyobject(py)?, 0i32.into_pyobject(py)?])?;
                t.into_any().unbind()
            }
            "wsgi.url_scheme" => PyString::new(py, &self.url_scheme).into_any().unbind(),
            "wsgi.input" => {
                let body = self.batch.body(self.index);
                let body_bytes = PyBytes::new(py, body);
                let bytes_io = BYTES_IO.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
                    Ok(py.import("io")?.getattr("BytesIO")?.unbind())
                })?;
                let bio = bytes_io.bind(py).call1((body_bytes,))?;
                bio.into_any().unbind()
            }
            "wsgi.errors" => {
                let stderr = SYS_STDERR.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
                    Ok(py.import("sys")?.getattr("stderr")?.unbind())
                })?;
                stderr.clone_ref(py)
            }
            "wsgi.multithread" => true.into_pyobject(py)?.to_owned().into_any().unbind(),
            "wsgi.multiprocess" => true.into_pyobject(py)?.to_owned().into_any().unbind(),
            "wsgi.run_once" => false.into_pyobject(py)?.to_owned().into_any().unbind(),
            "CONTENT_TYPE" => {
                if let Some(ref cache) = self.header_cache {
                    let v = cache.get("content-type").map(String::as_str).unwrap_or("");
                    PyString::new(py, v).into_any().unbind()
                } else {
                    let raw = self.batch.headers_raw(self.index);
                    let v = iter_raw_headers(raw)
                        .find(|(n, _)| n.eq_ignore_ascii_case(b"content-type"))
                        .map(|(_, val)| String::from_utf8_lossy(val))
                        .unwrap_or_default();
                    PyString::new(py, &v).into_any().unbind()
                }
            }
            "CONTENT_LENGTH" => {
                if let Some(ref cache) = self.header_cache {
                    let v = cache
                        .get("content-length")
                        .map(String::as_str)
                        .unwrap_or("");
                    PyString::new(py, v).into_any().unbind()
                } else {
                    let raw = self.batch.headers_raw(self.index);
                    let v = iter_raw_headers(raw)
                        .find(|(n, _)| n.eq_ignore_ascii_case(b"content-length"))
                        .map(|(_, val)| String::from_utf8_lossy(val))
                        .unwrap_or_default();
                    PyString::new(py, &v).into_any().unbind()
                }
            }
            _ if key.starts_with("HTTP_") => {
                let rest = &key.as_bytes()[5..];
                if let Some(ref cache) = self.header_cache {
                    let header_name = key[5..].to_ascii_lowercase().replace('_', "-");
                    match cache.get(&header_name) {
                        Some(val) => PyString::new(py, val).into_any().unbind(),
                        None => return Ok(None),
                    }
                } else {
                    let raw = self.batch.headers_raw(self.index);
                    match iter_raw_headers(raw).find(|(n, _)| eq_http_header_name(n, rest)) {
                        Some((_, val)) => {
                            let val_str = String::from_utf8_lossy(val);
                            PyString::new(py, &val_str).into_any().unbind()
                        }
                        None => return Ok(None),
                    }
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(obj))
    }

    fn collect_keys(&mut self) -> Vec<String> {
        let _ = self.ensure_headers();
        let headers = self.header_cache.as_ref().unwrap();
        let extras_len = self.extras.as_ref().map_or(0, HashMap::len);
        let mut keys: Vec<String> =
            Vec::with_capacity(STATIC_KEYS.len() + extras_len + headers.len());
        for k in STATIC_KEYS {
            if !self.is_deleted(k) {
                keys.push((*k).to_string());
            }
        }
        if headers.contains_key("content-type") && !self.is_deleted("CONTENT_TYPE") {
            keys.push("CONTENT_TYPE".to_string());
        }
        if headers.contains_key("content-length") && !self.is_deleted("CONTENT_LENGTH") {
            keys.push("CONTENT_LENGTH".to_string());
        }
        for name in headers.keys() {
            if name == "content-type" || name == "content-length" {
                continue;
            }
            let cgi = format!("HTTP_{}", name.to_ascii_uppercase().replace('-', "_"));
            if !self.is_deleted(&cgi) {
                keys.push(cgi);
            }
        }
        if let Some(ref extras) = self.extras {
            for k in extras.keys() {
                if !self.is_deleted(k)
                    && !STATIC_KEYS.contains(&k.as_str())
                    && !k.starts_with("HTTP_")
                    && k != "CONTENT_TYPE"
                    && k != "CONTENT_LENGTH"
                {
                    keys.push(k.clone());
                }
            }
        }
        keys
    }
}

#[pymethods]
impl WsgiEnviron {
    fn __getitem__(&mut self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        if self.is_deleted(key) {
            return Err(PyKeyError::new_err(key.to_string()));
        }
        if let Some(ref extras) = self.extras
            && let Some(v) = extras.get(key)
        {
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
        if let Some(ref mut deleted) = self.deleted {
            deleted.remove(&key);
        }
        self.extras
            .get_or_insert_with(HashMap::new)
            .insert(key, value);
    }

    fn __delitem__(&mut self, key: &str) -> PyResult<()> {
        let in_extras = self
            .extras
            .as_mut()
            .is_some_and(|extras| extras.remove(key).is_some());
        self.cached.remove(key);
        if in_extras
            || STATIC_KEYS.contains(&key)
            || key.starts_with("HTTP_")
            || key == "CONTENT_TYPE"
            || key == "CONTENT_LENGTH"
        {
            self.deleted
                .get_or_insert_with(HashSet::new)
                .insert(key.to_string());
            return Ok(());
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&mut self, key: &str) -> bool {
        if self.is_deleted(key) {
            return false;
        }
        if self
            .extras
            .as_ref()
            .is_some_and(|extras| extras.contains_key(key))
            || STATIC_KEYS.contains(&key)
        {
            return true;
        }
        if key == "CONTENT_TYPE" || key == "CONTENT_LENGTH" {
            return true;
        }
        if let Some(rest) = key.strip_prefix("HTTP_") {
            if let Some(ref cache) = self.header_cache {
                let header_name = rest.to_ascii_lowercase().replace('_', "-");
                return cache.contains_key(&header_name);
            }
            let raw = self.batch.headers_raw(self.index);
            return iter_raw_headers(raw).any(|(n, _)| eq_http_header_name(n, rest.as_bytes()));
        }
        false
    }

    fn __len__(&mut self) -> usize {
        self.collect_keys().len()
    }

    fn __iter__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Py<WsgiEnvironKeyIter>> {
        let keys = slf.collect_keys();
        Py::new(py, WsgiEnvironKeyIter { keys, pos: 0 })
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

    fn keys<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
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

    fn __repr__(&self) -> String {
        format!(
            "WsgiEnviron(method={:?}, path={:?})",
            self.batch.node(self.index).method_name(),
            self.batch.path(self.index)
        )
    }
}

/// Iterator over the environ's live keys.
#[pyclass]
pub struct WsgiEnvironKeyIter {
    keys: Vec<String>,
    pos: usize,
}

#[pymethods]
impl WsgiEnvironKeyIter {
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

/// Domain-oriented alias for [`WsgiEnvironKeyIter`].
pub type WsgiEnvironIterator = WsgiEnvironKeyIter;
