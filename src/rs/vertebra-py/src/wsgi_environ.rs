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
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use vertebra_core::transport::BatchSnapshot;

use crate::headers::iter_raw_headers;

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
    extras: HashMap<String, Py<PyAny>>,
    /// Keys that have been removed via `__delitem__`.
    deleted: HashSet<String>,
    /// Lazily decoded header map (lowercase name -> value). Built on first
    /// HTTP_* lookup.
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
            extras: HashMap::new(),
            deleted: HashSet::new(),
            header_cache: None,
        }
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
                let io = py.import("io")?;
                let bio = io.call_method1("BytesIO", (body_bytes,))?;
                bio.into_any().unbind()
            }
            "wsgi.errors" => {
                let sys = py.import("sys")?;
                sys.getattr("stderr")?.unbind()
            }
            "wsgi.multithread" => true.into_pyobject(py)?.to_owned().into_any().unbind(),
            "wsgi.multiprocess" => true.into_pyobject(py)?.to_owned().into_any().unbind(),
            "wsgi.run_once" => false.into_pyobject(py)?.to_owned().into_any().unbind(),
            "CONTENT_TYPE" => {
                let v = self
                    .ensure_headers()
                    .get("content-type")
                    .map(String::as_str)
                    .unwrap_or("");
                PyString::new(py, v).into_any().unbind()
            }
            "CONTENT_LENGTH" => {
                let v = self
                    .ensure_headers()
                    .get("content-length")
                    .map(String::as_str)
                    .unwrap_or("");
                PyString::new(py, v).into_any().unbind()
            }
            _ if key.starts_with("HTTP_") => {
                let header_name = key[5..].to_ascii_lowercase().replace('_', "-");
                match self.ensure_headers().get(&header_name) {
                    Some(val) => PyString::new(py, val).into_any().unbind(),
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(obj))
    }

    fn collect_keys(&mut self) -> Vec<String> {
        let _ = self.ensure_headers();
        let headers = self.header_cache.as_ref().unwrap();
        let mut keys: Vec<String> =
            Vec::with_capacity(STATIC_KEYS.len() + self.extras.len() + headers.len());
        for k in STATIC_KEYS {
            if !self.deleted.contains(*k) {
                keys.push((*k).to_string());
            }
        }
        if headers.contains_key("content-type") && !self.deleted.contains("CONTENT_TYPE") {
            keys.push("CONTENT_TYPE".to_string());
        }
        if headers.contains_key("content-length") && !self.deleted.contains("CONTENT_LENGTH") {
            keys.push("CONTENT_LENGTH".to_string());
        }
        for name in headers.keys() {
            if name == "content-type" || name == "content-length" {
                continue;
            }
            let cgi = format!("HTTP_{}", name.to_ascii_uppercase().replace('-', "_"));
            if !self.deleted.contains(&cgi) {
                keys.push(cgi);
            }
        }
        for k in self.extras.keys() {
            if !self.deleted.contains(k)
                && !STATIC_KEYS.contains(&k.as_str())
                && !k.starts_with("HTTP_")
                && k != "CONTENT_TYPE"
                && k != "CONTENT_LENGTH"
            {
                keys.push(k.clone());
            }
        }
        keys
    }
}

#[pymethods]
impl WsgiEnviron {
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
        self.cached.remove(key);
        if in_extras
            || STATIC_KEYS.contains(&key)
            || key.starts_with("HTTP_")
            || key == "CONTENT_TYPE"
            || key == "CONTENT_LENGTH"
        {
            self.deleted.insert(key.to_string());
            return Ok(());
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&mut self, key: &str) -> bool {
        if self.deleted.contains(key) {
            return false;
        }
        if self.extras.contains_key(key) || STATIC_KEYS.contains(&key) {
            return true;
        }
        if key == "CONTENT_TYPE" || key == "CONTENT_LENGTH" {
            return true;
        }
        if let Some(rest) = key.strip_prefix("HTTP_") {
            let header_name = rest.to_ascii_lowercase().replace('_', "-");
            return self.ensure_headers().contains_key(&header_name);
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
                self.deleted.remove(&key);
                self.extras.insert(key, val);
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
