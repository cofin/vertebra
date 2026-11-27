//! Lazy WSGI environ backed by `BatchSnapshot`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple, PyType};

use vertebra_core::transport::{BatchSnapshot, encode_headers_compact};

use super::input::{WsgiFileWrapper, WsgiInput};
use crate::headers::iter_raw_headers;
use crate::synthetic::SyntheticRequest;

static WSGI_VERSION_TUPLE: pyo3::sync::PyOnceLock<Py<PyTuple>> = pyo3::sync::PyOnceLock::new();

/// Fixed-layout PEP 3333 WSGI keys exposed by every `WsgiEnviron`.
pub const STATIC_KEYS: &[&str] = &[
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
    "wsgi.file_wrapper",
];

#[inline]
pub fn intern_static_wsgi_key<'py>(py: Python<'py>, key: &str) -> Bound<'py, PyString> {
    match key {
        "REQUEST_METHOD" => pyo3::intern!(py, "REQUEST_METHOD").clone(),
        "SCRIPT_NAME" => pyo3::intern!(py, "SCRIPT_NAME").clone(),
        "PATH_INFO" => pyo3::intern!(py, "PATH_INFO").clone(),
        "QUERY_STRING" => pyo3::intern!(py, "QUERY_STRING").clone(),
        "SERVER_NAME" => pyo3::intern!(py, "SERVER_NAME").clone(),
        "SERVER_PORT" => pyo3::intern!(py, "SERVER_PORT").clone(),
        "SERVER_PROTOCOL" => pyo3::intern!(py, "SERVER_PROTOCOL").clone(),
        "wsgi.version" => pyo3::intern!(py, "wsgi.version").clone(),
        "wsgi.url_scheme" => pyo3::intern!(py, "wsgi.url_scheme").clone(),
        "wsgi.input" => pyo3::intern!(py, "wsgi.input").clone(),
        "wsgi.errors" => pyo3::intern!(py, "wsgi.errors").clone(),
        "wsgi.multithread" => pyo3::intern!(py, "wsgi.multithread").clone(),
        "wsgi.multiprocess" => pyo3::intern!(py, "wsgi.multiprocess").clone(),
        "wsgi.run_once" => pyo3::intern!(py, "wsgi.run_once").clone(),
        "wsgi.file_wrapper" => pyo3::intern!(py, "wsgi.file_wrapper").clone(),
        "CONTENT_TYPE" => pyo3::intern!(py, "CONTENT_TYPE").clone(),
        "CONTENT_LENGTH" => pyo3::intern!(py, "CONTENT_LENGTH").clone(),
        "HTTP_HOST" => pyo3::intern!(py, "HTTP_HOST").clone(),
        "HTTP_USER_AGENT" => pyo3::intern!(py, "HTTP_USER_AGENT").clone(),
        "HTTP_ACCEPT" => pyo3::intern!(py, "HTTP_ACCEPT").clone(),
        "HTTP_ACCEPT_ENCODING" => pyo3::intern!(py, "HTTP_ACCEPT_ENCODING").clone(),
        "HTTP_AUTHORIZATION" => pyo3::intern!(py, "HTTP_AUTHORIZATION").clone(),
        "HTTP_COOKIE" => pyo3::intern!(py, "HTTP_COOKIE").clone(),
        "HTTP_CONNECTION" => pyo3::intern!(py, "HTTP_CONNECTION").clone(),
        "HTTP_UPGRADE" => pyo3::intern!(py, "HTTP_UPGRADE").clone(),
        _ => PyString::new(py, key),
    }
}

/// Batch-shared PEP 3333 WSGI server metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsgiServerConfig {
    pub root_path: String,
    pub server_name: String,
    pub server_port: u16,
    pub server_port_str: String,
    pub url_scheme: String,
}

impl WsgiServerConfig {
    pub fn new(
        root_path: impl Into<String>,
        server_name: impl Into<String>,
        server_port: u16,
        url_scheme: impl Into<String>,
    ) -> Self {
        let mut normalized_root = root_path.into();
        while normalized_root.ends_with('/') {
            normalized_root.pop();
        }
        Self {
            root_path: normalized_root,
            server_name: server_name.into(),
            server_port,
            server_port_str: server_port.to_string(),
            url_scheme: url_scheme.into(),
        }
    }
}

/// Lazy WSGI environ with freelist pooling and interned PEP 3333 keys.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(mapping, name = "WsgiEnviron", freelist = 128, immutable_type)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(mapping, name = "WsgiEnviron", immutable_type)
)]
pub struct WsgiEnviron {
    batch: Arc<BatchSnapshot>,
    index: usize,
    config: Arc<WsgiServerConfig>,
    wsgi_input: Option<Py<PyAny>>,
    cached: Option<HashMap<String, Py<PyAny>>>,
    extras: Option<HashMap<String, Py<PyAny>>>,
    deleted: Option<HashSet<String>>,
    header_cache: Option<HashMap<String, String>>,
}

impl WsgiEnviron {
    pub fn with_config(
        batch: Arc<BatchSnapshot>,
        index: usize,
        config: Arc<WsgiServerConfig>,
    ) -> Self {
        Self {
            batch,
            index,
            config,
            wsgi_input: None,
            cached: None,
            extras: None,
            deleted: None,
            header_cache: None,
        }
    }

    pub fn new(
        batch: Arc<BatchSnapshot>,
        index: usize,
        server_name: String,
        server_port: u16,
        url_scheme: String,
    ) -> Self {
        Self::with_config(
            batch,
            index,
            Arc::new(WsgiServerConfig::new(
                String::new(),
                server_name,
                server_port,
                url_scheme,
            )),
        )
    }

    pub fn with_root_path(
        batch: Arc<BatchSnapshot>,
        index: usize,
        root_path: String,
        server_name: String,
        server_port: u16,
        url_scheme: String,
    ) -> Self {
        Self::with_config(
            batch,
            index,
            Arc::new(WsgiServerConfig::new(
                root_path,
                server_name,
                server_port,
                url_scheme,
            )),
        )
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
                map.entry(name_lower)
                    .and_modify(|existing: &mut String| {
                        existing.push_str(", ");
                        existing.push_str(&val_str);
                    })
                    .or_insert(val_str);
            }
            self.header_cache = Some(map);
        }
        self.header_cache.as_ref().unwrap()
    }

    #[inline]
    fn build_static_key(&mut self, py: Python<'_>, key: &str) -> PyResult<Option<Py<PyAny>>> {
        let obj: Py<PyAny> = match key {
            "REQUEST_METHOD" => {
                crate::request::intern_http_method(py, self.batch.node(self.index).method_name())
                    .into_any()
                    .unbind()
            }
            "SCRIPT_NAME" => {
                if self.config.root_path.is_empty() {
                    pyo3::intern!(py, "").clone().into_any().unbind()
                } else {
                    PyString::new(py, &self.config.root_path)
                        .into_any()
                        .unbind()
                }
            }
            "PATH_INFO" => {
                let raw_path = self.batch.path(self.index);
                let path_info = if !self.config.root_path.is_empty()
                    && raw_path.starts_with(&self.config.root_path)
                {
                    let stripped = &raw_path[self.config.root_path.len()..];
                    if stripped.is_empty() { "/" } else { stripped }
                } else {
                    raw_path
                };
                PyString::new(py, path_info).into_any().unbind()
            }
            "QUERY_STRING" => match self.batch.query(self.index) {
                Some(q) if !q.is_empty() => PyString::new(py, q).into_any().unbind(),
                _ => pyo3::intern!(py, "").clone().into_any().unbind(),
            },
            "SERVER_NAME" => PyString::new(py, &self.config.server_name)
                .into_any()
                .unbind(),
            "SERVER_PORT" => PyString::new(py, &self.config.server_port_str)
                .into_any()
                .unbind(),
            "SERVER_PROTOCOL" => {
                let ver = self.batch.node(self.index).http_version();
                if ver.starts_with("HTTP/") {
                    crate::request::intern_http_version(py, ver)
                        .into_any()
                        .unbind()
                } else if ver == "1.1" {
                    pyo3::intern!(py, "HTTP/1.1").clone().into_any().unbind()
                } else if ver == "1.0" {
                    pyo3::intern!(py, "HTTP/1.0").clone().into_any().unbind()
                } else if ver == "2" || ver == "2.0" {
                    pyo3::intern!(py, "HTTP/2").clone().into_any().unbind()
                } else if ver == "3" || ver == "3.0" {
                    pyo3::intern!(py, "HTTP/3").clone().into_any().unbind()
                } else {
                    PyString::new(py, &format!("HTTP/{ver}"))
                        .into_any()
                        .unbind()
                }
            }
            "wsgi.version" => {
                let tup = WSGI_VERSION_TUPLE.get_or_try_init(py, || {
                    let t = PyTuple::new(py, [1i32.into_pyobject(py)?, 0i32.into_pyobject(py)?])?;
                    Ok::<Py<PyTuple>, PyErr>(t.unbind())
                })?;
                tup.clone_ref(py).into_any()
            }
            "wsgi.url_scheme" => {
                if self.config.url_scheme == "http" {
                    pyo3::intern!(py, "http").clone().into_any().unbind()
                } else if self.config.url_scheme == "https" {
                    pyo3::intern!(py, "https").clone().into_any().unbind()
                } else {
                    PyString::new(py, &self.config.url_scheme)
                        .into_any()
                        .unbind()
                }
            }
            "wsgi.input" => {
                if let Some(ref inp) = self.wsgi_input {
                    return Ok(Some(inp.clone_ref(py)));
                }
                let inp = Py::new(
                    py,
                    WsgiInput::from_snapshot(Arc::clone(&self.batch), self.index),
                )?
                .into_any();
                let clone = inp.clone_ref(py);
                self.wsgi_input = Some(inp);
                clone
            }
            "wsgi.file_wrapper" => PyType::new::<WsgiFileWrapper>(py).into_any().unbind(),
            "wsgi.errors" => {
                let stderr = py
                    .import(pyo3::intern!(py, "sys"))?
                    .getattr(pyo3::intern!(py, "stderr"))?;
                stderr.into_any().unbind()
            }
            "wsgi.multithread" => true.into_pyobject(py)?.to_owned().into_any().unbind(),
            "wsgi.multiprocess" => false.into_pyobject(py)?.to_owned().into_any().unbind(),
            "wsgi.run_once" => false.into_pyobject(py)?.to_owned().into_any().unbind(),
            _ => return Ok(None),
        };
        Ok(Some(obj))
    }

    #[inline]
    fn matches_cgi_header(raw_name: &[u8], cgi_rest: &[u8]) -> bool {
        if raw_name.len() != cgi_rest.len() {
            return false;
        }
        raw_name.iter().zip(cgi_rest.iter()).all(|(&nb, &kb)| {
            (nb == b'-' && kb == b'_') || (nb != b'_' && nb.to_ascii_uppercase() == kb)
        })
    }

    fn scan_header_in_place(&self, cgi_key: &str) -> Option<String> {
        let raw = self.batch.headers_raw(self.index);
        let mut combined: Option<String> = None;
        match cgi_key {
            "CONTENT_TYPE" => {
                for (name, value) in iter_raw_headers(raw) {
                    if name.eq_ignore_ascii_case(b"content-type") {
                        let val_str = String::from_utf8_lossy(value);
                        if let Some(ref mut existing) = combined {
                            existing.push_str(", ");
                            existing.push_str(&val_str);
                        } else {
                            combined = Some(val_str.into_owned());
                        }
                    }
                }
            }
            "CONTENT_LENGTH" => {
                for (name, value) in iter_raw_headers(raw) {
                    if name.eq_ignore_ascii_case(b"content-length") {
                        let val_str = String::from_utf8_lossy(value);
                        if let Some(ref mut existing) = combined {
                            existing.push_str(", ");
                            existing.push_str(&val_str);
                        } else {
                            combined = Some(val_str.into_owned());
                        }
                    }
                }
            }
            _ if cgi_key.starts_with("HTTP_") => {
                let rest = &cgi_key.as_bytes()[5..];
                for (name, value) in iter_raw_headers(raw) {
                    if Self::matches_cgi_header(name, rest) {
                        let val_str = String::from_utf8_lossy(value);
                        if let Some(ref mut existing) = combined {
                            existing.push_str(", ");
                            existing.push_str(&val_str);
                        } else {
                            combined = Some(val_str.into_owned());
                        }
                    }
                }
            }
            _ => {}
        }
        combined
    }

    #[inline]
    fn has_header_in_place(&self, cgi_key: &str) -> bool {
        let raw = self.batch.headers_raw(self.index);
        match cgi_key {
            "CONTENT_TYPE" => {
                iter_raw_headers(raw).any(|(n, _)| n.eq_ignore_ascii_case(b"content-type"))
            }
            "CONTENT_LENGTH" => {
                iter_raw_headers(raw).any(|(n, _)| n.eq_ignore_ascii_case(b"content-length"))
            }
            _ if cgi_key.starts_with("HTTP_") => {
                let rest = &cgi_key.as_bytes()[5..];
                iter_raw_headers(raw).any(|(n, _)| Self::matches_cgi_header(n, rest))
            }
            _ => false,
        }
    }

    fn build_header_key(&mut self, py: Python<'_>, key: &str) -> PyResult<Option<Py<PyAny>>> {
        if let Some(ref cache) = self.header_cache {
            let obj: Py<PyAny> = match key {
                "CONTENT_TYPE" => {
                    let v = cache.get("content-type").map(String::as_str).unwrap_or("");
                    PyString::new(py, v).into_any().unbind()
                }
                "CONTENT_LENGTH" => {
                    let v = cache
                        .get("content-length")
                        .map(String::as_str)
                        .unwrap_or("");
                    PyString::new(py, v).into_any().unbind()
                }
                _ if key.starts_with("HTTP_") => {
                    let header_name = key[5..].to_ascii_lowercase().replace('_', "-");
                    match cache.get(&header_name) {
                        Some(val) => PyString::new(py, val).into_any().unbind(),
                        None => return Ok(None),
                    }
                }
                _ => return Ok(None),
            };
            return Ok(Some(obj));
        }
        match key {
            "CONTENT_TYPE" | "CONTENT_LENGTH" => {
                let val = self.scan_header_in_place(key).unwrap_or_default();
                Ok(Some(PyString::new(py, &val).into_any().unbind()))
            }
            _ if key.starts_with("HTTP_") => match self.scan_header_in_place(key) {
                Some(val) => Ok(Some(PyString::new(py, &val).into_any().unbind())),
                None => Ok(None),
            },
            _ => Ok(None),
        }
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
    #[staticmethod]
    #[pyo3(signature = (*, method="GET", path="/", query_string="", headers=None, body=None, server_name="localhost", server_port=8000, url_scheme="http"))]
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        method: &str,
        path: &str,
        query_string: &str,
        headers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
        body: Option<&[u8]>,
        server_name: &str,
        server_port: u16,
        url_scheme: &str,
    ) -> Self {
        let headers_vec = headers.unwrap_or_default();
        let raw_headers = encode_headers_compact(&headers_vec);
        let snapshot = SyntheticRequest::build(
            method,
            path,
            if query_string.is_empty() {
                None
            } else {
                Some(query_string)
            },
            &raw_headers,
            body.unwrap_or(b""),
            0,
            0,
            "1.1",
        );
        Self::new(
            snapshot,
            0,
            server_name.to_string(),
            server_port,
            url_scheme.to_string(),
        )
    }

    pub fn __getitem__(&mut self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        if self.is_deleted(key) {
            return Err(PyKeyError::new_err(key.to_string()));
        }
        if let Some(ref extras) = self.extras
            && let Some(v) = extras.get(key)
        {
            return Ok(v.clone_ref(py));
        }
        if let Some(v) = self.build_static_key(py, key)? {
            return Ok(v);
        }
        if let Some(ref cached) = self.cached
            && let Some(v) = cached.get(key)
        {
            return Ok(v.clone_ref(py));
        }
        if let Some(v) = self.build_header_key(py, key)? {
            let clone = v.clone_ref(py);
            self.cached
                .get_or_insert_with(HashMap::new)
                .insert(key.to_string(), v);
            return Ok(clone);
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    pub fn __setitem__(&mut self, key: String, value: Py<PyAny>) {
        if let Some(ref mut deleted) = self.deleted {
            deleted.remove(&key);
        }
        self.extras
            .get_or_insert_with(HashMap::new)
            .insert(key, value);
    }

    pub fn __delitem__(&mut self, key: &str) -> PyResult<()> {
        let in_extras = self
            .extras
            .as_mut()
            .is_some_and(|extras| extras.remove(key).is_some());
        if key == "wsgi.input" {
            self.wsgi_input = None;
        }
        if let Some(ref mut cached) = self.cached {
            cached.remove(key);
        }
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

    pub fn __contains__(&mut self, key: &str) -> bool {
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
        if key == "CONTENT_TYPE" || key == "CONTENT_LENGTH" || key.starts_with("HTTP_") {
            if let Some(ref cache) = self.header_cache {
                if key == "CONTENT_TYPE" {
                    return cache.contains_key("content-type");
                }
                if key == "CONTENT_LENGTH" {
                    return cache.contains_key("content-length");
                }
                let header_name = key[5..].to_ascii_lowercase().replace('_', "-");
                return cache.contains_key(&header_name);
            }
            return self.has_header_in_place(key);
        }
        false
    }

    /// Return the underlying zero-copy `Request` backing this `WsgiEnviron`.
    pub fn request(&self) -> crate::request::Request {
        crate::request::Request::new(Arc::clone(&self.batch), self.index)
    }

    pub fn __len__(&mut self) -> usize {
        self.collect_keys().len()
    }

    pub fn __iter__(
        mut slf: PyRefMut<'_, Self>,
        py: Python<'_>,
    ) -> PyResult<Py<WsgiEnvironIterator>> {
        let keys = slf.collect_keys();
        Py::new(py, WsgiEnvironIterator { keys, pos: 0 })
    }

    #[pyo3(signature = (key, default=None))]
    pub fn get(
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
    pub fn pop(
        &mut self,
        py: Python<'_>,
        key: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        match self.__getitem__(py, key) {
            Ok(v) => {
                let _ = self.__delitem__(key);
                Ok(v)
            }
            Err(e) => {
                if let Some(def) = default {
                    Ok(def)
                } else {
                    Err(e)
                }
            }
        }
    }

    #[pyo3(signature = (key, default=None))]
    pub fn setdefault(
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

    pub fn keys<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let keys = self.collect_keys();
        let items: Vec<Bound<'py, PyAny>> = keys
            .into_iter()
            .map(|k| intern_static_wsgi_key(py, &k).into_any())
            .collect();
        PyList::new(py, &items)
    }

    pub fn values<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let keys = self.collect_keys();
        let mut items: Vec<Bound<'py, PyAny>> = Vec::with_capacity(keys.len());
        for k in &keys {
            let v = self.__getitem__(py, k)?;
            items.push(v.into_bound(py));
        }
        PyList::new(py, &items)
    }

    pub fn items<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let keys = self.collect_keys();
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(keys.len());
        for k in &keys {
            let v = self.__getitem__(py, k)?;
            let t = PyTuple::new(
                py,
                [intern_static_wsgi_key(py, k).into_any(), v.into_bound(py)],
            )?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    pub fn update(&mut self, other: &Bound<'_, PyAny>) -> PyResult<()> {
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

    pub fn copy<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        let keys = self.collect_keys();
        for k in &keys {
            let v = self.__getitem__(py, k)?;
            d.set_item(intern_static_wsgi_key(py, k), v)?;
        }
        Ok(d)
    }

    pub fn clear(&mut self) {
        let keys = self.collect_keys();
        for k in keys {
            let _ = self.__delitem__(&k);
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "WsgiEnviron(method={:?}, path={:?})",
            self.batch.node(self.index).method_name(),
            self.batch.path(self.index)
        )
    }
}

/// Iterator over the environ's live keys.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "WsgiEnvironIterator", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "WsgiEnvironIterator", immutable_type))]
pub struct WsgiEnvironIterator {
    keys: Vec<String>,
    pos: usize,
}

#[pymethods]
impl WsgiEnvironIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        if self.pos < self.keys.len() {
            let k = &self.keys[self.pos];
            self.pos += 1;
            Some(intern_static_wsgi_key(py, k))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wsgi_server_config_normalizes_root_path() {
        assert_eq!(
            WsgiServerConfig::new("/", "localhost", 8000, "http").root_path,
            ""
        );
        assert_eq!(
            WsgiServerConfig::new("/v1/", "localhost", 8000, "http").root_path,
            "/v1"
        );
        assert_eq!(
            WsgiServerConfig::new("/v1///", "localhost", 8000, "http").root_path,
            "/v1"
        );
        assert_eq!(
            WsgiServerConfig::new("", "localhost", 8000, "http").root_path,
            ""
        );
    }

    #[test]
    fn wsgi_environ_with_config_shares_arc_without_cloning_strings() {
        let snap = SyntheticRequest::build("GET", "/v1/items", None, b"", b"", 0, 1, "1.1");
        let cfg = Arc::new(WsgiServerConfig::new("/v1/", "api.local", 8443, "https"));
        let env1 = WsgiEnviron::with_config(Arc::clone(&snap), 0, Arc::clone(&cfg));
        let env2 = WsgiEnviron::with_config(Arc::clone(&snap), 0, Arc::clone(&cfg));
        assert_eq!(Arc::strong_count(&cfg), 3);
        assert!(Arc::ptr_eq(&env1.config, &env2.config));
        assert_eq!(env1.config.root_path, "/v1");
        assert_eq!(env1.config.server_name, "api.local");
        assert_eq!(env1.config.server_port, 8443);
        assert_eq!(env1.config.url_scheme, "https");
    }

    #[test]
    fn wsgi_environ_compat_constructors_preserve_metadata() {
        let snap = SyntheticRequest::build("GET", "/legacy/orders", None, b"", b"", 0, 2, "1.1");
        let env_new = WsgiEnviron::new(
            Arc::clone(&snap),
            0,
            "example.com".to_string(),
            8080,
            "https".to_string(),
        );
        assert_eq!(env_new.config.root_path, "");
        assert_eq!(env_new.config.server_name, "example.com");
        assert_eq!(env_new.config.server_port, 8080);
        assert_eq!(env_new.config.url_scheme, "https");

        let env_root = WsgiEnviron::with_root_path(
            snap,
            0,
            "/legacy/".to_string(),
            "internal".to_string(),
            9000,
            "http".to_string(),
        );
        assert_eq!(env_root.config.root_path, "/legacy");
        assert_eq!(env_root.config.server_name, "internal");
        assert_eq!(env_root.config.server_port, 9000);
        assert_eq!(env_root.config.url_scheme, "http");
    }
}
