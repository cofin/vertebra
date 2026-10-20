//! Lazy WSGI environ, zero-copy `WsgiInput`, and `WsgiFileWrapper` backed by `BatchSnapshot`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use pyo3::exceptions::{PyIOError, PyKeyError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple, PyType};

use vertebra_core::transport::{BatchSnapshot, TransportNode, encode_headers_compact};

use crate::headers::iter_raw_headers;

static SYS_STDERR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

#[inline]
fn eq_http_header_name(header_name: &[u8], cgi_rest: &[u8]) -> bool {
    header_name.len() == cgi_rest.len()
        && header_name
            .iter()
            .zip(cgi_rest)
            .all(|(&h, &k)| (h == b'-' && k == b'_') || h.eq_ignore_ascii_case(&k))
}

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
fn intern_static_wsgi_key<'py>(py: Python<'py>, key: &str) -> Bound<'py, PyString> {
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
        _ => PyString::new(py, key),
    }
}

/// Zero-copy PEP 3333 `wsgi.input` reader backed by `Bytes` without Python `io.BytesIO`.
#[pyclass(name = "WsgiInput", freelist = 128)]
pub struct WsgiInput {
    data: Bytes,
    pos: usize,
}

impl WsgiInput {
    pub fn from_bytes(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }
}

#[pymethods]
impl WsgiInput {
    #[new]
    #[pyo3(signature = (data=None))]
    pub fn py_new(data: Option<&[u8]>) -> Self {
        Self {
            data: Bytes::copy_from_slice(data.unwrap_or(b"")),
            pos: 0,
        }
    }

    #[pyo3(signature = (size=-1))]
    pub fn read<'py>(&mut self, py: Python<'py>, size: isize) -> Bound<'py, PyBytes> {
        let remaining = self.data.len().saturating_sub(self.pos);
        let take = if size < 0 {
            remaining
        } else {
            (size as usize).min(remaining)
        };
        let slice = &self.data[self.pos..self.pos + take];
        self.pos += take;
        PyBytes::new(py, slice)
    }

    #[pyo3(signature = (size=-1))]
    pub fn readline<'py>(&mut self, py: Python<'py>, size: isize) -> Bound<'py, PyBytes> {
        if self.pos >= self.data.len() {
            return PyBytes::new(py, b"");
        }
        let max_end = if size < 0 {
            self.data.len()
        } else {
            self.data.len().min(self.pos + (size as usize))
        };
        let window = &self.data[self.pos..max_end];
        let take = match window.iter().position(|&b| b == b'\n') {
            Some(idx) => idx + 1,
            None => window.len(),
        };
        let slice = &self.data[self.pos..self.pos + take];
        self.pos += take;
        PyBytes::new(py, slice)
    }

    #[pyo3(signature = (hint=-1))]
    pub fn readlines<'py>(&mut self, py: Python<'py>, hint: isize) -> PyResult<Bound<'py, PyList>> {
        let mut lines: Vec<Bound<'py, PyAny>> = Vec::new();
        let mut total = 0usize;
        loop {
            let line = self.readline(py, -1);
            let len = line.as_bytes().len();
            if len == 0 {
                break;
            }
            lines.push(line.into_any());
            total += len;
            if hint > 0 && total >= (hint as usize) {
                break;
            }
        }
        PyList::new(py, &lines)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        let line = self.readline(py, -1);
        if line.as_bytes().is_empty() {
            None
        } else {
            Some(line)
        }
    }
}

/// PEP 3333 `wsgi.file_wrapper` implementation resolving filesystem paths for Rust `pathsend`.
#[pyclass(name = "WsgiFileWrapper", freelist = 128)]
pub struct WsgiFileWrapper {
    filelike: Py<PyAny>,
    block_size: usize,
    path: Option<String>,
}

#[pymethods]
impl WsgiFileWrapper {
    #[new]
    #[pyo3(signature = (filelike, block_size=65536))]
    pub fn py_new(filelike: &Bound<'_, PyAny>, block_size: usize) -> Self {
        let path = filelike
            .getattr("name")
            .ok()
            .and_then(|n| n.extract::<String>().ok())
            .filter(|p| std::path::Path::new(p).is_file());
        Self {
            filelike: filelike.clone().unbind(),
            block_size: block_size.max(4096),
            path,
        }
    }

    #[getter]
    pub fn path(&self) -> Option<String> {
        self.path.clone()
    }

    #[getter]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    #[getter]
    pub fn filelike(&self, py: Python<'_>) -> Py<PyAny> {
        self.filelike.clone_ref(py)
    }

    /// Read the entire wrapped file in Rust (releasing the GIL) when backed by a filesystem path.
    pub fn read_all_bytes<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let Some(ref file_path) = self.path else {
            return Ok(None);
        };
        let path_owned = file_path.clone();
        let data = py
            .detach(|| std::fs::read(&path_owned))
            .map_err(|e| PyIOError::new_err(format!("failed to read {file_path}: {e}")))?;
        Ok(Some(PyBytes::new(py, &data)))
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let bound = self.filelike.bind(py);
        if bound.hasattr("close")? {
            bound.call_method0("close")?;
        }
        Ok(())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let chunk = self
            .filelike
            .bind(py)
            .call_method1("read", (self.block_size,))?;
        if let Ok(b) = chunk.cast::<PyBytes>() {
            if b.as_bytes().is_empty() {
                return Ok(None);
            }
            return Ok(Some(b.clone()));
        }
        let extracted: Vec<u8> = chunk.extract()?;
        if extracted.is_empty() {
            Ok(None)
        } else {
            Ok(Some(PyBytes::new(py, &extracted)))
        }
    }
}

/// Lazy WSGI environ with freelist pooling and interned PEP 3333 keys.
#[pyclass(mapping, name = "WsgiEnviron", freelist = 128)]
pub struct WsgiEnviron {
    batch: Arc<BatchSnapshot>,
    index: usize,
    server_name: String,
    server_port: u16,
    url_scheme: String,
    cached: HashMap<String, Py<PyAny>>,
    extras: Option<HashMap<String, Py<PyAny>>>,
    deleted: Option<HashSet<String>>,
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
            "SCRIPT_NAME" => pyo3::intern!(py, "").clone().into_any().unbind(),
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
                let body = Bytes::copy_from_slice(self.batch.body(self.index));
                Py::new(py, WsgiInput::from_bytes(body))?.into_any()
            }
            "wsgi.file_wrapper" => PyType::new::<WsgiFileWrapper>(py).into_any().unbind(),
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
        let body_bytes = body.unwrap_or(b"");
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
        let raw_headers = encode_headers_compact(&headers.unwrap_or_default());
        let mut slab = Vec::with_capacity(
            path.len() + query_string.len() + raw_headers.len() + body_bytes.len(),
        );
        let path_offset = 0u32;
        let path_len = path.len() as u32;
        slab.extend_from_slice(path.as_bytes());
        let query_offset = slab.len() as u32;
        let query_len = query_string.len() as u32;
        slab.extend_from_slice(query_string.as_bytes());
        let headers_offset = slab.len() as u32;
        let headers_len = raw_headers.len() as u32;
        slab.extend_from_slice(&raw_headers);
        let body_offset = slab.len() as u32;
        let body_len = body_bytes.len() as u32;
        slab.extend_from_slice(body_bytes);

        let node = TransportNode {
            method_tag,
            http_version_tag: 1,
            _pad: [0; 2],
            handler_id: 0,
            correlation_id: 0,
            path_offset,
            path_len,
            query_offset,
            query_len,
            headers_offset,
            headers_len,
            body_offset,
            body_len,
        };
        let snapshot = Arc::new(BatchSnapshot::new(slab, vec![node]));
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

    pub fn __repr__(&self) -> String {
        format!(
            "WsgiEnviron(method={:?}, path={:?})",
            self.batch.node(self.index).method_name(),
            self.batch.path(self.index)
        )
    }
}

/// Iterator over the environ's live keys.
#[pyclass(name = "WsgiEnvironIterator", freelist = 128)]
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
