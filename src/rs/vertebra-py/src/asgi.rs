//! Lazy ASGI 3.0 scope with a 20-slot inline key table (`0..14` ASGI + `14..20` Litestar v2/v3).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pyo3::exceptions::{
    PyIOError, PyIndexError, PyKeyError, PyRuntimeError, PyStopIteration, PyTypeError,
};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList, PyString, PyTuple};

use vertebra_core::transport::{BatchSnapshot, decode_headers_raw, encode_headers_compact};

use crate::file_io::read_file_range_sync;
use crate::headers::decode_asgi_headers;
use crate::request::{Request, RequestBatch};
use crate::response::Response;
use crate::synthetic::SyntheticRequest;
use crate::transport::{ResponseSubmitter, with_buffer_slice};

/// 24 inline ASGI scope slots:
/// - `0..14`: standard ASGI 3.0 HTTP connection scope keys
/// - `14..20`: Litestar v2 + v3 hot-path scope keys (`app`, `litestar_app`, `path_params`, `route_handler`, `path_template`, `connection`)
/// - `20..24`: Starlette + FastAPI hot-path scope keys (`router`, `endpoint`, `route`, `vertebra_request`)
pub const KNOWN_KEYS: [&str; 24] = [
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
    "router",
    "endpoint",
    "route",
    "vertebra_request",
];

const IDX_STATE: usize = 12;
const IDX_EXTENSIONS: usize = 13;
/// Slots `14..24` begin unset until populated by framework routing or middleware.
const INITIAL_UNSET_MASK: u32 = 0b11_1111_1111 << 14;

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
        "router" => Some(20),
        "endpoint" => Some(21),
        "route" => Some(22),
        "vertebra_request" => Some(23),
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
        20 => pyo3::intern!(py, "router"),
        21 => pyo3::intern!(py, "endpoint"),
        22 => pyo3::intern!(py, "route"),
        23 => pyo3::intern!(py, "vertebra_request"),
        _ => unreachable!(),
    }
}

static ASGI_VERSION_DICT: pyo3::sync::PyOnceLock<Py<PyDict>> = pyo3::sync::PyOnceLock::new();
static PLAINTEXT_EXTENSIONS: pyo3::sync::PyOnceLock<Py<PyDict>> = pyo3::sync::PyOnceLock::new();
static TLS_EXTENSIONS: pyo3::sync::PyOnceLock<Py<PyDict>> = pyo3::sync::PyOnceLock::new();

#[inline]
pub(crate) fn mark_immortal_if_supported(obj: &Bound<'_, PyAny>) {
    #[cfg(Py_3_15)]
    unsafe {
        let _ = pyo3::ffi::PyUnstable_SetImmortal(obj.as_ptr());
    }
    #[cfg(not(Py_3_15))]
    let _ = obj;
}

fn build_asgi_version_dict<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let dict_ref = ASGI_VERSION_DICT.get_or_try_init(py, || -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item(pyo3::intern!(py, "version"), pyo3::intern!(py, "3.0"))?;
        d.set_item(pyo3::intern!(py, "spec_version"), pyo3::intern!(py, "2.4"))?;
        mark_immortal_if_supported(d.as_any());
        Ok(d.unbind())
    })?;
    dict_ref.bind(py).copy()
}

fn build_empty_capability_entry<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    #[cfg(Py_3_15)]
    {
        let fd = pyo3::types::PyFrozenDict::empty(py)?;
        mark_immortal_if_supported(fd.as_any());
        Ok(fd.into_any())
    }
    #[cfg(not(Py_3_15))]
    {
        Ok(PyDict::new(py).into_any())
    }
}

fn build_extensions_dict<'py>(py: Python<'py>, is_tls: bool) -> PyResult<Bound<'py, PyDict>> {
    let cell = if is_tls {
        &TLS_EXTENSIONS
    } else {
        &PLAINTEXT_EXTENSIONS
    };
    let dict_ref = cell.get_or_try_init(py, || -> PyResult<Py<PyDict>> {
        let ext = PyDict::new(py);
        let empty = build_empty_capability_entry(py)?;
        ext.set_item(pyo3::intern!(py, "http.response.pathsend"), &empty)?;
        ext.set_item(pyo3::intern!(py, "http.response.zerocopysend"), &empty)?;
        ext.set_item(pyo3::intern!(py, "http.response.trailers"), &empty)?;
        if is_tls {
            ext.set_item(pyo3::intern!(py, "tls"), &empty)?;
        }
        mark_immortal_if_supported(ext.as_any());
        Ok(ext.unbind())
    })?;
    dict_ref.bind(py).copy()
}

/// Lazy ASGI 3.0 scope with a 20-slot inline cache and freelist pooling.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(mapping, name = "AsgiScope", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(mapping, name = "AsgiScope", immutable_type))]
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
    pub(crate) fn set_initial_state(&mut self, py: Python<'_>, state_dict: Bound<'_, PyDict>) {
        let stored = state_dict.unbind();
        self.cached[IDX_STATE] = Some(stored.clone_ref(py).into_any());
        self.state_cache = Some(stored);
    }

    #[inline]
    fn is_known_deleted(&self, idx: usize) -> bool {
        (self.deleted_known & (1u32 << idx)) != 0
    }

    fn collect_keys(&self) -> Vec<String> {
        let extra_len = self.extras.as_ref().map_or(0, HashMap::len);
        let mut keys = Vec::with_capacity(KNOWN_KEYS.len() + extra_len);
        for (idx, name) in KNOWN_KEYS.iter().enumerate() {
            if !self.is_known_deleted(idx) {
                keys.push((*name).to_string());
            }
        }
        if let Some(ref extras) = self.extras {
            for k in extras.keys() {
                keys.push(k.clone());
            }
        }
        keys
    }

    fn build_known(&self, py: Python<'_>, idx: usize) -> PyResult<Py<PyAny>> {
        let node = self.batch.node(self.index);
        let val: Py<PyAny> = match idx {
            0 => pyo3::intern!(py, "http").clone().into_any().unbind(),
            1 => build_asgi_version_dict(py)?.into_any().unbind(),
            2 => crate::request::intern_http_version(py, node.http_version())
                .into_any()
                .unbind(),
            3 => crate::request::intern_http_method(py, node.method_name())
                .into_any()
                .unbind(),
            4 => {
                let is_tls = (self.batch.flags() & 1) != 0;
                if is_tls {
                    pyo3::intern!(py, "https").clone().into_any().unbind()
                } else {
                    pyo3::intern!(py, "http").clone().into_any().unbind()
                }
            }
            5 => {
                let full_path = self.batch.path(self.index);
                let stripped =
                    if !self.root_path.is_empty() && full_path.starts_with(&self.root_path) {
                        let rem = &full_path[self.root_path.len()..];
                        if rem.is_empty() { "/" } else { rem }
                    } else {
                        full_path
                    };
                PyString::new(py, stripped).into_any().unbind()
            }
            6 => {
                let full_path = self.batch.path(self.index);
                let stripped =
                    if !self.root_path.is_empty() && full_path.starts_with(&self.root_path) {
                        let rem = &full_path[self.root_path.len()..];
                        if rem.is_empty() { "/" } else { rem }
                    } else {
                        full_path
                    };
                PyBytes::new(py, stripped.as_bytes()).into_any().unbind()
            }
            7 => {
                let q = self.batch.query(self.index).unwrap_or("");
                PyBytes::new(py, q.as_bytes()).into_any().unbind()
            }
            8 => PyString::new(py, &self.root_path).into_any().unbind(),
            9 => {
                let raw = self.batch.headers_raw(self.index);
                decode_asgi_headers(py, raw)?.into_any().unbind()
            }
            10 => {
                let t = PyTuple::new(
                    py,
                    [
                        pyo3::intern!(py, "127.0.0.1").clone().into_any(),
                        8000i32.into_pyobject(py)?.into_any(),
                    ],
                )?;
                t.into_any().unbind()
            }
            11 => {
                let t = PyTuple::new(
                    py,
                    [
                        pyo3::intern!(py, "127.0.0.1").clone().into_any(),
                        0i32.into_pyobject(py)?.into_any(),
                    ],
                )?;
                t.into_any().unbind()
            }
            12 => PyDict::new(py).into_any().unbind(),
            13 => {
                let is_tls = if let Some(ref c) = self.cached[4] {
                    c.bind(py)
                        .extract::<&str>()
                        .map(|s| s == "https" || s == "wss")
                        .unwrap_or(false)
                } else {
                    (self.batch.flags() & 1) != 0
                };
                build_extensions_dict(py, is_tls)?.into_any().unbind()
            }
            _ => {
                return Err(PyKeyError::new_err(KNOWN_KEYS[idx].to_string()));
            }
        };
        Ok(val)
    }
}

#[pymethods]
impl AsgiScope {
    #[new]
    #[pyo3(signature = (root_path=None))]
    pub fn py_new(root_path: Option<String>) -> Self {
        let snapshot = SyntheticRequest::build("GET", "/", None, b"", b"", 0, 0, "1.1");
        Self::new(snapshot, 0, root_path.unwrap_or_default())
    }

    /// Construct a standalone `AsgiScope` directly from HTTP components for unit tests and direct dispatch.
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, method="GET", path="/", query_string="", headers=None, root_path="", http_version="1.1", scheme="http", body=None))]
    pub fn from_parts(
        py: Python<'_>,
        method: &str,
        path: &str,
        query_string: &str,
        headers: Option<Vec<(Vec<u8>, Vec<u8>)>>,
        root_path: &str,
        http_version: &str,
        scheme: &str,
        body: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let raw_headers = encode_headers_compact(&headers.unwrap_or_default());
        let body_bytes = match body {
            Some(b) if !b.is_none() => crate::response::extract_buffer_bytes(py, b)?,
            _ => Bytes::new(),
        };
        let snapshot = SyntheticRequest::build(
            method,
            path,
            Some(query_string),
            &raw_headers,
            &body_bytes,
            0,
            0,
            http_version,
        );
        let mut scope = Self::new(snapshot, 0, root_path.to_string());
        scope.cached[4] = Some(PyString::new(py, scheme).into_any().unbind());
        scope.cached[5] = Some(PyString::new(py, path).into_any().unbind());
        scope.cached[6] = Some(PyBytes::new(py, path.as_bytes()).into_any().unbind());
        Ok(scope)
    }

    #[staticmethod]
    pub fn known_keys() -> Vec<&'static str> {
        KNOWN_KEYS.to_vec()
    }

    /// Return a zero-object `Request` proxy backed by this scope's underlying `BatchSnapshot` slot.
    pub fn request(&self) -> Request {
        Request::new(Arc::clone(&self.batch), self.index)
    }

    /// Return the raw request body bytes directly from the underlying `BatchSnapshot` slot.
    pub fn body_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.batch.body(self.index))
    }

    /// Pre-seed `_body` on a Starlette/FastAPI `Request` instance via `PyObject_GenericSetAttr`.
    #[pyo3(signature = (request, body=None))]
    pub fn preseed_request_body(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        body: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let body_obj: Bound<'_, PyAny> = match body {
            Some(b) if !b.is_none() => b.clone(),
            _ => PyBytes::new(py, self.batch.body(self.index)).into_any(),
        };
        let attr_name = pyo3::intern!(py, "_body");
        let rc = unsafe {
            ffi::PyObject_GenericSetAttr(request.as_ptr(), attr_name.as_ptr(), body_obj.as_ptr())
        };
        if rc < 0 {
            unsafe { ffi::PyErr_Clear() };
            request.setattr(attr_name, body_obj)?;
        }
        Ok(())
    }

    fn __getitem__(&mut self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        if let Some(idx) = known_key_index(key) {
            if self.is_known_deleted(idx) {
                return Err(PyKeyError::new_err(key.to_string()));
            }
            if idx == IDX_STATE {
                if let Some(ref d) = self.state_cache {
                    return Ok(d.clone_ref(py).into_any());
                }
                if let Some(ref c) = self.cached[idx] {
                    return Ok(c.clone_ref(py));
                }
                let val = self.build_known(py, idx)?;
                if let Ok(d) = val.bind(py).clone().cast_into::<PyDict>() {
                    self.state_cache = Some(d.unbind());
                }
                self.cached[idx] = Some(val.clone_ref(py));
                return Ok(val);
            }
            if let Some(ref c) = self.cached[idx] {
                return Ok(c.clone_ref(py));
            }
            let val = self.build_known(py, idx)?;
            self.cached[idx] = Some(val.clone_ref(py));
            return Ok(val);
        }
        if let Some(ref extras) = self.extras
            && let Some(val) = extras.get(key)
        {
            return Ok(val.clone_ref(py));
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __setitem__(&mut self, py: Python<'_>, key: String, value: Py<PyAny>) {
        if let Some(idx) = known_key_index(&key) {
            self.deleted_known &= !(1u32 << idx);
            if idx == IDX_STATE {
                let bound = value.bind(py);
                if let Ok(dict) = bound.cast::<PyDict>() {
                    self.state_cache = Some(dict.clone().unbind());
                } else {
                    self.state_cache = None;
                }
            }
            self.cached[idx] = Some(value);
            return;
        }
        let extras = self.extras.get_or_insert_with(HashMap::new);
        extras.insert(key, value);
    }

    fn __delitem__(&mut self, _py: Python<'_>, key: &str) -> PyResult<()> {
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

    /// Pop a key from the scope, returning its value and deleting it.
    #[pyo3(signature = (key, default=None))]
    pub fn pop(
        &mut self,
        py: Python<'_>,
        key: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        match self.__getitem__(py, key) {
            Ok(v) => {
                let _ = self.__delitem__(py, key);
                Ok(v)
            }
            Err(_) => match default {
                Some(d) => Ok(d),
                None => Err(PyKeyError::new_err(key.to_string())),
            },
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
        if key == "extensions" {
            if !self.is_known_deleted(IDX_EXTENSIONS)
                && let Some(ref existing) = self.cached[IDX_EXTENSIONS]
            {
                return Ok(existing.clone_ref(py));
            }
            if let Some(def_val) = default {
                let clone = def_val.clone_ref(py);
                self.deleted_known &= !(1u32 << IDX_EXTENSIONS);
                self.cached[IDX_EXTENSIONS] = Some(def_val);
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

    fn __repr__(&self) -> String {
        format!(
            "AsgiScope(path={:?}, index={})",
            self.batch.path(self.index),
            self.index
        )
    }
}

/// Iterator over key strings in an `AsgiScope`.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "AsgiScopeIterator", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "AsgiScopeIterator", immutable_type))]
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
            return Some(PyString::new(py, k));
        }
        None
    }
}

pub(crate) const ERROR_500_HEADERS_RAW: &[u8] = b"\x0ccontent-type\x0a\x00text/plain";
pub(crate) const ERROR_500_BODY: &[u8] = b"Internal Server Error";

#[inline]
pub(crate) fn push_compact_header(buf: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    let name_len = name.len().min(u8::MAX as usize);
    let value_len = value.len().min(u16::MAX as usize);
    buf.push(name_len as u8);
    buf.extend_from_slice(&name[..name_len]);
    buf.extend_from_slice(&(value_len as u16).to_le_bytes());
    buf.extend_from_slice(&value[..value_len]);
}

/// Immediately completed zero-suspension awaitable resolving to `None`.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "PyEmptyAwaitable", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(name = "PyEmptyAwaitable", frozen, immutable_type)
)]
pub struct PyEmptyAwaitable;

#[pymethods]
impl PyEmptyAwaitable {
    #[new]
    pub fn py_new() -> Self {
        Self
    }

    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Err(PyStopIteration::new_err(py.None()))
    }
}

static EMPTY_AWAITABLE: pyo3::sync::PyOnceLock<Py<PyEmptyAwaitable>> =
    pyo3::sync::PyOnceLock::new();

#[inline]
pub fn get_empty_awaitable(py: Python<'_>) -> PyResult<Py<PyEmptyAwaitable>> {
    EMPTY_AWAITABLE
        .get_or_try_init(py, || {
            let empty = Py::new(py, PyEmptyAwaitable)?;
            mark_immortal_if_supported(empty.bind(py).as_any());
            Ok(empty)
        })
        .map(|p| p.clone_ref(py))
}

/// Immediately completed zero-suspension awaitable resolving to a stored Python value.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "PyValueAwaitable", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(name = "PyValueAwaitable", frozen, immutable_type)
)]
pub struct PyValueAwaitable {
    value: Py<PyAny>,
}

impl PyValueAwaitable {
    #[inline]
    pub fn with_value(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<Self>> {
        Py::new(py, Self { value })
    }
}

#[pymethods]
impl PyValueAwaitable {
    #[new]
    #[pyo3(signature = (value=None))]
    pub fn py_new(py: Python<'_>, value: Option<Py<PyAny>>) -> Self {
        Self {
            value: value.unwrap_or_else(|| py.None()),
        }
    }

    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Err(PyStopIteration::new_err(self.value.clone_ref(py)))
    }
}

static EMPTY_HTTP_REQUEST_DICT: pyo3::sync::PyOnceLock<Py<PyDict>> = pyo3::sync::PyOnceLock::new();
static HTTP_DISCONNECT_DICT: pyo3::sync::PyOnceLock<Py<PyDict>> = pyo3::sync::PyOnceLock::new();

fn build_http_request_dict<'py>(py: Python<'py>, body: &[u8]) -> PyResult<Bound<'py, PyDict>> {
    if body.is_empty() {
        let tmpl = EMPTY_HTTP_REQUEST_DICT.get_or_try_init(py, || -> PyResult<Py<PyDict>> {
            let d = PyDict::new(py);
            d.set_item(pyo3::intern!(py, "type"), pyo3::intern!(py, "http.request"))?;
            d.set_item(pyo3::intern!(py, "body"), PyBytes::new(py, b""))?;
            d.set_item(pyo3::intern!(py, "more_body"), PyBool::new(py, false))?;
            mark_immortal_if_supported(d.as_any());
            Ok(d.unbind())
        })?;
        return tmpl.bind(py).copy();
    }
    let d = PyDict::new(py);
    d.set_item(pyo3::intern!(py, "type"), pyo3::intern!(py, "http.request"))?;
    d.set_item(pyo3::intern!(py, "body"), PyBytes::new(py, body))?;
    d.set_item(pyo3::intern!(py, "more_body"), PyBool::new(py, false))?;
    Ok(d)
}

fn build_http_disconnect_dict<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let tmpl = HTTP_DISCONNECT_DICT.get_or_try_init(py, || -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item(
            pyo3::intern!(py, "type"),
            pyo3::intern!(py, "http.disconnect"),
        )?;
        mark_immortal_if_supported(d.as_any());
        Ok(d.unbind())
    })?;
    tmpl.bind(py).copy()
}

pub(crate) enum AsgiBodySource {
    Snapshot {
        batch: Arc<BatchSnapshot>,
        index: usize,
    },
    Owned(Bytes),
}

impl AsgiBodySource {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Snapshot { batch, index } => batch.body(*index),
            Self::Owned(b) => b.as_ref(),
        }
    }
}

#[derive(Default)]
struct AsgiProtocolState {
    body_received: bool,
    started: bool,
    complete: bool,
    submitted: bool,
    immediate_submit: bool,
    expects_trailers: bool,
    trailers_done: bool,
    has_content_length: bool,
    status: u16,
    headers_raw: Vec<u8>,
    single_body_py: Option<Py<PyBytes>>,
    body_buf: Vec<u8>,
    disconnect_future: Option<Py<PyAny>>,
    loop_obj: Option<Py<PyAny>>,
}

#[inline]
fn extract_header_component_bytes<R>(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    f: impl FnOnce(&[u8]) -> PyResult<R>,
) -> PyResult<R> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        return f(b.as_bytes());
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return f(s.to_str()?.as_bytes());
    }
    with_buffer_slice(py, obj, f)
}

#[inline]
fn encode_header_pair(
    py: Python<'_>,
    pair: &Bound<'_, PyAny>,
    out: &mut Vec<u8>,
    has_content_length: &mut bool,
) -> PyResult<()> {
    let (k, v) = if let Ok(tup) = pair.cast::<PyTuple>() {
        if tup.len() != 2 {
            return Err(PyTypeError::new_err("header pair must have 2 elements"));
        }
        (tup.get_item(0)?, tup.get_item(1)?)
    } else if let Ok(lst) = pair.cast::<PyList>() {
        if lst.len() != 2 {
            return Err(PyTypeError::new_err("header pair must have 2 elements"));
        }
        (lst.get_item(0)?, lst.get_item(1)?)
    } else {
        (pair.get_item(0)?, pair.get_item(1)?)
    };
    extract_header_component_bytes(py, &k, |kb| {
        if kb.eq_ignore_ascii_case(b"content-length") {
            *has_content_length = true;
        }
        extract_header_component_bytes(py, &v, |vb| {
            push_compact_header(out, kb, vb);
            Ok(())
        })
    })
}

pub(crate) fn encode_py_headers_into(
    py: Python<'_>,
    headers_obj: &Bound<'_, PyAny>,
    out: &mut Vec<u8>,
    has_content_length: &mut bool,
) -> PyResult<()> {
    if headers_obj.is_none() {
        return Ok(());
    }
    if let Ok(list) = headers_obj.cast::<PyList>() {
        if out.capacity() == 0 && !list.is_empty() {
            out.reserve(list.len() * 32);
        }
        for item in list.iter() {
            encode_header_pair(py, &item, out, has_content_length)?;
        }
        return Ok(());
    }
    if let Ok(tup) = headers_obj.cast::<PyTuple>() {
        if out.capacity() == 0 && !tup.is_empty() {
            out.reserve(tup.len() * 32);
        }
        for item in tup.iter() {
            encode_header_pair(py, &item, out, has_content_length)?;
        }
        return Ok(());
    }
    for item in headers_obj.try_iter()? {
        let item = item?;
        encode_header_pair(py, &item, out, has_content_length)?;
    }
    Ok(())
}

fn encode_sse_message_into(message: &Bound<'_, PyDict>, out: &mut Vec<u8>) -> PyResult<()> {
    let data_opt = message.get_item("data")?;
    let event_opt = message.get_item("event")?;
    let id_opt = message.get_item("id")?;
    let retry_opt = message.get_item("retry")?;
    let comment_opt = message.get_item("comment")?;

    let data_str = match data_opt {
        Some(ref v) if !v.is_none() => v.str()?.to_str()?.to_owned(),
        _ => String::new(),
    };
    let has_event = event_opt.as_ref().is_some_and(|v| !v.is_none());
    let has_id = id_opt.as_ref().is_some_and(|v| !v.is_none());
    let has_retry = retry_opt.as_ref().is_some_and(|v| !v.is_none());
    let has_comment = comment_opt.as_ref().is_some_and(|v| !v.is_none());

    if data_str.is_empty() && !has_event && !has_id && !has_retry && !has_comment {
        return Ok(());
    }

    if let Some(ref c) = comment_opt
        && !c.is_none()
    {
        let c_str = c.str()?;
        let s = c_str.to_str()?;
        if s.is_empty() {
            out.extend_from_slice(b": \r\n");
        } else {
            for line in s.lines() {
                out.extend_from_slice(b": ");
                out.extend_from_slice(line.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
        }
    }
    if let Some(ref id_v) = id_opt
        && !id_v.is_none()
    {
        let id_s = id_v.str()?;
        let clean: String = id_s
            .to_str()?
            .chars()
            .filter(|&ch| ch != '\r' && ch != '\n' && ch != '\0')
            .collect();
        if !clean.is_empty() {
            out.extend_from_slice(b"id: ");
            out.extend_from_slice(clean.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
    if let Some(ref ev_v) = event_opt
        && !ev_v.is_none()
    {
        let ev_s = ev_v.str()?;
        let clean: String = ev_s
            .to_str()?
            .chars()
            .filter(|&ch| ch != '\r' && ch != '\n' && ch != '\0')
            .collect();
        if !clean.is_empty() {
            out.extend_from_slice(b"event: ");
            out.extend_from_slice(clean.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
    if let Some(ref r_v) = retry_opt
        && let Ok(retry_num) = r_v.extract::<i64>()
        && retry_num >= 0
    {
        out.extend_from_slice(b"retry: ");
        out.extend_from_slice(retry_num.to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if data_str.is_empty() {
        out.extend_from_slice(b"data: \r\n");
    } else {
        for line in data_str.lines() {
            out.extend_from_slice(b"data: ");
            out.extend_from_slice(line.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
    out.extend_from_slice(b"\r\n");
    Ok(())
}

/// Rust-native ASGI 3.0 HTTP protocol state machine (`receive`, `send`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "AsgiProtocol", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(name = "AsgiProtocol", frozen, immutable_type)
)]
pub struct AsgiProtocol {
    body_source: AsgiBodySource,
    request_id: u64,
    direct_submit: bool,
    state: Mutex<AsgiProtocolState>,
    disconnected: AtomicBool,
}

pub(crate) enum PreparedRawBody {
    Empty,
    PyBytes(Py<PyBytes>),
    Owned(Vec<u8>),
}

impl PreparedRawBody {
    #[inline]
    pub(crate) fn as_slice<'a>(&'a self, py: Python<'a>) -> &'a [u8] {
        match self {
            Self::Empty => b"",
            Self::PyBytes(b) => b.bind(py).as_bytes(),
            Self::Owned(v) => v.as_slice(),
        }
    }
}

pub(crate) struct FlushedProtocolResponse {
    pub(crate) request_id: u64,
    pub(crate) status: u16,
    pub(crate) headers_raw: Vec<u8>,
    pub(crate) body: PreparedRawBody,
}

impl AsgiProtocol {
    pub(crate) fn from_batch(
        batch: Arc<BatchSnapshot>,
        index: usize,
        loop_obj: Option<Py<PyAny>>,
    ) -> Self {
        let request_id = batch.node(index).correlation_id;
        Self {
            body_source: AsgiBodySource::Snapshot { batch, index },
            request_id,
            direct_submit: true,
            state: Mutex::new(AsgiProtocolState {
                status: 200,
                immediate_submit: false,
                loop_obj,
                ..AsgiProtocolState::default()
            }),
            disconnected: AtomicBool::new(false),
        }
    }

    #[inline]
    fn materialize_single_into_buf(py: Python<'_>, st: &mut AsgiProtocolState) {
        if let Some(prev_py) = st.single_body_py.take() {
            let bytes_slice = prev_py.bind(py).as_bytes();
            st.body_buf.extend_from_slice(bytes_slice);
        }
    }

    #[inline]
    fn extract_prepared_from_guard(
        request_id: u64,
        st: &mut AsgiProtocolState,
    ) -> FlushedProtocolResponse {
        st.submitted = true;
        let body = if let Some(py_b) = st.single_body_py.take() {
            PreparedRawBody::PyBytes(py_b)
        } else if !st.body_buf.is_empty() {
            PreparedRawBody::Owned(std::mem::take(&mut st.body_buf))
        } else {
            PreparedRawBody::Empty
        };
        FlushedProtocolResponse {
            request_id,
            status: st.status,
            headers_raw: std::mem::take(&mut st.headers_raw),
            body,
        }
    }

    pub(crate) fn take_completed_for_batch(
        &self,
        is_return: bool,
    ) -> Option<FlushedProtocolResponse> {
        let mut st = self.state.lock().ok()?;
        if st.submitted {
            return None;
        }
        let ready = if is_return {
            true
        } else {
            st.complete && (!st.expects_trailers || st.trailers_done)
        };
        if !ready {
            st.immediate_submit = true;
            return None;
        }
        if !st.started {
            st.submitted = true;
            return Some(FlushedProtocolResponse {
                request_id: self.request_id,
                status: 500,
                headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
                body: PreparedRawBody::Owned(ERROR_500_BODY.to_vec()),
            });
        }
        Some(Self::extract_prepared_from_guard(self.request_id, &mut st))
    }

    pub(crate) fn take_error_for_batch(&self) -> Option<FlushedProtocolResponse> {
        let mut st = self.state.lock().ok()?;
        if st.submitted {
            return None;
        }
        st.submitted = true;
        Some(FlushedProtocolResponse {
            request_id: self.request_id,
            status: 500,
            headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
            body: PreparedRawBody::Owned(ERROR_500_BODY.to_vec()),
        })
    }

    pub(crate) fn finalize_async_return(&self, py: Python<'_>) -> PyResult<()> {
        let maybe_resp = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("ASGI state lock poisoned"))?;
            if st.submitted || !self.direct_submit {
                None
            } else if !st.started {
                st.submitted = true;
                Some(FlushedProtocolResponse {
                    request_id: self.request_id,
                    status: 500,
                    headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
                    body: PreparedRawBody::Owned(ERROR_500_BODY.to_vec()),
                })
            } else {
                Some(Self::extract_prepared_from_guard(self.request_id, &mut st))
            }
        };
        if let Some(flushed) = maybe_resp {
            ResponseSubmitter::submit_raw_inner(
                py,
                flushed.request_id,
                flushed.status,
                &flushed.headers_raw,
                flushed.body.as_slice(py),
            )?;
        }
        Ok(())
    }

    pub(crate) fn finalize_async_error(&self, py: Python<'_>) -> PyResult<()> {
        let should_send_500 = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("ASGI state lock poisoned"))?;
            if st.submitted || !self.direct_submit {
                false
            } else {
                st.submitted = true;
                true
            }
        };
        if should_send_500 {
            ResponseSubmitter::submit_raw_inner(
                py,
                self.request_id,
                500,
                ERROR_500_HEADERS_RAW,
                ERROR_500_BODY,
            )?;
        }
        Ok(())
    }
}

#[pymethods]
impl AsgiProtocol {
    #[new]
    #[pyo3(signature = (body=None, request_id=0, *, direct_submit=false, loop_obj=None))]
    pub fn py_new(
        py: Python<'_>,
        body: Option<&Bound<'_, PyAny>>,
        request_id: u64,
        direct_submit: bool,
        loop_obj: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let owned = match body {
            Some(b) if !b.is_none() => crate::response::extract_buffer_bytes(py, b)?,
            _ => Bytes::new(),
        };
        Ok(Self {
            body_source: AsgiBodySource::Owned(owned),
            request_id,
            direct_submit,
            state: Mutex::new(AsgiProtocolState {
                status: 200,
                immediate_submit: direct_submit,
                loop_obj,
                ..AsgiProtocolState::default()
            }),
            disconnected: AtomicBool::new(false),
        })
    }

    pub fn receive(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("ASGI state lock poisoned"))?;
        if !st.body_received {
            st.body_received = true;
            drop(st);
            let msg = build_http_request_dict(py, self.body_source.as_slice())?;
            return Ok(PyValueAwaitable::with_value(py, msg.into_any().unbind())?.into_any());
        }

        if self.disconnected.load(Ordering::Acquire) {
            drop(st);
            let disc = build_http_disconnect_dict(py)?;
            return Ok(PyValueAwaitable::with_value(py, disc.into_any().unbind())?.into_any());
        }

        if let Some(ref fut) = st.disconnect_future {
            return Ok(fut.clone_ref(py));
        }

        let fut = if let Some(ref lp) = st.loop_obj {
            lp.bind(py)
                .call_method0(pyo3::intern!(py, "create_future"))?
        } else {
            let asyncio = py.import(pyo3::intern!(py, "asyncio"))?;
            let lp = asyncio.call_method0(pyo3::intern!(py, "get_running_loop"))?;
            lp.call_method0(pyo3::intern!(py, "create_future"))?
        };
        let fut_py = fut.unbind();
        st.disconnect_future = Some(fut_py.clone_ref(py));
        Ok(fut_py)
    }

    pub fn body_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.body_source.as_slice())
    }

    pub fn __call__(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyEmptyAwaitable>> {
        self.send(py, message)
    }

    #[pyo3(signature = (status, headers=None, body=None))]
    pub fn send_raw_response(
        &self,
        py: Python<'_>,
        status: u16,
        headers: Option<&Bound<'_, PyAny>>,
        body: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyEmptyAwaitable>> {
        let mut immediate_flush: Option<FlushedProtocolResponse> = None;
        {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("ASGI state lock poisoned"))?;
            if st.started {
                return Err(PyRuntimeError::new_err(
                    "Duplicate 'http.response.start' message",
                ));
            }
            st.started = true;
            st.complete = true;
            st.status = status;
            if let Some(hdrs) = headers {
                let mut has_cl = st.has_content_length;
                encode_py_headers_into(py, hdrs, &mut st.headers_raw, &mut has_cl)?;
                st.has_content_length = has_cl;
            }
            if let Some(body_obj) = body
                && !body_obj.is_none()
            {
                if let Ok(py_b) = body_obj.cast::<PyBytes>() {
                    if !py_b.as_bytes().is_empty() {
                        st.single_body_py = Some(py_b.clone().unbind());
                    }
                } else {
                    with_buffer_slice(py, body_obj, |slice| {
                        if !slice.is_empty() {
                            st.body_buf.extend_from_slice(slice);
                        }
                        Ok(())
                    })?;
                }
            }
            if self.direct_submit && st.immediate_submit && !st.submitted {
                immediate_flush =
                    Some(Self::extract_prepared_from_guard(self.request_id, &mut st));
            }
        }
        if let Some(flushed) = immediate_flush {
            ResponseSubmitter::submit_raw_inner(
                py,
                flushed.request_id,
                flushed.status,
                &flushed.headers_raw,
                flushed.body.as_slice(py),
            )?;
        }
        get_empty_awaitable(py)
    }

    pub fn send(
        &self,
        py: Python<'_>,
        message: &Bound<'_, PyDict>,
    ) -> PyResult<Py<PyEmptyAwaitable>> {
        let type_obj = message
            .get_item(pyo3::intern!(py, "type"))?
            .ok_or_else(|| PyKeyError::new_err("type"))?;
        let msg_type = type_obj.cast::<PyString>()?.to_str()?;

        let mut immediate_flush: Option<FlushedProtocolResponse> = None;
        {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("ASGI state lock poisoned"))?;

            match msg_type {
                "http.response.start" => {
                    if st.started {
                        return Err(PyRuntimeError::new_err(
                            "Duplicate 'http.response.start' message",
                        ));
                    }
                    st.started = true;
                    let status_obj = message
                        .get_item(pyo3::intern!(py, "status"))?
                        .ok_or_else(|| PyKeyError::new_err("status"))?;
                    st.status = status_obj.extract::<u16>()?;
                    if let Some(tr) = message.get_item(pyo3::intern!(py, "trailers"))? {
                        st.expects_trailers = tr.is_truthy()?;
                    }
                    if let Some(hdrs) = message.get_item(pyo3::intern!(py, "headers"))? {
                        let mut has_cl = st.has_content_length;
                        encode_py_headers_into(py, &hdrs, &mut st.headers_raw, &mut has_cl)?;
                        st.has_content_length = has_cl;
                    }
                }
                "http.response.body" => {
                    if !st.started {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.body' before 'http.response.start'",
                        ));
                    }
                    if st.complete {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.body' after response was already complete",
                        ));
                    }
                    let more_body = match message.get_item(pyo3::intern!(py, "more_body"))? {
                        Some(v) => v.is_truthy()?,
                        None => false,
                    };
                    if let Some(body_obj) = message.get_item(pyo3::intern!(py, "body"))?
                        && !body_obj.is_none()
                    {
                        if !more_body && st.body_buf.is_empty() && st.single_body_py.is_none() {
                            if let Ok(py_b) = body_obj.cast::<PyBytes>() {
                                if !py_b.as_bytes().is_empty() {
                                    st.single_body_py = Some(py_b.clone().unbind());
                                }
                            } else {
                                with_buffer_slice(py, &body_obj, |slice| {
                                    if !slice.is_empty() {
                                        st.body_buf.extend_from_slice(slice);
                                    }
                                    Ok(())
                                })?;
                            }
                        } else {
                            Self::materialize_single_into_buf(py, &mut st);
                            with_buffer_slice(py, &body_obj, |slice| {
                                if !slice.is_empty() {
                                    st.body_buf.extend_from_slice(slice);
                                }
                                Ok(())
                            })?;
                        }
                    }
                    if !more_body {
                        st.complete = true;
                        if self.direct_submit
                            && st.immediate_submit
                            && !st.submitted
                            && (!st.expects_trailers || st.trailers_done)
                        {
                            immediate_flush =
                                Some(Self::extract_prepared_from_guard(self.request_id, &mut st));
                        }
                    }
                }
                "http.response.zerocopysend" => {
                    if !st.started {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.zerocopysend' before 'http.response.start'",
                        ));
                    }
                    if st.complete {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.zerocopysend' after response was already complete",
                        ));
                    }
                    let more_body = match message.get_item(pyo3::intern!(py, "more_body"))? {
                        Some(v) => v.is_truthy()?,
                        None => false,
                    };
                    if let Some(buf_obj) = message.get_item(pyo3::intern!(py, "buffer"))?
                        && !buf_obj.is_none()
                    {
                        Self::materialize_single_into_buf(py, &mut st);
                        with_buffer_slice(py, &buf_obj, |slice| {
                            if !slice.is_empty() {
                                st.body_buf.extend_from_slice(slice);
                            }
                            Ok(())
                        })?;
                    }
                    if !more_body {
                        st.complete = true;
                        if self.direct_submit
                            && st.immediate_submit
                            && !st.submitted
                            && (!st.expects_trailers || st.trailers_done)
                        {
                            immediate_flush =
                                Some(Self::extract_prepared_from_guard(self.request_id, &mut st));
                        }
                    }
                }
                "http.response.pathsend" => {
                    if !st.started {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.pathsend' before 'http.response.start'",
                        ));
                    }
                    if st.complete {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.pathsend' after response was already complete",
                        ));
                    }
                    let path_obj = message
                        .get_item(pyo3::intern!(py, "path"))?
                        .ok_or_else(|| PyKeyError::new_err("path"))?;
                    let path_buf: std::path::PathBuf = path_obj.extract()?;
                    let path_str = path_buf.to_string_lossy().into_owned();
                    drop(st);
                    let path_clone = path_str.clone();
                    let file_bytes = py
                        .detach(|| read_file_range_sync(&path_clone, None, None))
                        .map_err(|e| {
                            PyIOError::new_err(format!("failed to read file {path_str}: {e}"))
                        })?;

                    let mut st_after = self
                        .state
                        .lock()
                        .map_err(|_| PyRuntimeError::new_err("ASGI state lock poisoned"))?;
                    Self::materialize_single_into_buf(py, &mut st_after);
                    if !st_after.has_content_length {
                        let cl_val = file_bytes.len().to_string();
                        push_compact_header(
                            &mut st_after.headers_raw,
                            b"content-length",
                            cl_val.as_bytes(),
                        );
                        st_after.has_content_length = true;
                    }
                    st_after.body_buf.extend_from_slice(&file_bytes);
                    st_after.complete = true;
                    if self.direct_submit
                        && st_after.immediate_submit
                        && !st_after.submitted
                        && (!st_after.expects_trailers || st_after.trailers_done)
                    {
                        immediate_flush = Some(Self::extract_prepared_from_guard(
                            self.request_id,
                            &mut st_after,
                        ));
                    }
                    drop(st_after);
                    if let Some(flushed) = immediate_flush {
                        ResponseSubmitter::submit_raw_inner(
                            py,
                            flushed.request_id,
                            flushed.status,
                            &flushed.headers_raw,
                            flushed.body.as_slice(py),
                        )?;
                    }
                    return get_empty_awaitable(py);
                }
                "http.response.trailers" => {
                    if !st.started {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.trailers' before 'http.response.start'",
                        ));
                    }
                    st.trailers_done = true;
                    if self.direct_submit && st.immediate_submit && st.complete && !st.submitted {
                        immediate_flush =
                            Some(Self::extract_prepared_from_guard(self.request_id, &mut st));
                    }
                }
                "http.response.sse" => {
                    if !st.started {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.sse' before 'http.response.start'",
                        ));
                    }
                    if st.complete {
                        return Err(PyRuntimeError::new_err(
                            "Received 'http.response.sse' after response was already complete",
                        ));
                    }
                    Self::materialize_single_into_buf(py, &mut st);
                    encode_sse_message_into(message, &mut st.body_buf)?;
                    let more_body = match message.get_item(pyo3::intern!(py, "more_body"))? {
                        Some(v) => v.is_truthy()?,
                        None => false,
                    };
                    if !more_body {
                        st.complete = true;
                        if self.direct_submit
                            && st.immediate_submit
                            && !st.submitted
                            && (!st.expects_trailers || st.trailers_done)
                        {
                            immediate_flush =
                                Some(Self::extract_prepared_from_guard(self.request_id, &mut st));
                        }
                    }
                }
                other => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Unexpected ASGI send message type: {other:?}"
                    )));
                }
            }
        }

        if let Some(flushed) = immediate_flush {
            ResponseSubmitter::submit_raw_inner(
                py,
                flushed.request_id,
                flushed.status,
                &flushed.headers_raw,
                flushed.body.as_slice(py),
            )?;
        }

        get_empty_awaitable(py)
    }

    pub fn notify_disconnect(&self, py: Python<'_>) -> PyResult<()> {
        self.disconnected.store(true, Ordering::Release);
        let maybe_fut = {
            let mut st = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("ASGI state lock poisoned"))?;
            st.disconnect_future.take()
        };
        if let Some(fut) = maybe_fut {
            let disc = build_http_disconnect_dict(py)?;
            let _ = fut
                .bind(py)
                .call_method1(pyo3::intern!(py, "set_result"), (disc,));
        }
        Ok(())
    }

    #[getter]
    pub fn started(&self) -> bool {
        self.state.lock().is_ok_and(|st| st.started)
    }

    #[getter]
    pub fn complete(&self) -> bool {
        self.state.lock().is_ok_and(|st| st.complete)
    }

    #[getter]
    pub fn status(&self) -> u16 {
        self.state.lock().map_or(200, |st| st.status)
    }

    pub fn take_response(&self, py: Python<'_>) -> PyResult<Response> {
        let mut st = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("ASGI state lock poisoned"))?;
        if !st.started {
            return Err(PyRuntimeError::new_err(
                "ASGI application did not send 'http.response.start' before completing",
            ));
        }
        let body = if let Some(py_b) = st.single_body_py.take() {
            Bytes::copy_from_slice(py_b.bind(py).as_bytes())
        } else {
            Bytes::from(std::mem::take(&mut st.body_buf))
        };
        let headers = decode_headers_raw(&st.headers_raw);
        st.headers_raw.clear();
        Ok(Response {
            status: st.status,
            body,
            headers,
            request_id: self.request_id,
        })
    }
}

pub(crate) const JSON_CONTENT_TYPE_COMPACT: &[u8] = b"\x0ccontent-type\x10\x00application/json";
pub(crate) const NATIVE_ERROR_403_BODY: &[u8] = b"forbidden";
pub(crate) const NATIVE_ERROR_500_BODY: &[u8] = b"internal server error";

static ENCODE_JSON_BYTES_FN: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();

fn prepare_native_flushed_response(
    py: Python<'_>,
    request_id: u64,
    default_status: u16,
    raw: &Bound<'_, PyAny>,
) -> PyResult<FlushedProtocolResponse> {
    if let Ok(py_b) = raw.cast::<PyBytes>() {
        return Ok(FlushedProtocolResponse {
            request_id,
            status: default_status,
            headers_raw: Vec::new(),
            body: PreparedRawBody::PyBytes(py_b.clone().unbind()),
        });
    }
    if let Ok(py_s) = raw.cast::<PyString>() {
        return Ok(FlushedProtocolResponse {
            request_id,
            status: default_status,
            headers_raw: Vec::new(),
            body: PreparedRawBody::Owned(py_s.to_str()?.as_bytes().to_vec()),
        });
    }
    if let Ok(resp) = raw.cast::<Response>() {
        let r = resp.borrow();
        let resolved_id = if r.request_id == 0 {
            request_id
        } else {
            r.request_id
        };
        let headers_raw = encode_headers_compact(&r.headers);
        return Ok(FlushedProtocolResponse {
            request_id: resolved_id,
            status: r.status,
            headers_raw,
            body: PreparedRawBody::Owned(r.body.to_vec()),
        });
    }
    if raw.is_instance_of::<pyo3::types::PyByteArray>()
        || raw.is_instance_of::<pyo3::types::PyMemoryView>()
    {
        let owned = with_buffer_slice(py, raw, |s| Ok(s.to_vec()))?;
        return Ok(FlushedProtocolResponse {
            request_id,
            status: default_status,
            headers_raw: Vec::new(),
            body: PreparedRawBody::Owned(owned),
        });
    }
    let encode_fn = ENCODE_JSON_BYTES_FN.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        let serializers = py.import(pyo3::intern!(py, "vertebra.utils.serializers"))?;
        Ok(serializers
            .getattr(pyo3::intern!(py, "encode_json_bytes"))?
            .unbind())
    })?;
    let encoded_any = encode_fn.bind(py).call1((raw,))?;
    if let Ok(py_b) = encoded_any.cast::<PyBytes>() {
        return Ok(FlushedProtocolResponse {
            request_id,
            status: default_status,
            headers_raw: JSON_CONTENT_TYPE_COMPACT.to_vec(),
            body: PreparedRawBody::PyBytes(py_b.clone().unbind()),
        });
    }
    let extracted = with_buffer_slice(py, &encoded_any, |s| Ok(s.to_vec()))?;
    Ok(FlushedProtocolResponse {
        request_id,
        status: default_status,
        headers_raw: JSON_CONTENT_TYPE_COMPACT.to_vec(),
        body: PreparedRawBody::Owned(extracted),
    })
}

fn prepare_native_error_response(
    py: Python<'_>,
    request_id: u64,
    err: &PyErr,
) -> FlushedProtocolResponse {
    let is_permission_denied = err
        .get_type(py)
        .name()
        .is_ok_and(|n| n == "PermissionDeniedError");
    if is_permission_denied {
        FlushedProtocolResponse {
            request_id,
            status: 403,
            headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
            body: PreparedRawBody::Owned(NATIVE_ERROR_403_BODY.to_vec()),
        }
    } else {
        FlushedProtocolResponse {
            request_id,
            status: 500,
            headers_raw: ERROR_500_HEADERS_RAW.to_vec(),
            body: PreparedRawBody::Owned(NATIVE_ERROR_500_BODY.to_vec()),
        }
    }
}

pub(crate) enum ProtocolFinalizer {
    Asgi(Py<AsgiProtocol>),
    Rsgi(Py<crate::rsgi::http::RsgiHttpProtocol>),
    Native {
        request_id: u64,
        default_status: u16,
    },
}

impl ProtocolFinalizer {
    fn on_return(&self, py: Python<'_>, ret_val: &Bound<'_, PyAny>) -> PyResult<()> {
        match self {
            Self::Asgi(proto) => proto.bind(py).get().finalize_async_return(py),
            Self::Rsgi(proto) => proto.bind(py).get().finalize_async_return(py),
            Self::Native {
                request_id,
                default_status,
            } => {
                let flushed =
                    prepare_native_flushed_response(py, *request_id, *default_status, ret_val)?;
                ResponseSubmitter::submit_raw_inner(
                    py,
                    flushed.request_id,
                    flushed.status,
                    &flushed.headers_raw,
                    flushed.body.as_slice(py),
                )
            }
        }
    }

    fn on_error(&self, py: Python<'_>, err: &PyErr) -> PyResult<()> {
        match self {
            Self::Asgi(proto) => proto.bind(py).get().finalize_async_error(py),
            Self::Rsgi(proto) => proto.bind(py).get().finalize_async_error(py),
            Self::Native { request_id, .. } => {
                let flushed = prepare_native_error_response(py, *request_id, err);
                ResponseSubmitter::submit_raw_inner(
                    py,
                    flushed.request_id,
                    flushed.status,
                    &flushed.headers_raw,
                    flushed.body.as_slice(py),
                )
            }
        }
    }
}

pub(crate) fn log_app_exception(py: Python<'_>, protocol_label: &str, err: &PyErr) {
    if let Ok(logging) = py.import(pyo3::intern!(py, "logging"))
        && let Ok(logger) =
            logging.call_method1(pyo3::intern!(py, "getLogger"), ("vertebra.server",))
    {
        let _ = logger.call_method1(
            pyo3::intern!(py, "error"),
            (
                "Unhandled exception in %s application: %s",
                protocol_label,
                err.value(py),
            ),
        );
    }
}

/// Rust coroutine continuation resuming a coroutine that yielded on its initial `PyIter_Send`.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "_SteppedCoroutine", freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "_SteppedCoroutine", immutable_type))]
pub struct SteppedCoroutine {
    coro: Option<Py<PyAny>>,
    first_yielded: Option<Py<PyAny>>,
    finalizer: Option<ProtocolFinalizer>,
    self_task: Option<Py<PyAny>>,
    protocol_label: &'static str,
}

impl SteppedCoroutine {
    fn step_ptr(&mut self, py: Python<'_>, arg_ptr: *mut ffi::PyObject) -> PyResult<Py<PyAny>> {
        if let Some(first) = self.first_yielded.take() {
            return Ok(first);
        }
        let Some(ref coro) = self.coro else {
            return Err(PyStopIteration::new_err(py.None()));
        };
        let mut yielded: *mut ffi::PyObject = std::ptr::null_mut();
        let send_res = unsafe { ffi::PyIter_Send(coro.as_ptr(), arg_ptr, &mut yielded) };
        match send_res {
            ffi::PySendResult::PYGEN_NEXT => {
                Ok(unsafe { Bound::from_owned_ptr(py, yielded) }.unbind())
            }
            ffi::PySendResult::PYGEN_RETURN => {
                let ret_val = if yielded.is_null() {
                    py.None()
                } else {
                    unsafe { Bound::from_owned_ptr(py, yielded) }.unbind()
                };
                self.coro = None;
                self.self_task = None;
                if let Some(fin) = self.finalizer.take() {
                    fin.on_return(py, ret_val.bind(py))?;
                }
                Err(PyStopIteration::new_err(ret_val))
            }
            ffi::PySendResult::PYGEN_ERROR => {
                let err = PyErr::fetch(py);
                self.coro = None;
                self.self_task = None;
                log_app_exception(py, self.protocol_label, &err);
                if let Some(fin) = self.finalizer.take() {
                    let _ = fin.on_error(py, &err);
                }
                Err(PyStopIteration::new_err(py.None()))
            }
        }
    }
}

#[pymethods]
impl SteppedCoroutine {
    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.step_ptr(py, unsafe { ffi::Py_None() })
    }

    fn send(&mut self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.step_ptr(py, value.as_ptr())
    }

    #[pyo3(signature = (typ, val=None, tb=None))]
    fn throw(
        &mut self,
        py: Python<'_>,
        typ: &Bound<'_, PyAny>,
        val: Option<&Bound<'_, PyAny>>,
        tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        self.first_yielded = None;
        let Some(ref coro) = self.coro else {
            return Err(PyStopIteration::new_err(py.None()));
        };
        let res = match (val, tb) {
            (None, None) => coro
                .bind(py)
                .call_method1(pyo3::intern!(py, "throw"), (typ,)),
            (Some(v), None) => coro
                .bind(py)
                .call_method1(pyo3::intern!(py, "throw"), (typ, v)),
            (v, t) => coro.bind(py).call_method1(
                pyo3::intern!(py, "throw"),
                (
                    typ,
                    v.map_or_else(|| py.None(), |x| x.clone().unbind()),
                    t.map_or_else(|| py.None(), |x| x.clone().unbind()),
                ),
            ),
        };
        match res {
            Ok(yielded) => Ok(yielded.unbind()),
            Err(err) => {
                self.coro = None;
                self.self_task = None;
                if err.is_instance_of::<PyStopIteration>(py) {
                    if let Some(fin) = self.finalizer.take() {
                        let val_bound = err
                            .value(py)
                            .getattr(pyo3::intern!(py, "value"))
                            .unwrap_or_else(|_| py.None().into_bound(py));
                        fin.on_return(py, &val_bound)?;
                    }
                    return Err(err);
                }
                if let Ok(asyncio) = py.import(pyo3::intern!(py, "asyncio"))
                    && let Ok(cancelled_cls) = asyncio.getattr(pyo3::intern!(py, "CancelledError"))
                    && err.value(py).is_instance(&cancelled_cls).unwrap_or(false)
                {
                    self.finalizer = None;
                    return Err(err);
                }
                log_app_exception(py, self.protocol_label, &err);
                if let Some(fin) = self.finalizer.take() {
                    let _ = fin.on_error(py, &err);
                }
                Err(PyStopIteration::new_err(py.None()))
            }
        }
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        self.first_yielded = None;
        self.self_task = None;
        self.finalizer = None;
        if let Some(coro) = self.coro.take() {
            let _ = coro.bind(py).call_method0(pyo3::intern!(py, "close"));
        }
        Ok(())
    }
}

pub(crate) fn spawn_stepped_coroutine<'py>(
    py: Python<'py>,
    loop_bound: &Bound<'py, PyAny>,
    coro: Py<PyAny>,
    first_yielded: Py<PyAny>,
    finalizer: ProtocolFinalizer,
    protocol_label: &'static str,
) -> PyResult<Bound<'py, PyAny>> {
    let stepped = Py::new(
        py,
        SteppedCoroutine {
            coro: Some(coro),
            first_yielded: Some(first_yielded),
            finalizer: Some(finalizer),
            self_task: None,
            protocol_label,
        },
    )?;
    let task = loop_bound.call_method1(pyo3::intern!(py, "create_task"), (stepped.bind(py),))?;
    stepped.borrow_mut(py).self_task = Some(task.clone().unbind());
    Ok(task)
}

#[derive(Clone)]
pub(crate) enum BatchTargets {
    Contiguous(Arc<BatchSnapshot>),
    Indexed(Arc<BatchSnapshot>, Vec<usize>),
    Requests(Vec<(Arc<BatchSnapshot>, usize)>),
}

impl BatchTargets {
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Contiguous(snap) => snap.is_empty(),
            Self::Indexed(_, idxs) => idxs.is_empty(),
            Self::Requests(reqs) => reqs.is_empty(),
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Contiguous(snap) => snap.len(),
            Self::Indexed(_, idxs) => idxs.len(),
            Self::Requests(reqs) => reqs.len(),
        }
    }

    pub(crate) fn for_each(
        &self,
        mut f: impl FnMut(&Arc<BatchSnapshot>, usize) -> PyResult<()>,
    ) -> PyResult<()> {
        match self {
            Self::Contiguous(snap) => {
                for idx in 0..snap.len() {
                    f(snap, idx)?;
                }
            }
            Self::Indexed(snap, idxs) => {
                for &idx in idxs {
                    f(snap, idx)?;
                }
            }
            Self::Requests(reqs) => {
                for (snap, idx) in reqs {
                    f(snap, *idx)?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn collect_batch_targets(
    batch: &Bound<'_, PyAny>,
    indices: Option<Vec<usize>>,
) -> PyResult<BatchTargets> {
    if let Ok(rb) = batch.cast::<RequestBatch>() {
        let snap = Arc::clone(&rb.borrow().snapshot);
        if let Some(idxs) = indices {
            for &idx in &idxs {
                if idx >= snap.len() {
                    return Err(PyIndexError::new_err("batch index out of range"));
                }
            }
            return Ok(BatchTargets::Indexed(snap, idxs));
        }
        return Ok(BatchTargets::Contiguous(snap));
    }

    let mut pairs = Vec::with_capacity(batch.len().unwrap_or(8));
    for item in batch.try_iter()? {
        let item = item?;
        let req = item
            .cast::<Request>()
            .map_err(|_| PyTypeError::new_err("batch items must be RequestBatch or Request"))?;
        let req_ref = req.get();
        pairs.push((Arc::clone(&req_ref.batch), req_ref.index));
    }
    if let Some(idxs) = indices {
        let mut selected = Vec::with_capacity(idxs.len());
        for idx in idxs {
            let entry = pairs
                .get(idx)
                .ok_or_else(|| PyIndexError::new_err("batch index out of range"))?;
            selected.push((Arc::clone(&entry.0), entry.1));
        }
        return Ok(BatchTargets::Requests(selected));
    }
    Ok(BatchTargets::Requests(pairs))
}

pub(crate) fn current_running_loop(py: Python<'_>) -> Option<Bound<'_, PyAny>> {
    let asyncio = py.import(pyo3::intern!(py, "asyncio")).ok()?;
    let lp = asyncio
        .call_method0(pyo3::intern!(py, "_get_running_loop"))
        .ok()?;
    if lp.is_none() { None } else { Some(lp) }
}

pub(crate) enum LoopExecutionMode<'py> {
    Inline {
        active_loop: Option<Bound<'py, PyAny>>,
        drive_until_complete: bool,
    },
    CrossThread(Bound<'py, PyAny>),
}

pub(crate) fn classify_loop_mode<'py>(
    py: Python<'py>,
    loop_obj: Option<&Bound<'py, PyAny>>,
) -> PyResult<LoopExecutionMode<'py>> {
    let running = current_running_loop(py);
    match loop_obj {
        None => Ok(LoopExecutionMode::Inline {
            active_loop: running,
            drive_until_complete: false,
        }),
        Some(lp) if lp.is_none() => Ok(LoopExecutionMode::Inline {
            active_loop: running,
            drive_until_complete: false,
        }),
        Some(lp) => {
            if let Some(ref cur) = running
                && cur.is(lp)
            {
                return Ok(LoopExecutionMode::Inline {
                    active_loop: Some(lp.clone()),
                    drive_until_complete: false,
                });
            }
            let is_running = lp
                .call_method0(pyo3::intern!(py, "is_running"))?
                .is_truthy()?;
            if is_running {
                Ok(LoopExecutionMode::CrossThread(lp.clone()))
            } else {
                Ok(LoopExecutionMode::Inline {
                    active_loop: Some(lp.clone()),
                    drive_until_complete: true,
                })
            }
        }
    }
}

#[pyclass(name = "_AsgiBatchLoopCallback", frozen, immutable_type)]
struct AsgiBatchLoopCallback {
    app: Py<PyAny>,
    targets: BatchTargets,
    loop_obj: Py<PyAny>,
    root_path: String,
    lifespan_state: Option<Py<PyDict>>,
}

#[pymethods]
impl AsgiBatchLoopCallback {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let _ = drive_asgi_targets_inline(
            py,
            self.app.bind(py),
            &self.targets,
            Some(self.loop_obj.bind(py)),
            false,
            &self.root_path,
            self.lifespan_state.as_ref().map(|d| d.bind(py)),
        )?;
        Ok(())
    }
}

fn drive_asgi_targets_inline(
    py: Python<'_>,
    app: &Bound<'_, PyAny>,
    targets: &BatchTargets,
    active_loop: Option<&Bound<'_, PyAny>>,
    drive_until_complete: bool,
    root_path: &str,
    lifespan_state: Option<&Bound<'_, PyDict>>,
) -> PyResult<usize> {
    if targets.is_empty() {
        return Ok(0);
    }

    let normalized_root = root_path.trim_end_matches('/');
    let has_lifespan = lifespan_state.is_some_and(|d| !d.is_empty());
    let loop_py_opt = active_loop.map(|l| l.clone().unbind());
    let mut ready_responses: Vec<FlushedProtocolResponse> = Vec::with_capacity(targets.len());
    let mut spawned_tasks: Vec<Bound<'_, PyAny>> = Vec::new();
    let mut completed_inline = 0usize;

    targets.for_each(|snap, idx| {
        let mut scope = AsgiScope::new(Arc::clone(snap), idx, normalized_root.to_string());
        if has_lifespan && let Some(ls) = lifespan_state {
            scope.set_initial_state(py, ls.copy()?);
        }
        let scope_py = Py::new(py, scope)?;
        let proto = AsgiProtocol::from_batch(
            Arc::clone(snap),
            idx,
            loop_py_opt.as_ref().map(|l| l.clone_ref(py)),
        );
        let proto_py = Py::new(py, proto)?;
        let proto_bound = proto_py.bind(py);
        let receive_fn = proto_bound.getattr(pyo3::intern!(py, "receive"))?;

        let call_res = app.call1((scope_py.bind(py), receive_fn, proto_bound));
        let coro = match call_res {
            Ok(c) => c,
            Err(err) => {
                log_app_exception(py, "ASGI", &err);
                if let Some(err_resp) = proto_bound.get().take_error_for_batch() {
                    ready_responses.push(err_resp);
                }
                completed_inline += 1;
                return Ok(());
            }
        };

        let mut yielded: *mut ffi::PyObject = std::ptr::null_mut();
        let send_res = unsafe { ffi::PyIter_Send(coro.as_ptr(), ffi::Py_None(), &mut yielded) };
        match send_res {
            ffi::PySendResult::PYGEN_RETURN => {
                if !yielded.is_null() {
                    unsafe {
                        let _ = Bound::from_owned_ptr(py, yielded);
                    }
                }
                if let Some(resp) = proto_bound.get().take_completed_for_batch(true) {
                    ready_responses.push(resp);
                }
                completed_inline += 1;
            }
            ffi::PySendResult::PYGEN_ERROR => {
                let err = PyErr::fetch(py);
                log_app_exception(py, "ASGI", &err);
                if let Some(err_resp) = proto_bound.get().take_error_for_batch() {
                    ready_responses.push(err_resp);
                }
                completed_inline += 1;
            }
            ffi::PySendResult::PYGEN_NEXT => {
                let first_yielded = unsafe { Bound::from_owned_ptr(py, yielded) }.unbind();
                if let Some(resp) = proto_bound.get().take_completed_for_batch(false) {
                    ready_responses.push(resp);
                }
                let lp = match active_loop {
                    Some(l) => l.clone(),
                    None => py
                        .import(pyo3::intern!(py, "asyncio"))?
                        .call_method0(pyo3::intern!(py, "get_running_loop"))?,
                };
                let task = spawn_stepped_coroutine(
                    py,
                    &lp,
                    coro.unbind(),
                    first_yielded,
                    ProtocolFinalizer::Asgi(proto_py),
                    "ASGI",
                )?;
                if drive_until_complete {
                    spawned_tasks.push(task);
                }
            }
        }
        Ok(())
    })?;

    if !ready_responses.is_empty() {
        let items: Vec<(u64, u16, &[u8], &[u8])> = ready_responses
            .iter()
            .map(|r| {
                (
                    r.request_id,
                    r.status,
                    r.headers_raw.as_slice(),
                    r.body.as_slice(py),
                )
            })
            .collect();
        ResponseSubmitter::submit_batch_raw_inner(py, &items)?;
    }

    if drive_until_complete
        && !spawned_tasks.is_empty()
        && let Some(lp) = active_loop
    {
        let asyncio = py.import(pyo3::intern!(py, "asyncio"))?;
        let gather = asyncio.getattr(pyo3::intern!(py, "gather"))?;
        let args = PyTuple::new(py, &spawned_tasks)?;
        let gathered = gather.call1(args)?;
        lp.call_method1(pyo3::intern!(py, "run_until_complete"), (gathered,))?;
    }

    Ok(completed_inline)
}

/// Drive a batch of ASGI 3.0 HTTP requests using C-API `PyIter_Send` single-step execution.
#[pyfunction]
#[pyo3(
    name = "drive_asgi_batch",
    signature = (app, batch, *, loop_obj=None, root_path="", lifespan_state=None, indices=None)
)]
pub fn drive_asgi_batch(
    py: Python<'_>,
    app: &Bound<'_, PyAny>,
    batch: &Bound<'_, PyAny>,
    loop_obj: Option<&Bound<'_, PyAny>>,
    root_path: &str,
    lifespan_state: Option<&Bound<'_, PyDict>>,
    indices: Option<Vec<usize>>,
) -> PyResult<usize> {
    let targets = collect_batch_targets(batch, indices)?;
    if targets.is_empty() {
        return Ok(0);
    }

    match classify_loop_mode(py, loop_obj)? {
        LoopExecutionMode::Inline {
            active_loop,
            drive_until_complete,
        } => drive_asgi_targets_inline(
            py,
            app,
            &targets,
            active_loop.as_ref(),
            drive_until_complete,
            root_path,
            lifespan_state,
        ),
        LoopExecutionMode::CrossThread(lp) => {
            let cb = Py::new(
                py,
                AsgiBatchLoopCallback {
                    app: app.clone().unbind(),
                    targets,
                    loop_obj: lp.clone().unbind(),
                    root_path: root_path.to_string(),
                    lifespan_state: lifespan_state.map(|d| d.clone().unbind()),
                },
            )?;
            lp.call_method1(pyo3::intern!(py, "call_soon_threadsafe"), (cb,))?;
            Ok(0)
        }
    }
}

#[pyclass(name = "_NativeBatchLoopCallback", frozen, immutable_type)]
struct NativeBatchLoopCallback {
    items: Py<PyAny>,
    loop_obj: Py<PyAny>,
}

#[pymethods]
impl NativeBatchLoopCallback {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let _ = drive_native_items_inline(
            py,
            self.items.bind(py),
            Some(self.loop_obj.bind(py)),
            false,
        )?;
        Ok(())
    }
}

fn drive_native_items_inline(
    py: Python<'_>,
    items: &Bound<'_, PyAny>,
    active_loop: Option<&Bound<'_, PyAny>>,
    drive_until_complete: bool,
) -> PyResult<usize> {
    let cap = items.len().unwrap_or(8);
    if cap == 0 {
        return Ok(0);
    }
    let mut ready_responses: Vec<FlushedProtocolResponse> = Vec::with_capacity(cap);
    let mut spawned_tasks: Vec<Bound<'_, PyAny>> = Vec::new();
    let mut completed_inline = 0usize;

    for entry in items.try_iter()? {
        let entry = entry?;
        let tup = entry.cast::<PyTuple>()?;
        if tup.len() < 2 {
            return Err(PyTypeError::new_err(
                "drive_native_batch items must be (request_id, coro[, status_code]) tuples",
            ));
        }
        let request_id = tup.get_item(0)?.extract::<u64>()?;
        let coro = tup.get_item(1)?;
        let default_status = if tup.len() >= 3 {
            tup.get_item(2)?.extract::<u16>().unwrap_or(200)
        } else {
            200
        };

        let mut yielded: *mut ffi::PyObject = std::ptr::null_mut();
        let send_res = unsafe { ffi::PyIter_Send(coro.as_ptr(), ffi::Py_None(), &mut yielded) };
        match send_res {
            ffi::PySendResult::PYGEN_RETURN => {
                let ret_bound = if yielded.is_null() {
                    py.None().into_bound(py)
                } else {
                    unsafe { Bound::from_owned_ptr(py, yielded) }
                };
                match prepare_native_flushed_response(py, request_id, default_status, &ret_bound) {
                    Ok(flushed) => ready_responses.push(flushed),
                    Err(err) => {
                        log_app_exception(py, "Native", &err);
                        ready_responses.push(prepare_native_error_response(py, request_id, &err));
                    }
                }
                completed_inline += 1;
            }
            ffi::PySendResult::PYGEN_ERROR => {
                let err = PyErr::fetch(py);
                log_app_exception(py, "Native", &err);
                ready_responses.push(prepare_native_error_response(py, request_id, &err));
                completed_inline += 1;
            }
            ffi::PySendResult::PYGEN_NEXT => {
                let first_yielded = unsafe { Bound::from_owned_ptr(py, yielded) }.unbind();
                let lp = match active_loop {
                    Some(l) => l.clone(),
                    None => py
                        .import(pyo3::intern!(py, "asyncio"))?
                        .call_method0(pyo3::intern!(py, "get_running_loop"))?,
                };
                let task = spawn_stepped_coroutine(
                    py,
                    &lp,
                    coro.unbind(),
                    first_yielded,
                    ProtocolFinalizer::Native {
                        request_id,
                        default_status,
                    },
                    "Native",
                )?;
                if drive_until_complete {
                    spawned_tasks.push(task);
                }
            }
        }
    }

    if !ready_responses.is_empty() {
        let batch_slices: Vec<(u64, u16, &[u8], &[u8])> = ready_responses
            .iter()
            .map(|r| {
                (
                    r.request_id,
                    r.status,
                    r.headers_raw.as_slice(),
                    r.body.as_slice(py),
                )
            })
            .collect();
        ResponseSubmitter::submit_batch_raw_inner(py, &batch_slices)?;
    }

    if drive_until_complete
        && !spawned_tasks.is_empty()
        && let Some(lp) = active_loop
    {
        let asyncio = py.import(pyo3::intern!(py, "asyncio"))?;
        let gather = asyncio.getattr(pyo3::intern!(py, "gather"))?;
        let args = PyTuple::new(py, &spawned_tasks)?;
        let gathered = gather.call1(args)?;
        lp.call_method1(pyo3::intern!(py, "run_until_complete"), (gathered,))?;
    }

    Ok(completed_inline)
}

/// Step a batch of native `async def` handler coroutines in C via `PyIter_Send`.
#[pyfunction]
#[pyo3(name = "drive_native_batch", signature = (items, *, loop_obj=None))]
pub fn drive_native_batch(
    py: Python<'_>,
    items: &Bound<'_, PyAny>,
    loop_obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<usize> {
    if items.len().unwrap_or(0) == 0 {
        return Ok(0);
    }
    match classify_loop_mode(py, loop_obj)? {
        LoopExecutionMode::Inline {
            active_loop,
            drive_until_complete,
        } => drive_native_items_inline(py, items, active_loop.as_ref(), drive_until_complete),
        LoopExecutionMode::CrossThread(lp) => {
            let cb = Py::new(
                py,
                NativeBatchLoopCallback {
                    items: items.clone().unbind(),
                    loop_obj: lp.clone().unbind(),
                },
            )?;
            lp.call_method1(pyo3::intern!(py, "call_soon_threadsafe"), (cb,))?;
            Ok(0)
        }
    }
}
