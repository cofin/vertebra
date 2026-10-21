//! RSGI v1.6 scope and headers PyO3 types (`RsgiScope`, `RsgiHeaders`).

use std::sync::Arc;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};

use vertebra_core::transport::{BatchSnapshot, iter_raw_headers};

/// Case-insensitive RSGI header container backed by normalized `(lowercase_name, value)` pairs.
#[pyclass(name = "RsgiHeaders", frozen, freelist = 128)]
#[derive(Clone, Debug, Default)]
pub struct RsgiHeaders {
    entries: Vec<(String, String)>,
}

impl RsgiHeaders {
    /// Build `RsgiHeaders` from raw compact-encoded batch slab header bytes.
    pub fn from_raw_slab(raw: &[u8]) -> Self {
        let mut entries = Vec::with_capacity(8);
        for (name, value) in iter_raw_headers(raw) {
            let lower = String::from_utf8_lossy(name).to_ascii_lowercase();
            let val = String::from_utf8_lossy(value).into_owned();
            entries.push((lower, val));
        }
        Self { entries }
    }

    /// Build `RsgiHeaders` from pre-normalized header pairs.
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Self {
        let entries = pairs
            .into_iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect();
        Self { entries }
    }
}

#[pymethods]
impl RsgiHeaders {
    #[new]
    #[pyo3(signature = (entries=None))]
    pub fn py_new(entries: Option<Vec<(String, String)>>) -> Self {
        Self::from_pairs(entries.unwrap_or_default())
    }

    pub fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyString>> {
        for (name, value) in &self.entries {
            if name.eq_ignore_ascii_case(key) {
                return Ok(PyString::new(py, value));
            }
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    pub fn __contains__(&self, key: &str) -> bool {
        self.entries
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case(key))
    }

    pub fn __len__(&self) -> usize {
        self.entries.len()
    }

    #[pyo3(signature = (key, default=None))]
    pub fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        for (name, value) in &self.entries {
            if name.eq_ignore_ascii_case(key) {
                return PyString::new(py, value).into_any().unbind();
            }
        }
        default.unwrap_or_else(|| py.None())
    }

    pub fn get_all<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyList>> {
        let mut matches: Vec<Bound<'py, PyAny>> = Vec::new();
        for (name, value) in &self.entries {
            if name.eq_ignore_ascii_case(key) {
                matches.push(PyString::new(py, value).into_any());
            }
        }
        PyList::new(py, &matches)
    }

    pub fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<Bound<'py, PyAny>> = self
            .entries
            .iter()
            .map(|(k, _)| PyString::new(py, k).into_any())
            .collect();
        PyList::new(py, &items)
    }

    pub fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<Bound<'py, PyAny>> = self
            .entries
            .iter()
            .map(|(_, v)| PyString::new(py, v).into_any())
            .collect();
        PyList::new(py, &items)
    }

    pub fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(self.entries.len());
        for (k, v) in &self.entries {
            let pair = PyTuple::new(
                py,
                [
                    PyString::new(py, k).into_any(),
                    PyString::new(py, v).into_any(),
                ],
            )?;
            tuples.push(pair.into_any());
        }
        PyList::new(py, &tuples)
    }

    pub fn __repr__(&self) -> String {
        format!("RsgiHeaders(len={})", self.entries.len())
    }
}

/// RSGI v1.6 HTTP / WebSocket scope (`rsgi_version = "1.6"`).
#[pyclass(name = "RsgiScope", frozen, freelist = 128)]
pub struct RsgiScope {
    proto: String,
    http_version: String,
    server: String,
    client: String,
    scheme: String,
    method: String,
    path: String,
    query_string: String,
    headers: Py<RsgiHeaders>,
    authority: Option<String>,
}

impl RsgiScope {
    /// Construct an `RsgiScope` directly from a `BatchSnapshot` slot without intermediate dicts.
    pub fn from_batch(
        py: Python<'_>,
        batch: &Arc<BatchSnapshot>,
        index: usize,
        proto: &str,
        scheme: &str,
        server: &str,
        client: &str,
    ) -> PyResult<Self> {
        let node = batch.node(index);
        let raw_headers = batch.headers_raw(index);
        let headers_obj = RsgiHeaders::from_raw_slab(raw_headers);
        let authority = iter_raw_headers(raw_headers)
            .find(|(name, _)| {
                name.eq_ignore_ascii_case(b":authority") || name.eq_ignore_ascii_case(b"host")
            })
            .map(|(_, val)| String::from_utf8_lossy(val).into_owned());
        Ok(Self {
            proto: proto.to_string(),
            http_version: node.http_version().to_string(),
            server: server.to_string(),
            client: client.to_string(),
            scheme: scheme.to_string(),
            method: node.method_name().to_string(),
            path: batch.path(index).to_string(),
            query_string: batch.query(index).unwrap_or("").to_string(),
            headers: Py::new(py, headers_obj)?,
            authority,
        })
    }
}

#[pymethods]
impl RsgiScope {
    #[new]
    #[pyo3(signature = (
        *,
        proto="http",
        http_version="1.1",
        server="127.0.0.1:8000",
        client="127.0.0.1:0",
        scheme="http",
        method="GET",
        path="/",
        query_string="",
        headers=None,
        authority=None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn py_new(
        py: Python<'_>,
        proto: &str,
        http_version: &str,
        server: &str,
        client: &str,
        scheme: &str,
        method: &str,
        path: &str,
        query_string: &str,
        headers: Option<Py<RsgiHeaders>>,
        authority: Option<String>,
    ) -> PyResult<Self> {
        let headers = match headers {
            Some(h) => h,
            None => Py::new(py, RsgiHeaders::default())?,
        };
        Ok(Self {
            proto: proto.to_string(),
            http_version: http_version.to_string(),
            server: server.to_string(),
            client: client.to_string(),
            scheme: scheme.to_string(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: query_string.to_string(),
            headers,
            authority,
        })
    }

    #[getter]
    pub fn proto<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::intern(py, &self.proto)
    }

    #[getter]
    pub fn http_version<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::intern(py, &self.http_version)
    }

    #[getter]
    pub fn rsgi_version<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        pyo3::intern!(py, "1.6")
    }

    #[getter]
    pub fn server<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self.server)
    }

    #[getter]
    pub fn client<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self.client)
    }

    #[getter]
    pub fn scheme<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::intern(py, &self.scheme)
    }

    #[getter]
    pub fn method<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::intern(py, &self.method)
    }

    #[getter]
    pub fn path<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self.path)
    }

    #[getter]
    pub fn query_string<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self.query_string)
    }

    #[getter]
    pub fn headers(&self, py: Python<'_>) -> Py<RsgiHeaders> {
        self.headers.clone_ref(py)
    }

    #[getter]
    pub fn authority<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        self.authority.as_deref().map(|a| PyString::new(py, a))
    }

    pub fn __repr__(&self) -> String {
        format!(
            "RsgiScope(proto={:?}, method={:?}, path={:?}, rsgi_version=\"1.6\")",
            self.proto, self.method, self.path
        )
    }
}
