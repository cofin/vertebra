//! RSGI v1.6 scope implementation backed lazily by [`BatchSnapshot`].

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyString;

use vertebra_core::transport::{BatchSnapshot, encode_headers_compact, iter_raw_headers};

use crate::headers::Headers;
use crate::request::{intern_http_method, intern_http_version};
use crate::synthetic::SyntheticRequest;

#[derive(Clone)]
enum ProtoKind {
    Http,
    Ws,
    Custom(Box<str>),
}

impl ProtoKind {
    #[inline]
    fn from_str_tag(proto: &str) -> Self {
        match proto {
            "http" => Self::Http,
            "ws" => Self::Ws,
            other => Self::Custom(other.into()),
        }
    }

    #[inline]
    fn as_str(&self) -> &str {
        match self {
            Self::Http => "http",
            Self::Ws => "ws",
            Self::Custom(s) => s,
        }
    }
}

#[derive(Clone)]
enum SchemeKind {
    Http,
    Https,
    Ws,
    Wss,
    Custom(Box<str>),
}

impl SchemeKind {
    #[inline]
    fn from_str_tag(scheme: &str) -> Self {
        match scheme {
            "http" => Self::Http,
            "https" => Self::Https,
            "ws" => Self::Ws,
            "wss" => Self::Wss,
            other => Self::Custom(other.into()),
        }
    }
}

/// RSGI v1.6 HTTP / WebSocket scope (`rsgi_version = "1.6"`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "RsgiScope", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "RsgiScope", frozen, immutable_type))]
pub struct RsgiScope {
    batch: Arc<BatchSnapshot>,
    index: usize,
    root_path: String,
    proto_kind: ProtoKind,
    scheme_kind: SchemeKind,
    server_override: Option<Box<str>>,
    client_override: Option<Box<str>>,
    authority_cache: OnceLock<Option<String>>,
    headers_cache: PyOnceLock<Py<Headers>>,
}

impl RsgiScope {
    /// Construct an `RsgiScope` directly from a `BatchSnapshot` slot with zero eager string copies.
    #[inline]
    pub fn from_batch(
        batch: &Arc<BatchSnapshot>,
        index: usize,
        proto: &str,
        scheme: &str,
        server: &str,
        client: &str,
        root_path: &str,
    ) -> Self {
        let trimmed_root = root_path.trim_end_matches('/');
        let root_owned = if trimmed_root.is_empty() {
            String::new()
        } else {
            trimmed_root.to_string()
        };
        let effective_scheme = if scheme == "http" && (batch.flags() & 1) != 0 {
            SchemeKind::Https
        } else {
            SchemeKind::from_str_tag(scheme)
        };
        Self {
            batch: Arc::clone(batch),
            index,
            root_path: root_owned,
            proto_kind: ProtoKind::from_str_tag(proto),
            scheme_kind: effective_scheme,
            server_override: if server == "127.0.0.1:8000" {
                None
            } else {
                Some(server.into())
            },
            client_override: if client == "127.0.0.1:0" {
                None
            } else {
                Some(client.into())
            },
            authority_cache: OnceLock::new(),
            headers_cache: PyOnceLock::new(),
        }
    }

    #[inline]
    fn stripped_path(&self) -> &str {
        let full_path = self.batch.path(self.index);
        if !self.root_path.is_empty() && full_path.starts_with(&self.root_path) {
            let rem = &full_path[self.root_path.len()..];
            if rem.is_empty() {
                return "/";
            }
            if rem.starts_with('/') {
                return rem;
            }
        }
        full_path
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
        root_path="",
        query_string="",
        headers=None,
        authority=None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn py_new(
        proto: &str,
        http_version: &str,
        server: &str,
        client: &str,
        scheme: &str,
        method: &str,
        path: &str,
        root_path: &str,
        query_string: &str,
        headers: Option<Vec<(String, String)>>,
        authority: Option<String>,
    ) -> PyResult<Self> {
        let raw_headers = match headers {
            Some(pairs) => {
                let raw_pairs: Vec<(Vec<u8>, Vec<u8>)> = pairs
                    .into_iter()
                    .map(|(k, v)| (k.into_bytes(), v.into_bytes()))
                    .collect();
                encode_headers_compact(&raw_pairs)
            }
            None => Vec::new(),
        };

        let snapshot = SyntheticRequest::build(
            method,
            path,
            Some(query_string),
            &raw_headers,
            b"",
            0,
            0,
            http_version,
        );

        let scope = Self::from_batch(&snapshot, 0, proto, scheme, server, client, root_path);
        if let Some(explicit_auth) = authority {
            let _ = scope.authority_cache.set(Some(explicit_auth));
        }
        Ok(scope)
    }

    #[getter]
    pub fn proto<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        match &self.proto_kind {
            ProtoKind::Http => pyo3::intern!(py, "http").clone(),
            ProtoKind::Ws => pyo3::intern!(py, "ws").clone(),
            ProtoKind::Custom(s) => PyString::new(py, s),
        }
    }

    #[getter]
    pub fn rsgi_version<'py>(&self, py: Python<'py>) -> &'py Bound<'py, PyString> {
        pyo3::intern!(py, "1.6")
    }

    #[getter]
    pub fn http_version<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        intern_http_version(py, self.batch.node(self.index).http_version())
    }

    #[getter]
    pub fn server<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        match &self.server_override {
            None => pyo3::intern!(py, "127.0.0.1:8000").clone(),
            Some(s) => PyString::new(py, s),
        }
    }

    #[getter]
    pub fn client<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        match &self.client_override {
            None => pyo3::intern!(py, "127.0.0.1:0").clone(),
            Some(s) => PyString::new(py, s),
        }
    }

    #[getter]
    pub fn scheme<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        match &self.scheme_kind {
            SchemeKind::Http => pyo3::intern!(py, "http").clone(),
            SchemeKind::Https => pyo3::intern!(py, "https").clone(),
            SchemeKind::Ws => pyo3::intern!(py, "ws").clone(),
            SchemeKind::Wss => pyo3::intern!(py, "wss").clone(),
            SchemeKind::Custom(s) => PyString::new(py, s),
        }
    }

    #[getter]
    pub fn method<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        intern_http_method(py, self.batch.node(self.index).method_name())
    }

    #[getter]
    pub fn path<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        let stripped = self.stripped_path();
        if stripped == "/" {
            pyo3::intern!(py, "/").clone()
        } else {
            PyString::new(py, stripped)
        }
    }

    #[getter]
    pub fn root_path<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        if self.root_path.is_empty() {
            pyo3::intern!(py, "").clone()
        } else {
            PyString::new(py, &self.root_path)
        }
    }

    #[getter]
    pub fn query_string<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        match self.batch.query(self.index) {
            None | Some("") => pyo3::intern!(py, "").clone(),
            Some(q) => PyString::new(py, q),
        }
    }

    #[getter]
    pub fn headers(&self, py: Python<'_>) -> PyResult<Py<Headers>> {
        self.headers_cache
            .get_or_try_init(py, || {
                let raw = self.batch.headers_raw(self.index);
                Py::new(py, Headers::new(Bytes::copy_from_slice(raw)))
            })
            .map(|h| h.clone_ref(py))
    }

    #[getter]
    pub fn authority(&self) -> Option<&str> {
        self.authority_cache
            .get_or_init(|| {
                let raw_headers = self.batch.headers_raw(self.index);
                iter_raw_headers(raw_headers)
                    .find(|(name, _)| {
                        name.eq_ignore_ascii_case(b":authority")
                            || name.eq_ignore_ascii_case(b"host")
                    })
                    .map(|(_, val)| String::from_utf8_lossy(val).into_owned())
            })
            .as_deref()
    }

    fn __repr__(&self) -> String {
        format!(
            "RsgiScope(proto={:?}, method={:?}, path={:?})",
            self.proto_kind.as_str(),
            self.batch.node(self.index).method_name(),
            self.stripped_path(),
        )
    }
}
