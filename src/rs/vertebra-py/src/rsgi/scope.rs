//! RSGI v1.6 scope implementation backed lazily by [`BatchSnapshot`].

use std::sync::Arc;

use pyo3::prelude::*;

use vertebra_core::transport::{BatchSnapshot, encode_headers_compact, iter_raw_headers};

use crate::headers::Headers;
use crate::synthetic::SyntheticRequest;

/// RSGI v1.6 HTTP / WebSocket scope (`rsgi_version = "1.6"`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(name = "RsgiScope", frozen, freelist = 128, immutable_type)
)]
#[cfg_attr(Py_GIL_DISABLED, pyclass(name = "RsgiScope", frozen, immutable_type))]
pub struct RsgiScope {
    batch: Arc<BatchSnapshot>,
    index: usize,
    proto: String,
    http_version: String,
    server: String,
    client: String,
    scheme: String,
    method: String,
    path: String,
    query_string: String,
    authority: Option<String>,
}

impl RsgiScope {
    /// Construct an `RsgiScope` directly from a `BatchSnapshot` slot without intermediate dicts.
    pub fn from_batch(
        _py: Python<'_>,
        batch: &Arc<BatchSnapshot>,
        index: usize,
        proto: &str,
        scheme: &str,
        server: &str,
        client: &str,
    ) -> PyResult<Self> {
        let node = batch.node(index);
        let raw_headers = batch.headers_raw(index);
        let authority = iter_raw_headers(raw_headers)
            .find(|(name, _)| {
                name.eq_ignore_ascii_case(b":authority") || name.eq_ignore_ascii_case(b"host")
            })
            .map(|(_, val)| String::from_utf8_lossy(val).into_owned());

        Ok(Self {
            batch: Arc::clone(batch),
            index,
            proto: proto.to_string(),
            http_version: node.http_version().to_string(),
            server: server.to_string(),
            client: client.to_string(),
            scheme: scheme.to_string(),
            method: node.method_name().to_string(),
            path: batch.path(index).to_string(),
            query_string: batch.query(index).unwrap_or("").to_string(),
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
        proto: &str,
        http_version: &str,
        server: &str,
        client: &str,
        scheme: &str,
        method: &str,
        path: &str,
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

        Ok(Self {
            batch: snapshot,
            index: 0,
            proto: proto.to_string(),
            http_version: http_version.to_string(),
            server: server.to_string(),
            client: client.to_string(),
            scheme: scheme.to_string(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: query_string.to_string(),
            authority,
        })
    }

    #[getter]
    pub fn proto(&self) -> &str {
        &self.proto
    }

    #[getter]
    pub fn rsgi_version(&self) -> &str {
        "1.6"
    }

    #[getter]
    pub fn http_version(&self) -> &str {
        &self.http_version
    }

    #[getter]
    pub fn server(&self) -> &str {
        &self.server
    }

    #[getter]
    pub fn client(&self) -> &str {
        &self.client
    }

    #[getter]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    #[getter]
    pub fn method(&self) -> &str {
        &self.method
    }

    #[getter]
    pub fn path(&self) -> &str {
        &self.path
    }

    #[getter]
    pub fn query_string(&self) -> &str {
        &self.query_string
    }

    #[getter]
    pub fn headers(&self) -> Headers {
        let raw = self.batch.headers_raw(self.index);
        Headers::new(bytes::Bytes::copy_from_slice(raw))
    }

    #[getter]
    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    fn __repr__(&self) -> String {
        format!(
            "RsgiScope(proto={:?}, method={:?}, path={:?})",
            self.proto, self.method, self.path
        )
    }
}
