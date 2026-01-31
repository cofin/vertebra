use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyBytes};
use hyper::Request;
use hyper::body::{Incoming, Body};
use std::sync::Arc;
use tokio::sync::Mutex;
use http_body_util::BodyExt; // For collect/frame
use bytes::Bytes;

use crate::embedded::EmbeddedError;

pub struct DirectExecutor;

impl DirectExecutor {
    pub fn new() -> Self {
        Self
    }

    /// Convert a Hyper request to an ASGI scope.
    pub fn request_to_scope<'py, B>(
        &self,
        py: Python<'py>,
        req: &Request<B>,
        scheme: &str,
    ) -> Result<Bound<'py, PyDict>, EmbeddedError> {
        let scope = PyDict::new(py);

        // ASGI / Type
        scope.set_item("type", "http")?;
        let asgi = PyDict::new(py);
        asgi.set_item("version", "3.0")?;
        asgi.set_item("spec_version", "2.3")?;
        scope.set_item("asgi", asgi)?;

        // Method
        scope.set_item("method", req.method().as_str())?;

        // Scheme
        scope.set_item("scheme", scheme)?;

        // Path & Query
        let uri = req.uri();
        scope.set_item("path", uri.path())?;
        // Query string must be bytes
        scope.set_item("query_string", PyBytes::new(py, uri.query().unwrap_or("").as_bytes()))?;

        // Version
        let version = match req.version() {
            hyper::Version::HTTP_10 => "1.0",
            hyper::Version::HTTP_11 => "1.1",
            hyper::Version::HTTP_2 => "2",
            hyper::Version::HTTP_3 => "3",
            _ => "1.1", // Fallback
        };
        scope.set_item("http_version", version)?;

        // Headers
        let headers_list = PyList::empty(py);
        for (name, value) in req.headers() {
            let pair = (
                PyBytes::new(py, name.as_str().as_bytes()),
                PyBytes::new(py, value.as_bytes()),
            );
            headers_list.append(pair)?;
        }
        scope.set_item("headers", headers_list)?;

        // Root path
        scope.set_item("root_path", "")?;

        Ok(scope)
    }
}

/// A Python-compatible callable that streams the request body.
///
/// Implements the ASGI `receive` awaitable interface.
#[pyclass]
pub struct ZeroCopyBody {
    // Wrapped in Arc<Mutex> because PyClass must be Send, and Body is mutable.
    // Option allows us to take ownership if needed, or just mutable access.
    inner: Arc<Mutex<Option<Incoming>>>,
}

#[pymethods]
impl ZeroCopyBody {
    #[new]
    fn new() -> Self {
        // This is mostly for testing/Python side init.
        // In practice, Rust will construct this via `from_body`.
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    fn __call__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            
            // Check if body is present
            let body = match guard.as_mut() {
                Some(b) => b,
                None => {
                     // End of stream or empty
                     let msg = Python::with_gil(|py| {
                         let dict = PyDict::new(py);
                         dict.set_item("type", "http.request")?;
                         dict.set_item("body", PyBytes::new(py, &[]))?;
                         dict.set_item("more_body", false)?;
                         Ok::<_, PyErr>(dict.unbind())
                     })?;
                     return Ok(msg);
                }
            };

            // Read next frame
            // hyper::body::Incoming::frame returns Future
            match body.frame().await {
                Some(Ok(frame)) => {
                    let data = frame.into_data().unwrap_or_else(|_| Bytes::new());
                    let more_body = !body.is_end_stream();

                    let msg = Python::with_gil(|py| {
                        let dict = PyDict::new(py);
                        dict.set_item("type", "http.request")?;
                        dict.set_item("body", PyBytes::new(py, &data))?;
                        dict.set_item("more_body", more_body)?;
                        Ok::<_, PyErr>(dict.unbind())
                    })?;
                    Ok(msg)
                }
                Some(Err(e)) => {
                    Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
                }
                None => {
                    // Stream finished
                    let msg = Python::with_gil(|py| {
                        let dict = PyDict::new(py);
                        dict.set_item("type", "http.request")?;
                        dict.set_item("body", PyBytes::new(py, &[]))?;
                        dict.set_item("more_body", false)?;
                        Ok::<_, PyErr>(dict.unbind())
                    })?;
                    Ok(msg)
                }
            }
        })
    }
}

impl ZeroCopyBody {
    pub fn from_body(body: Incoming) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(body))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::Request;
    
    #[test]
    fn test_scope_mapping() {
        pyo3::prepare_freethreaded_python();
        
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/users?limit=10")
            .header("host", "example.com")
            .header("content-type", "application/json")
            .body(()) // Empty body for testing scope mapping
            .unwrap();
            
        let executor = DirectExecutor::new();
        
        Python::with_gil(|py| {
            let scope = executor.request_to_scope(py, &req, "http").unwrap();
            
            // Assertions
            assert_eq!(scope.get_item("type").unwrap().unwrap().extract::<String>().unwrap(), "http");
            assert_eq!(scope.get_item("method").unwrap().unwrap().extract::<String>().unwrap(), "POST");
            assert_eq!(scope.get_item("path").unwrap().unwrap().extract::<String>().unwrap(), "/api/v1/users");
            assert_eq!(scope.get_item("scheme").unwrap().unwrap().extract::<String>().unwrap(), "http");
            
            // Query string (bytes)
            let qs: Vec<u8> = scope.get_item("query_string").unwrap().unwrap().extract().unwrap();
            assert_eq!(qs, b"limit=10");
            
            // Headers
            let headers: Vec<(Vec<u8>, Vec<u8>)> = scope.get_item("headers").unwrap().unwrap().extract().unwrap();
            
            // Check host header
            let host = headers.iter().find(|(k, _)| k == b"host").expect("Host header missing");
            assert_eq!(host.1, b"example.com");
            
            // Check content-type
            let ct = headers.iter().find(|(k, _)| k == b"content-type").expect("Content-Type missing");
            assert_eq!(ct.1, b"application/json");
        });
    }
}
