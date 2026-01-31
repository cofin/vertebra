use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyBytes};
use hyper::Request;
use hyper::body::{Incoming, Body, Frame};
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot, mpsc};
use http_body_util::BodyExt; // For collect/frame
use bytes::Bytes;
use hyper::{StatusCode, header::{HeaderName, HeaderValue, HeaderMap}};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;

use crate::embedded::{EmbeddedError, EmbeddedWorker};

/// Info extracted from http.response.start
#[derive(Debug)]
pub struct ResponseInfo {
    pub status: StatusCode,
    pub headers: HeaderMap,
}

/// A response body that streams from an MPSC channel.
pub struct ResponseBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, hyper::Error>>,
}

impl Body for ResponseBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.rx.poll_recv(cx)
    }

    fn is_end_stream(&self) -> bool {
        // We don't know if it's end until channel closes
        false 
    }
}

/// The Hyper Service that bridges requests to the EmbeddedWorker.
#[derive(Clone)]
pub struct EmbeddedService {
    worker: EmbeddedWorker,
}

impl EmbeddedService {
    pub fn new(worker: EmbeddedWorker) -> Self {
        Self { worker }
    }
}

impl hyper::service::Service<Request<Incoming>> for EmbeddedService {
    type Response = hyper::Response<ResponseBody>;
    type Error = EmbeddedError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let worker = self.worker.clone();
        let executor = DirectExecutor::new();
        
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let zero_copy_body = ZeroCopyBody::from_body(body);
            
            let (start_tx, start_rx) = oneshot::channel();
            let (body_tx, body_rx) = mpsc::channel(1);
            let sender = ResponseSender::new(start_tx, body_tx);
            
            // Acquire GIL to map request and spawn python task
            let py_future = Python::with_gil(|py| -> Result<_, EmbeddedError> {
                // Reconstruct request with empty body for mapping
                let req_head = Request::from_parts(parts, ());
                let scope = executor.request_to_scope(py, &req_head, "http")?;
                
                let receive_callable = Py::new(py, zero_copy_body)?;
                let send_callable = Py::new(py, sender)?;
                
                let coroutine = worker.execute(
                    py, 
                    scope, 
                    receive_callable.bind(py).clone().into_any(), 
                    send_callable.bind(py).clone().into_any()
                )?;
                
                // Convert to Rust future
                let fut = pyo3_async_runtimes::tokio::into_future(coroutine)?;
                Ok(fut)
            })?;
            
            // Spawn the python execution
            tokio::spawn(async move {
                if let Err(e) = py_future.await {
                    eprintln!("Python execution error: {:?}", e);
                    // We can't easily signal error to response if headers already sent
                    // If headers not sent, start_rx will drop and we handle it below
                }
            });
            
            // Wait for start signal
            let info = start_rx.await.map_err(|_| EmbeddedError::Path("Application did not send http.response.start".into()))?;
            
            let mut builder = hyper::Response::builder()
                .status(info.status);
            
            *builder.headers_mut().unwrap() = info.headers;
            
            let response = builder
                .body(ResponseBody { rx: body_rx })
                .map_err(|e| EmbeddedError::Path(e.to_string()))?;
                
            Ok(response)
        })
    }
}

/// A Python-compatible callable that handles ASGI `send` messages.
#[pyclass]
pub struct ResponseSender {
    // We use Arc<Mutex<Option<...>>> to safely handle the one-time start channel
    start_tx: Arc<Mutex<Option<oneshot::Sender<ResponseInfo>>>>,
    // Channel for streaming body chunks
    body_tx: mpsc::Sender<Result<Frame<Bytes>, hyper::Error>>,
}

#[pymethods]
impl ResponseSender {
    fn __call__<'p>(&self, py: Python<'p>, message: Bound<'p, PyDict>) -> PyResult<Bound<'p, PyAny>> {
        let start_tx = self.start_tx.clone();
        let body_tx = self.body_tx.clone();
        
        // Extract data immediately while we have the GIL
        let msg_type: String = message.get_item("type")?.ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing type"))?.extract()?;
        
        // Prepare data for async block
        let mut status_code = None;
        let mut headers_vec = None;
        let mut body_bytes = None;
        let mut _more_body = false;

        if msg_type == "http.response.start" {
            status_code = Some(message.get_item("status")?.ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing status"))?.extract::<u16>()?);
            // Extract headers
            if let Some(headers) = message.get_item("headers")? {
                let h_list: Bound<'_, PyList> = headers.extract()?;
                let mut vec = Vec::new();
                for item in h_list.iter() {
                    let tuple: Bound<'_, pyo3::types::PyTuple> = item.extract()?;
                    let k: Vec<u8> = tuple.get_item(0)?.extract()?;
                    let v: Vec<u8> = tuple.get_item(1)?.extract()?;
                    vec.push((k, v));
                }
                headers_vec = Some(vec);
            }
        } else if msg_type == "http.response.body" {
            let bytes_obj = message.get_item("body")?.ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing body"))?;
            body_bytes = Some(bytes_obj.extract::<Vec<u8>>()?); // TODO: Avoid copy?
            if let Some(mb) = message.get_item("more_body")? {
                _more_body = mb.extract::<bool>()?;
            }
        }

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if msg_type == "http.response.start" {
                let mut guard = start_tx.lock().await;
                if let Some(tx) = guard.take() {
                    let mut headers = HeaderMap::new();
                    if let Some(h_vec) = headers_vec {
                        for (k, v) in h_vec {
                            if let (Ok(n), Ok(val)) = (HeaderName::from_bytes(&k), HeaderValue::from_bytes(&v)) {
                                headers.append(n, val);
                            }
                        }
                    }
                    
                    let info = ResponseInfo {
                        status: StatusCode::from_u16(status_code.unwrap()).unwrap_or(StatusCode::OK),
                        headers,
                    };
                    let _ = tx.send(info); // Ignore error if receiver dropped
                } else {
                    // Already sent start?
                    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("http.response.start already sent"));
                }
            } else if msg_type == "http.response.body" {
                if let Some(bytes) = body_bytes {
                     let frame = Frame::data(Bytes::from(bytes));
                     if body_tx.send(Ok(frame)).await.is_err() {
                         return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>("Response receiver closed"));
                     }
                }
            }
            Ok(())
        })
    }
}

impl ResponseSender {
    pub fn new(
        start_tx: oneshot::Sender<ResponseInfo>,
        body_tx: mpsc::Sender<Result<Frame<Bytes>, hyper::Error>>
    ) -> Self {
        Self {
            start_tx: Arc::new(Mutex::new(Some(start_tx))),
            body_tx,
        }
    }
}

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
    use hyper::StatusCode;

    #[test]
    fn test_scope_mapping() {
        pyo3::prepare_freethreaded_python();
        
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/users?limit=10")
            .header("host", "example.com")
            .header("content-type", "application/json")
            .body(()) // Use empty body for scope mapping test
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
            let binding = scope.get_item("query_string").unwrap().unwrap();
            let qs: Vec<u8> = binding.extract().unwrap();
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

    #[tokio::test]
    async fn test_response_sender_integration() {
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let (body_tx, mut body_rx) = tokio::sync::mpsc::channel(1);
        let sender = ResponseSender::new(start_tx, body_tx);

        // Spawn thread to run Python
        let handle = std::thread::spawn(move || {
             // Ensure initialized
             pyo3::prepare_freethreaded_python();
             Python::with_gil(|py| {
                 let sender = Py::new(py, sender).unwrap();
                 let code = c"
import asyncio
async def worker(sender):
    await sender({'type': 'http.response.start', 'status': 201, 'headers': [(b'x-foo', b'bar')]})
    await sender({'type': 'http.response.body', 'body': b'hello'})

def run(sender):
    asyncio.run(worker(sender))
";
                 let module = PyModule::from_code(py, code, c"test.py", c"test").unwrap();
                 module.getattr("run").unwrap().call1((sender,)).unwrap();
             });
        });
        
        // Verify start
        let info = start_rx.await.unwrap();
        assert_eq!(info.status, StatusCode::CREATED);
        assert_eq!(info.headers.get("x-foo").unwrap(), "bar");
        
        // Verify body
        let frame = body_rx.recv().await.unwrap().unwrap();
        assert_eq!(frame.into_data().unwrap(), Bytes::from("hello"));
        
        handle.join().unwrap();
    }
}
