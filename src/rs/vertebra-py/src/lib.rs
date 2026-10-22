//! vertebra-py — PyO3 extension module for Vertebra.
//!
//! Exposes the batch FFI API, protocol scopes, native runners, and telemetry.

use pyo3::prelude::*;

pub mod agent;
pub mod asgi;
pub mod broker;
pub mod file_io;
pub mod headers;
pub mod request;
pub mod response;
pub mod rsgi;
pub mod synthetic;
pub mod telemetry;
pub mod transport;
pub mod wsgi;

pub use agent::{SchemaDigestCache, classify_jsonrpc_frame};
pub use asgi::{AsgiScope, AsgiScopeIterator};
pub use broker::{
    NativeBrokerStats, NativeLeaseBroker, NativeLeaseGrant, NativeStatementCache,
    NativeStatementCacheStats, NativeStatementPlan,
};
pub use headers::{Headers, HeadersIterator};
pub use request::{Request, RequestBatch, RequestBatchIterator};
pub use response::Response;
pub use rsgi::{RsgiHttpProtocol, RsgiScope, RsgiTransport};
pub use telemetry::{
    NativeJwtGuard, NativeLogSink, NativeRateLimiter, format_traceparent, parse_traceparent,
    sanitize_sql,
};
pub use wsgi::{
    WsgiEnviron, WsgiEnvironIterator, WsgiFileWrapper, WsgiInput, WsgiStartResponse,
    run_wsgi_callable,
};

/// Whether this build targets free-threaded Python (3.14t+).
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// The `_vertebra` native extension module.
#[pymodule(gil_used = false)]
#[pyo3(name = "_vertebra")]
pub fn pymodule_init(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;

    m.add_class::<request::Request>()?;
    m.add_class::<headers::Headers>()?;
    m.add_class::<headers::HeadersIterator>()?;
    m.add_class::<response::Response>()?;
    m.add_class::<request::RequestBatch>()?;
    m.add_class::<request::RequestBatchIterator>()?;
    m.add_class::<asgi::AsgiScope>()?;
    m.add_class::<asgi::AsgiScopeIterator>()?;
    m.add_class::<wsgi::WsgiEnviron>()?;
    m.add_class::<wsgi::WsgiEnvironIterator>()?;
    m.add_class::<wsgi::WsgiInput>()?;
    m.add_class::<wsgi::WsgiFileWrapper>()?;
    m.add_class::<wsgi::WsgiStartResponse>()?;
    m.add_class::<rsgi::RsgiScope>()?;
    m.add_class::<rsgi::RsgiHttpProtocol>()?;
    m.add_class::<rsgi::RsgiTransport>()?;
    m.add_class::<telemetry::NativeLogSink>()?;
    m.add_class::<telemetry::NativeJwtGuard>()?;
    m.add_class::<telemetry::NativeRateLimiter>()?;
    m.add_class::<broker::NativeLeaseGrant>()?;
    m.add_class::<broker::NativeBrokerStats>()?;
    m.add_class::<broker::NativeLeaseBroker>()?;
    m.add_class::<broker::NativeStatementPlan>()?;
    m.add_class::<broker::NativeStatementCacheStats>()?;
    m.add_class::<broker::NativeStatementCache>()?;
    m.add_class::<agent::SchemaDigestCache>()?;

    // Agentic JSON-RPC fastpath
    m.add_function(wrap_pyfunction!(agent::classify_jsonrpc_frame, m)?)?;

    // Vectorized batch API
    m.add_function(wrap_pyfunction!(transport::init_transport, m)?)?;
    m.add_function(wrap_pyfunction!(transport::get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(transport::submit_results, m)?)?;
    m.add_function(wrap_pyfunction!(transport::submit_response, m)?)?;
    m.add_function(wrap_pyfunction!(transport::submit_raw_response, m)?)?;
    m.add_function(wrap_pyfunction!(transport::submit_response_raw, m)?)?;
    m.add_function(wrap_pyfunction!(transport::submit_batch_raw, m)?)?;

    // SHM async helpers
    m.add_function(wrap_pyfunction!(transport::get_request_notify_fd, m)?)?;
    m.add_function(wrap_pyfunction!(transport::clear_request_notification, m)?)?;
    m.add_function(wrap_pyfunction!(transport::try_get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(file_io::stream_file_chunks, m)?)?;
    m.add_function(wrap_pyfunction!(wsgi::run_wsgi_callable, m)?)?;

    // Telemetry helpers
    m.add_function(wrap_pyfunction!(telemetry::parse_traceparent, m)?)?;
    m.add_function(wrap_pyfunction!(telemetry::format_traceparent, m)?)?;
    m.add_function(wrap_pyfunction!(telemetry::sanitize_sql, m)?)?;

    Ok(())
}
