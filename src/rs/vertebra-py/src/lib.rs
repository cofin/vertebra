//! vertebra-py — PyO3 extension module for Vertebra.
//!
//! Exposes the batch FFI API, protocol scopes, native runners, and telemetry.

use pyo3::prelude::*;

pub mod agent;
pub mod asgi;
pub mod broker;
pub mod db;
pub mod file_io;
pub mod headers;
pub mod msgspec_bridge;
pub mod request;
pub mod response;
pub mod rsgi;
pub mod synthetic;
pub mod telemetry;
pub mod transport;
pub mod wsgi;

pub use agent::{SchemaDigestCache, classify_jsonrpc_frame};
pub use asgi::{
    AsgiProtocol, AsgiScope, AsgiScopeIterator, PyEmptyAwaitable, PyValueAwaitable,
    drive_asgi_batch,
};
pub use broker::{
    BrokerStats, LeaseBroker, LeaseGrant, StatementCache, StatementCacheStats, StatementPlan,
};
pub use db::{AlloyDBDialer, ArrowCursor, CloudSQLDialer};
pub use headers::{Headers, HeadersIterator};
pub use msgspec_bridge::{
    call_vectorcall, hydrate_one_vectorcall, hydrate_rows_vectorcall,
    instantiate_struct_vectorcall, orm_to_struct_vectorcall, pack_grpc_lpm_frame,
};
pub use request::{Request, RequestBatch, RequestBatchIterator};
pub use response::Response;
pub use rsgi::{RsgiHttpProtocol, RsgiScope, RsgiTransport, drive_rsgi_batch};
pub use telemetry::{
    JWTGuard, LogSink, RateLimiter, format_traceparent, parse_traceparent, sanitize_sql,
};
pub use wsgi::{
    WsgiEnviron, WsgiEnvironIterator, WsgiFileWrapper, WsgiInput, WsgiStartResponse,
    drive_wsgi_batch, run_wsgi_callable,
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
    m.add_class::<asgi::AsgiProtocol>()?;
    m.add_class::<asgi::PyEmptyAwaitable>()?;
    m.add_class::<asgi::PyValueAwaitable>()?;
    m.add_class::<wsgi::WsgiEnviron>()?;
    m.add_class::<wsgi::WsgiEnvironIterator>()?;
    m.add_class::<wsgi::WsgiInput>()?;
    m.add_class::<wsgi::WsgiFileWrapper>()?;
    m.add_class::<wsgi::WsgiStartResponse>()?;
    m.add_class::<rsgi::RsgiScope>()?;
    m.add_class::<rsgi::RsgiHttpProtocol>()?;
    m.add_class::<rsgi::RsgiTransport>()?;
    m.add_class::<telemetry::LogSink>()?;
    m.add_class::<telemetry::JWTGuard>()?;
    m.add_class::<telemetry::RateLimiter>()?;
    m.add_class::<broker::LeaseGrant>()?;
    m.add_class::<broker::BrokerStats>()?;
    m.add_class::<broker::LeaseBroker>()?;
    m.add_class::<broker::StatementPlan>()?;
    m.add_class::<broker::StatementCacheStats>()?;
    m.add_class::<broker::StatementCache>()?;
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
    m.add_function(wrap_pyfunction!(transport::prepare_request_sleep, m)?)?;
    m.add_function(wrap_pyfunction!(transport::cancel_request_sleep, m)?)?;
    m.add_function(wrap_pyfunction!(transport::try_get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(file_io::stream_file_chunks, m)?)?;
    m.add_function(wrap_pyfunction!(wsgi::run_wsgi_callable, m)?)?;
    m.add_function(wrap_pyfunction!(wsgi::drive_wsgi_batch, m)?)?;
    m.add_function(wrap_pyfunction!(asgi::drive_asgi_batch, m)?)?;
    m.add_function(wrap_pyfunction!(rsgi::drive_rsgi_batch, m)?)?;

    // Telemetry helpers
    m.add_function(wrap_pyfunction!(telemetry::parse_traceparent, m)?)?;
    m.add_function(wrap_pyfunction!(telemetry::format_traceparent, m)?)?;
    m.add_function(wrap_pyfunction!(telemetry::sanitize_sql, m)?)?;

    // Zero-copy msgspec, gRPC LPM, and vectorcall hydration helpers
    m.add_function(wrap_pyfunction!(
        msgspec_bridge::instantiate_struct_vectorcall,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(msgspec_bridge::call_vectorcall, m)?)?;
    m.add_function(wrap_pyfunction!(msgspec_bridge::hydrate_one_vectorcall, m)?)?;
    m.add_function(wrap_pyfunction!(
        msgspec_bridge::hydrate_rows_vectorcall,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        msgspec_bridge::orm_to_struct_vectorcall,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(msgspec_bridge::pack_grpc_lpm_frame, m)?)?;

    // Native database dialers, wire codecs, and SHM Arrow IPC cursors
    db::register_db_bindings(m)?;

    Ok(())
}
