//! vertebra-py — PyO3 extension module for Vertebra.
//!
//! Exposes the batch FFI API:
//! - `Request` — zero-object request proxy
//! - `Headers` — lazy header mapping
//! - `Response` — response builder
//! - `get_batch()` / `submit_results()` — vectorized batch API
//! - `init_transport()` — channel initialization

use pyo3::prelude::*;

pub mod api;
pub mod asgi_scope;
pub mod batch;
pub mod headers;
pub mod request;
pub mod response;
pub mod wsgi_environ;

pub use asgi_scope::{AsgiScope, AsgiScopeIterator};
pub use batch::{RequestBatch, RequestBatchIterator};
pub use headers::{Headers, HeadersIterator};
pub use request::Request;
pub use response::Response;
pub use wsgi_environ::{WsgiEnviron, WsgiEnvironIterator};

/// Whether this build targets free-threaded Python (3.14t+).
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// The `_vertebra` native extension module.
#[pymodule]
#[pyo3(name = "_vertebra")]
pub fn pymodule_init(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;

    m.add_class::<request::Request>()?;
    m.add_class::<headers::Headers>()?;
    m.add_class::<headers::HeadersIterator>()?;
    m.add_class::<response::Response>()?;
    m.add_class::<batch::RequestBatch>()?;
    m.add_class::<batch::RequestBatchIterator>()?;
    m.add_class::<asgi_scope::AsgiScope>()?;
    m.add_class::<asgi_scope::AsgiScopeIterator>()?;
    m.add_class::<wsgi_environ::WsgiEnviron>()?;
    m.add_class::<wsgi_environ::WsgiEnvironIterator>()?;

    // Vectorized batch API
    m.add_function(wrap_pyfunction!(api::init_transport, m)?)?;
    m.add_function(wrap_pyfunction!(api::get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(api::submit_results, m)?)?;
    m.add_function(wrap_pyfunction!(api::submit_response, m)?)?;

    // SHM async helpers
    m.add_function(wrap_pyfunction!(api::get_request_notify_fd, m)?)?;
    m.add_function(wrap_pyfunction!(api::clear_request_notification, m)?)?;
    m.add_function(wrap_pyfunction!(api::try_get_batch, m)?)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_threaded_const_exists() {
        // Verifies the FREE_THREADED constant compiles correctly
        let _ = FREE_THREADED;
    }
}
