//! vertebra-py — PyO3 extension module for Vertebra.
//!
//! Exposes the Ghost Framework batch FFI API:
//! - `VertebraRequest` — zero-object request proxy
//! - `VertebraHeaders` — lazy header mapping
//! - `VertebraResponse` — response builder
//! - `get_batch()` / `submit_results()` — vectorized batch API
//! - `init_batch_channel()` — channel initialization

use pyo3::prelude::*;

pub mod api;
pub mod batch;
pub mod headers;
pub mod protocol;
pub mod request;
pub mod response;

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

    // Ghost Framework FFI types
    m.add_class::<request::VertebraRequest>()?;
    m.add_class::<headers::VertebraHeaders>()?;
    m.add_class::<headers::VertebraHeadersIter>()?;
    m.add_class::<response::VertebraResponse>()?;
    m.add_class::<batch::VertebraBatch>()?;
    m.add_class::<batch::VertebraBatchIter>()?;

    // Vectorized batch API
    m.add_function(wrap_pyfunction!(api::init_transport, m)?)?;
    m.add_function(wrap_pyfunction!(api::get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(api::submit_results, m)?)?;
    m.add_function(wrap_pyfunction!(api::submit_response, m)?)?;

    // SHM async helpers
    m.add_function(wrap_pyfunction!(api::get_request_notify_fd, m)?)?;
    m.add_function(wrap_pyfunction!(api::clear_request_notification, m)?)?;
    m.add_function(wrap_pyfunction!(api::try_get_batch, m)?)?;

    // Protocol flags
    m.add("FLAGS_NONE", vertebra_core::protocol::flags::NONE)?;
    m.add("FLAGS_PRIORITY", vertebra_core::protocol::flags::PRIORITY)?;
    m.add(
        "FLAGS_COMPRESSED",
        vertebra_core::protocol::flags::COMPRESSED,
    )?;
    m.add(
        "FLAGS_FIRE_AND_FORGET",
        vertebra_core::protocol::flags::FIRE_AND_FORGET,
    )?;
    m.add("FLAGS_BATCH", vertebra_core::protocol::flags::BATCH)?;
    m.add("FLAGS_OK", vertebra_core::protocol::flags::OK)?;
    m.add("FLAGS_ERROR", vertebra_core::protocol::flags::ERROR)?;

    // Compression codec enum
    m.add_class::<protocol::PyCompressionCodec>()?;

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
