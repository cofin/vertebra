//! Standalone native PostgreSQL client extension for Vertebra.

pub(crate) mod auth;
pub mod binding;
pub mod config;
pub mod errors;
pub(crate) mod protocol;
pub mod session;
pub(crate) mod tls;

use pyo3::prelude::*;

/// Whether this build targets free-threaded Python.
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// Whether importing this module requires the Python GIL.
pub const GIL_USED: bool = false;

#[pymodule(gil_used = false)]
fn _postgres(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", GIL_USED)?;
    Ok(())
}
