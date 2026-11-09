//! `vertebra-oracle` crate and `vertebra._oracle` PyO3 extension module.

pub mod config;
pub mod dialer;
pub mod errors;

use pyo3::prelude::*;

/// Whether this build targets free-threaded Python (3.14t+).
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// Whether this extension requires the Python GIL (`false` for free-threaded safety).
pub const GIL_USED: bool = false;

#[pymodule(gil_used = false)]
pub fn _oracle(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", GIL_USED)?;
    Ok(())
}
