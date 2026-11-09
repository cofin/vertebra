//! Native Rust PostgreSQL Protocol 3.0 client and pool extension (`vertebra._postgres`).

use pyo3::prelude::*;

pub mod auth;
pub mod binding;
pub mod cloud;
pub mod config;
pub mod credentials;
pub mod errors;
pub mod protocol;
pub mod session;
pub mod tls;

/// Whether this build targets free-threaded CPython (`Py_GIL_DISABLED`).
pub const FREE_THREADED: bool = cfg!(Py_GIL_DISABLED);

/// Whether the `vertebra._postgres` module requires the GIL on import (`false`).
pub const GIL_USED: bool = false;

#[pymodule(gil_used = false)]
fn _postgres(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", GIL_USED)?;
    Ok(())
}
