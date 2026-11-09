//! `vertebra-oracle` crate and `vertebra._oracle` PyO3 extension module.

pub mod arrow;
pub mod client;
pub mod config;
pub mod dialer;
pub mod errors;
pub mod pool;
pub mod types;

use pyo3::prelude::*;

use crate::client::{AsyncOracleClient, SyncOracleClient};
use crate::pool::{AsyncOraclePool, SyncOraclePool};
use crate::types::{OracleColumn, OracleQueryResult};

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
    m.add_class::<OracleColumn>()?;
    m.add_class::<OracleQueryResult>()?;
    m.add_class::<SyncOracleClient>()?;
    m.add_class::<AsyncOracleClient>()?;
    m.add_class::<SyncOraclePool>()?;
    m.add_class::<AsyncOraclePool>()?;
    Ok(())
}
