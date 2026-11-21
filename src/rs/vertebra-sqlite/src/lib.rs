//! Standalone SQLite 3.45+ PyO3 extension (`vertebra._sqlite`).

pub mod arrow_bridge;
pub mod config;
pub mod errors;
pub mod py_client;
pub mod value;
pub mod worker;

use py_client::{
    AsyncSQLiteClient, AsyncSQLiteCursor, SyncSQLiteClient, SyncSQLiteCursor, broker_engine_capsule,
};
use pyo3::prelude::*;

/// Whether this build targets free-threaded Python (3.14t+).
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// Extension module initialization.
#[pymodule(gil_used = false)]
fn _sqlite(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_function(wrap_pyfunction!(broker_engine_capsule, m)?)?;
    m.add_class::<SyncSQLiteClient>()?;
    m.add_class::<AsyncSQLiteClient>()?;
    m.add_class::<SyncSQLiteCursor>()?;
    m.add_class::<AsyncSQLiteCursor>()?;
    m.add(
        "__all__",
        vec![
            "AsyncSQLiteClient",
            "AsyncSQLiteCursor",
            "FREE_THREADED",
            "GIL_USED",
            "SyncSQLiteClient",
            "SyncSQLiteCursor",
            "__version__",
            "broker_engine_capsule",
        ],
    )?;
    let _ = py;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_version_and_gil() {
        assert!(
            rusqlite::version_number() >= 3_045_000,
            "SQLite bundled version must be >= 3.45.0, got {}",
            rusqlite::version_number()
        );
        const { assert!(!FREE_THREADED || cfg!(Py_GIL_DISABLED)) };
    }
}
