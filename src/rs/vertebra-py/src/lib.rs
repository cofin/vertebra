//! PyO3 bindings for Vertebra.
//!
//! This module is imported as `vertebra._vertebra` in Python and provides
//! access to high-performance Rust-based functionality.

use pyo3::prelude::*;

/// Python module initialization.
#[pymodule]
fn _vertebra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", vertebra_core::VERSION)?;

    // Future PRDs will add:
    // - IPC client/server classes
    // - Zero-copy buffer types
    // - Type bridge utilities

    Ok(())
}
