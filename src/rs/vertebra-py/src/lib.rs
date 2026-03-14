//! vertebra-py — PyO3 extension module for Vertebra.
//!
//! This crate is a stub. FFI bindings arrive in the `pyo3-request-proxy` chapter.

use pyo3::prelude::*;

/// Minimal PyO3 module. Exposes `__version__` to Python.
#[pymodule]
fn _vertebra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn module_compiles() {
        // Compilation of this file is the test.
        assert!(true);
    }
}
