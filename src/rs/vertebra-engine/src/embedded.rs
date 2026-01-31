//! Embedded Python interpreter management.
//!
//! Handles:
//! - Initializing the Python interpreter
//! - Setting up sys.path
//! - Managing the Global Interpreter Lock (GIL) for the main thread

use pyo3::prelude::*;
use pyo3::types::PyList;
use std::path::Path;

/// Errors that can occur during embedded Python operations.
#[derive(Debug, thiserror::Error)]
pub enum EmbeddedError {
    #[error("python error: {0}")]
    Python(#[from] PyErr),
    #[error("invalid path: {0}")]
    Path(String),
}

/// Initialize the embedded Python interpreter and configure sys.path.
///
/// # Arguments
/// * `extra_paths` - List of paths to add to sys.path (e.g. project root)
pub fn init_python(extra_paths: &[&str]) -> Result<(), EmbeddedError> {
    // pyo3::prepare_freethreaded_python(); // Auto-init feature handles this

    Python::with_gil(|py| -> PyResult<()> {
        let sys = py.import("sys")?;
        // Use downcast_into -> cast_as (PyO3 0.21+) or just extract
        // In 0.27 (Bound API), downcast_into is deprecated for cast_into??
        // Let's try the modern Bound API pattern.
        let path: Bound<'_, PyList> = sys.getattr("path")?.extract()?;

        for p in extra_paths {
            let path_str = Path::new(p)
                .canonicalize()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
            
            // Insert at beginning to prioritize local modules
            path.insert(0, path_str.to_string_lossy().as_ref())?;
        }
        
        // Ensure the current working directory is also in path if not present
        let cwd = std::env::current_dir()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        path.insert(0, cwd.to_string_lossy().as_ref())?;

        Ok(())
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_init() {
        init_python(&[]).expect("failed to init python");
        
        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            let version = sys.getattr("version").unwrap();
            println!("Python version: {}", version);
            assert!(!version.to_string().is_empty());
        });
    }

    #[test]
    fn test_sys_path_injection() {
        let test_dir = std::env::temp_dir();
        let test_path = test_dir.to_str().unwrap();
        
        init_python(&[test_path]).expect("failed to init python");

        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            let path: Bound<'_, PyList> = sys.getattr("path").unwrap().extract().unwrap();
            
            // Check if our path is present (might be canonicalized)
            let mut found = false;
            for item in path.iter() {
                let s = item.extract::<String>().unwrap();
                if s.contains(test_path) || std::path::Path::new(&s).starts_with(&test_dir) {
                    found = true;
                    break;
                }
            }
            assert!(found, "sys.path missing injected path");
        });
    }
}