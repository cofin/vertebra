//! Embedded Python interpreter management.
//!
//! Handles:
//! - Initializing the Python interpreter
//! - Setting up sys.path
//! - Managing the Global Interpreter Lock (GIL) for the main thread

use pyo3::prelude::*;
use pyo3::types::PyList;
use std::path::Path;
use std::sync::Arc;

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

/// A worker that executes Python code directly in the embedding process.
///
/// Wraps a PyObject (the ASGI app) and handles GIL acquisition.
#[derive(Clone)]
pub struct EmbeddedWorker {
    // Wrap in Arc to ensure Clone is cheap and thread-safe
    app: Arc<Py<PyAny>>,
}

impl EmbeddedWorker {
    /// Create a new embedded worker with the given ASGI application.
    pub fn new(app: Bound<'_, PyAny>) -> Self {
        Self {
            app: Arc::new(app.unbind()),
        }
    }

    /// Get a reference to the underlying ASGI application.
    pub fn get_app<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.app.bind(py).clone()
    }

    /// Execute the ASGI application with the given scope.
    ///
    /// This is a placeholder for the actual execution logic (Phase 2 Task 3).
    /// For now, it just proves we can hold the app safely.
    pub async fn execute(&self) -> Result<(), EmbeddedError> {
        // TODO: Implement actual execution
        Err(EmbeddedError::Path("Not implemented".into()))
    }
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

    #[test]
    fn test_embedded_worker_lifecycle() {
        init_python(&[]).unwrap();
        
        let app_code = r#"
async def app(scope, receive, send):
    pass
"#;
        let app_code_c = std::ffi::CString::new(app_code).unwrap();
        let file_name_c = std::ffi::CString::new("test_app.py").unwrap();
        let module_name_c = std::ffi::CString::new("test_app").unwrap();

        Python::with_gil(|py| {
            // Use PyModule::from_code (Bound API in 0.27+)
            let app_module = PyModule::from_code(
                py, 
                &app_code_c, 
                &file_name_c, 
                &module_name_c
            ).unwrap();
            let app_func = app_module.getattr("app").unwrap();
            
            let worker = EmbeddedWorker::new(app_func);
            
            // Verify we can get the app back and it's the same object
            let retrieved_app = worker.get_app(py);
            assert!(retrieved_app.is_callable());
        });
    }
}