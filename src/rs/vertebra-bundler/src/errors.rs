//! Error types for vertebra-bundler

use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BundlerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error in {path}: {message}")]
    Parse { path: String, message: String },

    #[error("Transform error: {0}")]
    Transform(String),

    #[error("Watcher error: {0}")]
    Watcher(String),

    #[error("SSR error: {0}")]
    Ssr(String),

    #[error("SSR pool exhausted — all workers busy")]
    PoolExhausted,
}

impl From<BundlerError> for PyErr {
    fn from(err: BundlerError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, BundlerError>;
