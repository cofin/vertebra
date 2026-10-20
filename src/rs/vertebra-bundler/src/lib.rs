//! vertebra-bundler: Native OXC bundler integration for Vertebra
//!
//! This crate provides high-performance JavaScript/TypeScript bundling
//! using oxc (the foundation of Rolldown) with PyO3 Python bindings.

#![allow(clippy::useless_conversion)]

mod bundler;
mod config;
mod errors;
mod hmr;
mod ssr;

use pyo3::prelude::*;

pub use bundler::Bundler;
pub use config::{BuildResult, BundleConfig, DevConfig, OutputFile};
pub use errors::BundlerError;
pub use hmr::{HmrEvent, SourceWatcher};
pub use ssr::{SsrPool, SsrResult};

/// Python module definition
#[pymodule]
fn _bundler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Bundler>()?;
    m.add_class::<BundleConfig>()?;
    m.add_class::<DevConfig>()?;
    m.add_class::<BuildResult>()?;
    m.add_class::<OutputFile>()?;
    Ok(())
}
