//! vertebra-bundler: Native OXC bundler integration for Vertebra
//!
//! This crate provides high-performance JavaScript/TypeScript bundling
//! using oxc (the foundation of Rolldown) with PyO3 Python bindings.

#![allow(clippy::useless_conversion)]

pub mod bundler;
pub mod config;
pub mod css;
pub mod declarations;
pub mod errors;
pub mod hmr;
pub mod ssr;

use pyo3::prelude::*;

pub use bundler::Bundler;
pub use config::{BuildResult, BundleConfig, DevConfig, OutputFile};
pub use css::minify_css;
pub use declarations::IsolatedDeclarationsGenerator;
pub use errors::BundlerError;
pub use hmr::{HmrEvent, PySourceWatcher, SourceWatcher};
pub use ssr::{SsrPool, SsrResult};

/// Python module definition
#[pymodule(gil_used = false)]
fn _bundler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("GIL_USED", false)?;
    m.add_class::<Bundler>()?;
    m.add_class::<BundleConfig>()?;
    m.add_class::<DevConfig>()?;
    m.add_class::<BuildResult>()?;
    m.add_class::<OutputFile>()?;
    m.add_class::<PySourceWatcher>()?;
    m.add_class::<SsrPool>()?;
    m.add_class::<SsrResult>()?;
    m.add_class::<IsolatedDeclarationsGenerator>()?;
    m.add_function(wrap_pyfunction!(minify_css, m)?)?;
    Ok(())
}
