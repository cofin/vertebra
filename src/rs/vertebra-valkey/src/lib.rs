//! Native pure-Rust Valkey 8.0+/9.0 PyO3 extension (`vertebra._valkey`).

pub mod client;
pub mod config;
pub mod pubsub;

pub use vertebra_resp::{
    cluster, codec, ensure_rustls_provider, error, sentinel, streams, tracking, transport,
};

use pyo3::prelude::*;

/// Event-loop attribute key storing this extension's independent `AsyncCompletionQueue` drainer.
pub const VALKEY_COMPLETION_DRAINER_ATTR: &str = "_vertebra_valkey_completion_drainer";

/// Compute CRC-16/XMODEM checksum matching Valkey Cluster slot hashing.
#[pyfunction]
#[must_use]
pub fn crc16_xmodem(data: &[u8]) -> u16 {
    vertebra_resp::crc16_xmodem(data)
}

/// Compute the 14-bit Valkey Cluster hash slot (`0..16384`) respecting `{hash_tag}`.
#[pyfunction]
#[must_use]
pub fn valkey_hash_slot(key: &[u8]) -> u16 {
    vertebra_resp::hash_slot(key)
}

/// Parse a Valkey cluster redirection error (`MOVED <slot> <host>:<port>` or `ASK <slot> <host>:<port>`).
#[pyfunction]
#[must_use]
pub fn parse_cluster_redirect(message: &str) -> Option<(String, u16, String, u16)> {
    vertebra_resp::parse_cluster_redirect(message)
}

#[pymodule(gil_used = false)]
fn _valkey(m: &Bound<'_, PyModule>) -> PyResult<()> {
    ensure_rustls_provider();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", cfg!(Py_GIL_DISABLED))?;
    m.add("GIL_USED", false)?;
    m.add_function(wrap_pyfunction!(crc16_xmodem, m)?)?;
    m.add_function(wrap_pyfunction!(valkey_hash_slot, m)?)?;
    m.add_function(wrap_pyfunction!(parse_cluster_redirect, m)?)?;
    m.add_class::<config::ValkeyClientConfig>()?;
    m.add_class::<client::SyncValkeyClient>()?;
    m.add_class::<client::AsyncValkeyClient>()?;
    m.add_class::<client::SyncValkeyPipeline>()?;
    m.add_class::<client::AsyncValkeyPipeline>()?;
    m.add_class::<pubsub::SyncValkeyListener>()?;
    m.add_class::<pubsub::AsyncValkeyListener>()?;
    Ok(())
}
