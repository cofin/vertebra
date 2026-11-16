//! `vertebra-redis` — Standalone PyO3 extension (`vertebra._redis`) for Redis 7.2+/8.0.

#![allow(
    clippy::too_many_arguments,
    clippy::redundant_closure,
    clippy::collapsible_if,
    clippy::manual_strip
)]

pub mod caching;
pub mod client_core;
pub mod config;
pub mod errors;
pub mod pubsub;
pub mod py_client;
pub mod streams;
pub mod topology;
pub mod value;

use pyo3::prelude::*;
use vertebra_db::resp3::{
    crc16_xmodem as rs_crc16_xmodem, parse_cluster_redirect as rs_parse_cluster_redirect,
    redis_hash_slot as rs_redis_hash_slot,
};

#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// Compute CRC16-XMODEM for Redis Cluster keys.
#[pyfunction]
pub fn crc16_xmodem(data: Bound<'_, PyAny>) -> PyResult<u16> {
    if let Ok(s) = data.extract::<&str>() {
        Ok(rs_crc16_xmodem(s.as_bytes()))
    } else if let Ok(b) = data.extract::<&[u8]>() {
        Ok(rs_crc16_xmodem(b))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err("data must be str or bytes"))
    }
}

/// Compute Redis Cluster hash slot (`0..16383`) supporting `{hash_tag}` extraction.
#[pyfunction]
pub fn hash_slot(key: Bound<'_, PyAny>) -> PyResult<u16> {
    if let Ok(s) = key.extract::<&str>() {
        Ok(rs_redis_hash_slot(s.as_bytes()))
    } else if let Ok(b) = key.extract::<&[u8]>() {
        Ok(rs_redis_hash_slot(b))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err("key must be str or bytes"))
    }
}

/// Alias for `hash_slot`.
#[pyfunction]
pub fn redis_hash_slot(key: Bound<'_, PyAny>) -> PyResult<u16> {
    hash_slot(key)
}

/// Parse a Redis Cluster `MOVED` or `ASK` redirection error into `(kind, slot, host, port)`.
#[pyfunction]
pub fn parse_cluster_redirect(err_msg: Bound<'_, PyAny>) -> PyResult<Option<(String, u16, String, u16)>> {
    let s = if let Ok(st) = err_msg.extract::<&str>() {
        st
    } else if let Ok(b) = err_msg.extract::<&[u8]>() {
        std::str::from_utf8(b).unwrap_or("")
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err("err_msg must be str or bytes"));
    };
    Ok(rs_parse_cluster_redirect(s).map(|r| (r.kind.as_str().to_string(), r.slot, r.host, r.port)))
}

#[pymodule(gil_used = false)]
fn _redis(m: &Bound<'_, PyModule>) -> PyResult<()> {
    vertebra_db::install_rustls_ring_provider();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_function(wrap_pyfunction!(crc16_xmodem, m)?)?;
    m.add_function(wrap_pyfunction!(hash_slot, m)?)?;
    m.add_function(wrap_pyfunction!(redis_hash_slot, m)?)?;
    m.add_function(wrap_pyfunction!(parse_cluster_redirect, m)?)?;
    m.add_class::<py_client::SyncRedisClient>()?;
    m.add_class::<py_client::AsyncRedisClient>()?;
    m.add_class::<py_client::SyncRedisPipeline>()?;
    m.add_class::<py_client::AsyncRedisPipeline>()?;
    m.add_class::<pubsub::SyncRedisListener>()?;
    m.add_class::<pubsub::AsyncRedisListener>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_hash_slot_and_crc16() {
        assert_eq!(rs_redis_hash_slot(b"123456789"), 0x31C3);
        assert_eq!(
            rs_redis_hash_slot(b"{user}:1"),
            rs_redis_hash_slot(b"{user}:2")
        );
    }

    #[test]
    fn test_parse_cluster_redirect() {
        let moved = rs_parse_cluster_redirect("MOVED 3999 127.0.0.1:6381").expect("moved");
        assert_eq!(moved.slot, 3999);
        assert_eq!(moved.host, "127.0.0.1");
        assert_eq!(moved.port, 6381);

        let ask = rs_parse_cluster_redirect("ASK 3999 127.0.0.1:6381").expect("ask");
        assert_eq!(ask.slot, 3999);
        assert_eq!(ask.host, "127.0.0.1");
        assert_eq!(ask.port, 6381);

        assert!(rs_parse_cluster_redirect("ERR unknown command").is_none());
    }
}
