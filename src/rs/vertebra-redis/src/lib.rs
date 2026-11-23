//! `vertebra-redis` — Standalone PyO3 extension (`vertebra._redis`) backed by `vertebra-resp`.

pub mod pubsub;
pub mod py_client;

pub use vertebra_resp::{
    client, cluster, codec, config, error, sentinel, streams, tracking, transport,
};

use pyo3::prelude::*;

pub const FREE_THREADED: bool = cfg!(Py_GIL_DISABLED);

fn extract_str_or_bytes(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Vec<u8>> {
    if let Ok(s) = obj.extract::<&str>() {
        Ok(s.as_bytes().to_vec())
    } else if let Ok(b) = obj.extract::<&[u8]>() {
        Ok(b.to_vec())
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "{name} must be str or bytes"
        )))
    }
}

/// Compute CRC16-XMODEM for Redis Cluster keys.
#[pyfunction]
pub fn crc16_xmodem(data: Bound<'_, PyAny>) -> PyResult<u16> {
    let bytes = extract_str_or_bytes(&data, "data")?;
    Ok(vertebra_resp::crc16_xmodem(&bytes))
}

/// Compute Redis Cluster hash slot (`0..16383`) supporting `{hash_tag}` extraction.
#[pyfunction]
pub fn hash_slot(key: Bound<'_, PyAny>) -> PyResult<u16> {
    let bytes = extract_str_or_bytes(&key, "key")?;
    Ok(vertebra_resp::hash_slot(&bytes))
}

/// Alias for `hash_slot`.
#[pyfunction]
pub fn redis_hash_slot(key: Bound<'_, PyAny>) -> PyResult<u16> {
    hash_slot(key)
}

/// Parse a Redis Cluster `MOVED` or `ASK` redirection error into `(kind, slot, host, port)`.
#[pyfunction]
pub fn parse_cluster_redirect(
    err_msg: Bound<'_, PyAny>,
) -> PyResult<Option<(String, u16, String, u16)>> {
    let s = if let Ok(st) = err_msg.extract::<&str>() {
        st.to_string()
    } else if let Ok(b) = err_msg.extract::<&[u8]>() {
        std::str::from_utf8(b).unwrap_or("").to_string()
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "err_msg must be str or bytes",
        ));
    };
    Ok(vertebra_resp::parse_cluster_redirect(&s))
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
    #[test]
    fn test_redis_hash_slot_and_crc16() {
        assert_eq!(vertebra_resp::crc16_xmodem(b"123456789"), 0x31C3);
        assert_eq!(
            vertebra_resp::hash_slot(b"{user}:1"),
            vertebra_resp::hash_slot(b"{user}:2")
        );
    }

    #[test]
    fn test_parse_cluster_redirect() {
        assert_eq!(
            vertebra_resp::parse_cluster_redirect("MOVED 3999 127.0.0.1:6381"),
            Some(("MOVED".to_string(), 3999, "127.0.0.1".to_string(), 6381))
        );
        assert_eq!(
            vertebra_resp::parse_cluster_redirect("ASK 3999 127.0.0.1:6381"),
            Some(("ASK".to_string(), 3999, "127.0.0.1".to_string(), 6381))
        );
        assert!(vertebra_resp::parse_cluster_redirect("ERR unknown command").is_none());
    }
}
