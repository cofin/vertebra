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

#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

pub const CRC16_XMODEM_TABLE: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b,
    0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738,
    0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
    0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
    0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d,
    0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

pub fn raw_crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        let idx = ((crc >> 8) as u8 ^ byte) as usize;
        crc = (crc << 8) ^ CRC16_XMODEM_TABLE[idx];
    }
    crc
}

pub fn raw_hash_slot(key: &[u8]) -> u16 {
    let slice = if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(rel_end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if rel_end > 0 {
                &key[start + 1..start + 1 + rel_end]
            } else {
                key
            }
        } else {
            key
        }
    } else {
        key
    };
    raw_crc16_xmodem(slice) & 0x3FFF
}

/// Compute CRC16-XMODEM for Redis Cluster keys.
#[pyfunction]
pub fn crc16_xmodem(data: Bound<'_, PyAny>) -> PyResult<u16> {
    if let Ok(s) = data.extract::<&str>() {
        Ok(raw_crc16_xmodem(s.as_bytes()))
    } else if let Ok(b) = data.extract::<&[u8]>() {
        Ok(raw_crc16_xmodem(b))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "data must be str or bytes",
        ))
    }
}

/// Compute Redis Cluster hash slot (`0..16383`) supporting `{hash_tag}` extraction.
#[pyfunction]
pub fn hash_slot(key: Bound<'_, PyAny>) -> PyResult<u16> {
    if let Ok(s) = key.extract::<&str>() {
        Ok(raw_hash_slot(s.as_bytes()))
    } else if let Ok(b) = key.extract::<&[u8]>() {
        Ok(raw_hash_slot(b))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "key must be str or bytes",
        ))
    }
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
        st
    } else if let Ok(b) = err_msg.extract::<&[u8]>() {
        std::str::from_utf8(b).unwrap_or("")
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "err_msg must be str or bytes",
        ));
    };
    let trimmed = s.trim().trim_start_matches('-').trim();
    let mut parts = trimmed.split_whitespace();
    let kind_raw = match parts.next() {
        Some(k) => k,
        None => return Ok(None),
    };
    let kind = match kind_raw {
        "MOVED" | "moved" => "MOVED",
        "ASK" | "ask" => "ASK",
        _ => return Ok(None),
    };
    let slot: u16 = match parts.next().and_then(|p| p.parse().ok()) {
        Some(sl) if sl < 16384 => sl,
        _ => return Ok(None),
    };
    let addr = match parts.next() {
        Some(a) => a,
        None => return Ok(None),
    };
    let (host, port_str) = match addr.rsplit_once(':') {
        Some(hp) => hp,
        None => return Ok(None),
    };
    let host_clean = host.trim_start_matches('[').trim_end_matches(']');
    if host_clean.is_empty() {
        return Ok(None);
    }
    let port: u16 = match port_str.parse().ok() {
        Some(p) => p,
        None => return Ok(None),
    };
    Ok(Some((kind.to_string(), slot, host_clean.to_string(), port)))
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
        assert_eq!(raw_crc16_xmodem(b"123456789"), 0x31C3);
        assert_eq!(raw_hash_slot(b"{user}:1"), raw_hash_slot(b"{user}:2"));
    }

    #[test]
    fn test_parse_cluster_redirect() {
        let moved = topology::check_cluster_redirect("MOVED 3999 127.0.0.1:6381").expect("moved");
        match moved {
            topology::ClusterRedirectAction::Moved { slot, host, port } => {
                assert_eq!(slot, 3999);
                assert_eq!(host, "127.0.0.1");
                assert_eq!(port, 6381);
            }
            _ => panic!("expected Moved"),
        }

        let ask = topology::check_cluster_redirect("ASK 3999 127.0.0.1:6381").expect("ask");
        match ask {
            topology::ClusterRedirectAction::Ask { slot, host, port } => {
                assert_eq!(slot, 3999);
                assert_eq!(host, "127.0.0.1");
                assert_eq!(port, 6381);
            }
            _ => panic!("expected Ask"),
        }

        assert!(topology::check_cluster_redirect("ERR unknown command").is_none());
    }
}
