//! `vertebra-mysql` — Standalone PyO3 extension (`vertebra._mysql`) for MySQL 8.4/9 and MariaDB.

pub mod client;
pub mod config;
pub mod errors;
pub mod protocol;

use pyo3::prelude::*;

pub use client::{AsyncMySQLClient, MYSQL_COMPLETION_DRAINER_ATTR, SyncMySQLClient};
pub use config::{MySQLConnectConfig, MySQLSslMode};
pub use errors::{MySQLError, MySQLErrorKind, classify_mysql_error, map_mysql_error};
pub use protocol::{
    HandshakeV10, compute_caching_sha2_scramble, compute_caching_sha2_scramble_bytes,
    compute_mysql_native_scramble, compute_mysql_native_scramble_bytes, decode_mysql_packet,
    encrypt_caching_sha2_rsa, encrypt_caching_sha2_rsa_password, encode_mysql_packet,
    normalize_mysql_placeholders, split_sql_statements,
};

#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

#[pymodule(gil_used = false)]
fn _mysql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    vertebra_db::install_rustls_ring_provider();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_class::<SyncMySQLClient>()?;
    m.add_class::<AsyncMySQLClient>()?;
    m.add_function(wrap_pyfunction!(compute_caching_sha2_scramble, m)?)?;
    m.add_function(wrap_pyfunction!(compute_mysql_native_scramble, m)?)?;
    m.add_function(wrap_pyfunction!(encrypt_caching_sha2_rsa_password, m)?)?;
    m.add_function(wrap_pyfunction!(map_mysql_error, m)?)?;
    Ok(())
}
